#!/usr/bin/env python
"""Main module of kube-schedule-scaler"""

import concurrent.futures
import json
import logging
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from os import _exit as exit
from queue import Queue
from signal import SIGINT, SIGQUIT, SIGTERM, signal, strsignal
from time import sleep
from types import FrameType

import pykube
import requests
from croniter import croniter

# custom type
ScheduleActions = list[dict[str, str]]

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)


class ScaleTarget(Enum):
    DEPLOYMENT = 0
    HORIZONAL_POD_AUTOSCALER = 1


def get_kube_api() -> pykube.HTTPClient:
    """Initiating the API from Service Account or when running locally from ~/.kube/config"""
    return pykube.HTTPClient(pykube.KubeConfig.from_env())


@dataclass
class DeploymentStore:
    deployments: dict[tuple[str, str], ScheduleActions]
    lock: threading.Lock

    def __init__(self) -> None:
        self.deployments = {}
        self.lock = threading.Lock()


def parse_schedules(schedules: str, identifier: tuple[str, str]) -> ScheduleActions:
    """Parse the JSON schedule"""
    try:
        return json.loads(schedules)
    except (TypeError, json.JSONDecodeError) as err:
        logging.error("%s - Error in parsing JSON %s", identifier, schedules)
        logging.exception(err)
        return []


def get_delta_sec(schedule: str, timezone_name: str | None = None) -> int:
    """Returns the number of seconds passed since last occurence of the given cron expression"""
    # localize the time to the provided timezone, if specified
    if not timezone_name:
        tz = None
    else:
        tz = dateutil.tz.gettz(timezone_name)

    # get current time
    now = datetime.now(tz)
    # get the last previous occurrence of the cron expression
    time = croniter(schedule, now).get_prev()
    # convert now to unix timestamp
    now_ts = now.timestamp()
    # return the delta
    return int(now_ts - time)


def get_wait_sec() -> float:
    """Return the number of seconds to wait before the next minute"""
    now = datetime.now()
    future = datetime(now.year, now.month, now.day, now.hour, now.minute) + timedelta(
        minutes=1
    )
    return (future - now).total_seconds()


def process_deployment(
    deployment: tuple[str, str], sa: ScheduleActions, queue: Queue
) -> None:
    """Determine actions to run for the given deployment and list of schedules"""
    namespace, name = deployment
    for schedule in sa:
        # when provided, convert the values to int
        replicas = schedule.get("replicas", None)
        if replicas is not None:
            replicas = int(replicas)
        min_replicas = schedule.get("minReplicas", None)
        if min_replicas is not None:
            min_replicas = int(min_replicas)
        max_replicas = schedule.get("maxReplicas", None)
        if max_replicas is not None:
            max_replicas = int(max_replicas)

        schedule_expr = schedule.get("schedule", None)

        if not schedule_expr:
            return

        schedule_timezone = schedule.get("tz", None)
        logging.debug("%s %s", deployment, schedule)

        # if less than 60 seconds have passed from the trigger
        if get_delta_sec(schedule_expr, schedule_timezone) < 60:
            if replicas is not None:
                queue.put((ScaleTarget.DEPLOYMENT, name, namespace, replicas))
            if min_replicas is not None or max_replicas is not None:
                queue.put(
                    (
                        ScaleTarget.HORIZONAL_POD_AUTOSCALER,
                        name,
                        namespace,
                        min_replicas,
                        max_replicas,
                    )
                )


def scale_deployment(name: str, namespace: str, replicas: int) -> None:
    """Scale the deployment to the given number of replicas"""

    api = get_kube_api()
    try:
        deployment = (
            pykube.Deployment.objects(api).filter(namespace=namespace).get(name=name)
        )
    except pykube.exceptions.ObjectDoesNotExist:
        logging.warning("Deployment %s/%s does not exist", namespace, name)
        return

    if replicas == deployment.replicas:
        return

    try:
        deployment.patch({"spec": {"replicas": replicas}}, subresource="scale")
        logging.info(
            "Deployment %s/%s scaled to %s replicas", namespace, name, replicas
        )

    except pykube.exceptions.HTTPError as err:
        logging.error(
            "Exception raised while patching deployment %s/%s", namespace, name
        )
        logging.exception(err)


def scale_hpa(
    name: str, namespace: str, min_replicas: int | None, max_replicas: int | None
) -> None:
    """Adjust hpa min and max number of replicas"""

    api = get_kube_api()
    try:
        hpa = (
            pykube.HorizontalPodAutoscaler.objects(api)
            .filter(namespace=namespace)
            .get(name=name)
        )
    except pykube.exceptions.ObjectDoesNotExist:
        logging.warning("HPA %s/%s does not exist", namespace, name)
        return

    patch = {}

    spec = hpa.obj["spec"]
    if min_replicas is not None and min_replicas != spec["minReplicas"]:
        patch["minReplicas"] = min_replicas

    if max_replicas is not None and max_replicas != spec["maxReplicas"]:
        patch["maxReplicas"] = max_replicas

    if not patch:
        return

    try:
        hpa.patch({"spec": patch})
        if min_replicas:
            logging.info(
                "HPA %s/%s minReplicas set to %s", namespace, name, min_replicas
            )
        if max_replicas:
            logging.info(
                "HPA %s/%s maxReplicas set to %s", namespace, name, max_replicas
            )
    except pykube.exceptions.HTTPError as err:
        logging.error("Exception raised while patching HPA %s/%s", namespace, name)
        logging.exception(err)


def watch_deployments(ds: DeploymentStore) -> None:
    """Sync deployment objects between k8s api server and kube-schedule-scaler"""
    logging.info("Starting watcher thread")

    last_resource_version = None
    while True:
        try:
            # avoid stale tokens by initializing the client at every reconnect
            api = pykube.HTTPClient(pykube.KubeConfig.from_env(), timeout=120)

            query = pykube.Deployment.objects(api).filter(namespace=pykube.all)

            for event_type, obj in query.watch(
                since=last_resource_version, params={"allowWatchBookmarks": "true"}
            ):
                last_resource_version = obj.metadata.get("resourceVersion")
                logging.debug(f"watch last_resource_version -> {last_resource_version}")

                if event_type == "ERROR":
                    logging.warning(f"watch error: {obj.obj}")
                    # 410 indicates the provided last_resource_version value is expired
                    if obj.obj["code"] == 410:
                        logging.debug("watch: last_resource_version -> None")
                        last_resource_version = None
                        with ds.lock:
                            # should be fine because lock is used when processing deployments
                            ds.deployments.clear()
                    break

                if event_type == "BOOKMARK":
                    logging.debug(f"watch bookmark: {obj.obj}")
                    continue

                key = (obj.namespace, obj.name)
                if event_type in ["ADDED", "MODIFIED"] and (
                    schedules := obj.annotations.get("zalando.org/schedule-actions")
                ):
                    with ds.lock:
                        ds.deployments[key] = parse_schedules(schedules, key)
                else:
                    with ds.lock:
                        ds.deployments.pop(key, None)

                logging.debug(f"Deployments: {ds.deployments}")

        except requests.exceptions.ConnectionError as e:
            # This catches ReadTimeouts, ConnectionReset, and API restarts
            logging.error(f"Watch disconnected: {e}. Reconnecting...")


def collect_scaling_jobs(ds: DeploymentStore, queue: Queue) -> None:
    """Collect scaling jobs and adds them to the queue"""
    logging.info("Starting collector thread")
    while True:
        with ds.lock:
            for deployment, schedule_action in ds.deployments.items():
                process_deployment(deployment, schedule_action, queue)
        logging.debug(f"queue items: {list(queue.queue)}")
        sleep(get_wait_sec())


def process_scaling_jobs(queue: Queue) -> None:
    """Processes scaling jobs"""
    logging.info("Starting processor thread")
    while True:
        item = queue.get()
        match item[0]:
            case ScaleTarget.DEPLOYMENT:
                scale_deployment(*item[1:])
            case ScaleTarget.HORIZONAL_POD_AUTOSCALER:
                scale_hpa(*item[1:])


def handle_shutdown(signum: int, _: FrameType | None) -> None:
    """Handle shutdown related signals"""
    sig_str = strsignal(signum)
    sig_str = sig_str.split(":")[0] if sig_str else "Unknown"
    logging.info(f"Received {sig_str}: exiting immediately")
    exit(0)


if __name__ == "__main__":
    signal(SIGTERM, handle_shutdown)
    signal(SIGINT, handle_shutdown)
    signal(SIGQUIT, handle_shutdown)

    ds = DeploymentStore()
    queue = Queue()

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(watch_deployments, ds): "watcher",
            executor.submit(collect_scaling_jobs, ds, queue): "collector",
            executor.submit(process_scaling_jobs, queue): "processor",
        }

        for future in concurrent.futures.as_completed(futures):
            task_name = futures[future]
            try:
                result = future.result()
                logging.debug(f"success: {task_name}: {result}")
            except Exception as e:
                logging.fatal(f"failure: task {task_name}: {type(e).__name__}: {e}")
                exit(1)
