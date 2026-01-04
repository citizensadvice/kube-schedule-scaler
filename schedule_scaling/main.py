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
from functools import partial
from queue import Queue
from signal import SIGABRT, SIGINT, SIGQUIT, SIGTERM, signal, strsignal
from sys import exit
from types import FrameType
from typing import cast

import dateutil
from croniter import croniter
from kubernetes import client, config, watch
from kubernetes.client.models import V1Deployment, V1ObjectMeta
from kubernetes.client.rest import ApiException

# client is shared across the program
try:
    config.load_incluster_config()
except config.ConfigException:
    config.load_kube_config()

apps_v1 = client.AppsV1Api()
autoscaling_v1 = client.AutoscalingV1Api()

# custom type
ScheduleActions = list[dict[str, str]]

# when this is True, gracefully terminate
shutdown = False
# exit code to return when all threads are terminated
exit_status_code = 0

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)


class ScaleTarget(Enum):
    DEPLOYMENT = 0
    HORIZONAL_POD_AUTOSCALER = 1


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
    try:
        patch_body = {"spec": {"replicas": replicas}}
        apps_v1.patch_namespaced_deployment_scale(
            name=name, namespace=namespace, body=patch_body
        )
        logging.info(
            "Deployment %s/%s scaled to %s replicas", namespace, name, replicas
        )
    except ApiException as e:
        if e.status == 404:
            logging.warning("Deployment %s/%s not found", namespace, name)
        else:
            logging.error("API error patching deployment %s/%s: %s", namespace, name, e)


def scale_hpa(
    name: str, namespace: str, min_replicas: int | None, max_replicas: int | None
) -> None:
    """Adjust HPA min/max replicas via a direct patch"""

    patch_body = {}
    if min_replicas is not None:
        patch_body["minReplicas"] = min_replicas
    if max_replicas is not None:
        patch_body["maxReplicas"] = max_replicas

    if not patch_body:
        return

    try:
        autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler(
            name=name, namespace=namespace, body={"spec": patch_body}
        )

        if min_replicas:
            logging.info(
                "HPA %s/%s minReplicas set to %s", namespace, name, min_replicas
            )
        if max_replicas:
            logging.info(
                "HPA %s/%s maxReplicas set to %s", namespace, name, max_replicas
            )

    except ApiException as e:
        if e.status == 404:
            logging.warning("HPA %s/%s not found", namespace, name)
        else:
            logging.error("API error patching HPA %s/%s: %s", namespace, name, e)


def watch_deployments(ds: DeploymentStore) -> None:
    """Sync deployment objects between k8s api server and kube-schedule-scaler"""
    global shutdown
    logging.info("Starting watcher thread")

    w = watch.Watch()

    last_resource_version = None
    while not shutdown:
        try:
            # watch bookmarks help with having the latest resource version
            # necessary to resume the event stream (on reconnect) without
            # getting 410 "Resource version too old" errors
            stream = w.stream(
                apps_v1.list_deployment_for_all_namespaces,
                resource_version=last_resource_version,
                allow_watch_bookmarks=True,
            )

            for event in stream:
                if not isinstance(event, dict):
                    logging.warning(f"Skipping non dict event data: {event}")
                    continue

                # watch can keep running for a long time so we need this here
                if shutdown:
                    logging.info("Watcher thread: exit")
                    return

                obj: dict | V1Deployment = event["object"]
                event_type = event["type"]

                # some events (e.g. BOOKMARK) return a dict
                if isinstance(obj, dict):
                    last_resource_version = obj["metadata"]["resourceVersion"]
                else:
                    last_resource_version = cast(
                        V1ObjectMeta, obj.metadata
                    ).resource_version
                logging.debug(f"watch last_resource_version -> {last_resource_version}")

                match event_type:
                    case "ADDED" | "MODIFIED" | "DELETED":
                        metadata = cast(V1ObjectMeta, obj.metadata)
                        logging.debug(
                            f"watch {event_type}: {metadata.namespace}/{metadata.name}"
                        )
                        key = (cast(str, metadata.namespace), cast(str, metadata.name))

                        if event_type != "DELETED" and (
                            schedules := cast(dict[str, str], metadata.annotations).get(
                                "zalando.org/schedule-actions"
                            )
                        ):
                            res = parse_schedules(schedules, key)
                            with ds.lock:
                                ds.deployments[key] = res
                        else:
                            with ds.lock:
                                ds.deployments.pop(key, None)
                    case _:
                        logging.debug(f"watch {event_type} {obj}")

                logging.debug(f"Deployments: {ds.deployments}")

        except ApiException as e:
            logging.error(f"Kubernetes API error: {e}")

            # Handle 410 Gone (Resource version too old)
            if e.status == 410:
                logging.debug("Resetting watch last_resource_version: expired")
                last_resource_version = None
                with ds.lock:
                    ds.deployments.clear()

        except Exception as e:
            logging.error(f"Watcher failed: {type(e).__name__}: {e}")
            handle_shutdown(SIGQUIT, None, queue, exit_code=2)

    logging.info("Watcher thread: exit")


class Collector:
    # collector is wrapped in a class so that we can use the condition
    # to notify it and wake it up on graceful shutdown
    condition = threading.Condition()

    @classmethod
    def collect_scaling_jobs(cls, ds: DeploymentStore, queue: Queue) -> None:
        """Collect scaling jobs and adds them to the queue"""
        global shutdown

        logging.info("Starting collector thread")

        while not shutdown:
            with ds.lock:
                for deployment, schedule_action in ds.deployments.items():
                    process_deployment(deployment, schedule_action, queue)
            logging.debug(f"queue items: {list(queue.queue)}")
            # wait until next minute but wake up if you have to shutdown
            with cls.condition:
                cls.condition.wait_for(lambda: shutdown, timeout=get_wait_sec())

        logging.info("Collector thread: exit")


def process_scaling_jobs(queue: Queue) -> None:
    """Processes scaling jobs"""
    global shutdown
    logging.info("Starting processor thread")

    while not shutdown:
        # this blocks but we can add a dummy item to wake the thread
        # if we want to shut down gracefully
        item = queue.get()
        match item[0]:
            case ScaleTarget.DEPLOYMENT:
                scale_deployment(*item[1:])
            case ScaleTarget.HORIZONAL_POD_AUTOSCALER:
                scale_hpa(*item[1:])

    logging.info("Processor thread: exit")


def handle_shutdown(
    signum: int, _: FrameType | None, queue: Queue, exit_code: int
) -> None:
    """Handle shutdown related signals"""
    global shutdown
    global exit_status_code

    if shutdown:
        # it means it's been already triggered by another signal before
        # no need to do the work twice and it can cause issues
        return

    sig_str = strsignal(signum)
    sig_str = sig_str.split(":")[0] if sig_str else "Unknown"
    logging.info(f"Received {sig_str}: exiting gracefully")
    shutdown = True
    # wake up the processor
    queue.put("notify")
    # wake up the collector
    with Collector.condition:
        Collector.condition.notify()
    exit_status_code = exit_code


if __name__ == "__main__":
    ds = DeploymentStore()
    queue = Queue()

    signal(SIGTERM, partial(handle_shutdown, queue=queue, exit_code=143))
    signal(SIGINT, partial(handle_shutdown, queue=queue, exit_code=130))
    signal(SIGQUIT, partial(handle_shutdown, queue=queue, exit_code=131))
    signal(SIGABRT, partial(handle_shutdown, queue=queue, exit_code=134))

    # for the watcher, we use a daemon thread so that it won't block graceful shutdown
    # since there's no easy way to interrupt a watch and the thread could
    # sleep for a long time
    threading.Thread(target=watch_deployments, args=[ds], daemon=True).start()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(Collector.collect_scaling_jobs, ds, queue): "collector",
            executor.submit(process_scaling_jobs, queue): "processor",
        }

        # NOTE: block waiting for the tasks, but report their success or failure as
        # soon as each individual one completes
        for future in concurrent.futures.as_completed(futures):
            task_name = futures[future]
            try:
                result = future.result()
                logging.debug(f"success: {task_name}: {result}")
            except Exception as e:
                logging.error(f"failure: task {task_name}: {type(e).__name__}: {e}")
                handle_shutdown(SIGQUIT, None, queue, exit_code=1)

        # expliticly return the correct status code since we're trapping signals
        exit(exit_status_code)
