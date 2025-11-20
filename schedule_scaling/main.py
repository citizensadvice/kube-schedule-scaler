#!/usr/bin/env python3
"""Main module of kube-schedule-scaler"""

import json
import logging
import os
from datetime import datetime, timedelta
from time import sleep

import dateutil.tz
import pykube
from croniter import croniter

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)


def get_kube_api() -> pykube.HTTPClient:
    """Initiating the API from Service Account or when running locally from ~/.kube/config"""
    return pykube.HTTPClient(pykube.KubeConfig.from_env())


def deployments_to_scale() -> dict[tuple[str, str], list[dict[str, str]]]:
    """Getting the deployments configured for schedule scaling"""
    deployments = []
    scaling_dict = {}
    for deployment in pykube.Deployment.objects(api).filter(namespace=pykube.all):
        f_deployment: tuple[str, str] = (deployment.namespace, deployment.name)

        annotations = deployment.metadata.get("annotations", {})
        schedule_actions = parse_schedules(
            annotations.get("zalando.org/schedule-actions", "[]"), f_deployment
        )

        if not schedule_actions:
            continue

        deployments.append([deployment.metadata["name"]])
        scaling_dict[f_deployment] = schedule_actions
    if not deployments:
        logging.info("No deployment is configured for schedule scaling")

    return scaling_dict


def parse_schedules(
    schedules: str, identifier: tuple[str, str]
) -> list[dict[str, str]]:
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


def process_deployment(deployment: tuple[str, str], schedules: list[dict]) -> None:
    """Determine actions to run for the given deployment and list of schedules"""
    namespace, name = deployment
    for schedule in schedules:
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

        schedule_timezone = schedule.get("tz", None)
        logging.debug("%s %s", deployment, schedule)

        # if less than 60 seconds have passed from the trigger
        if get_delta_sec(schedule_expr, schedule_timezone) < 60:
            if replicas is not None:
                scale_deployment(name, namespace, replicas)
            if min_replicas is not None or max_replicas is not None:
                scale_hpa(name, namespace, min_replicas, max_replicas)


def scale_deployment(name, namespace, replicas):
    """Scale the deployment to the given number of replicas"""
    try:
        deployment = (
            pykube.Deployment.objects(api).filter(namespace=namespace).get(name=name)
        )
    except pykube.exceptions.ObjectDoesNotExist:
        logging.warning("Deployment %s/%s does not exist", namespace, name)
        return

    if replicas is None or replicas == deployment.replicas:
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


if __name__ == "__main__":
    logging.info("Main loop started")
    while True:
        global api
        api = get_kube_api()

        logging.debug("Waiting until the next minute")
        sleep(get_wait_sec())
        logging.debug("Getting deployments")
        for d, s in deployments_to_scale().items():
            process_deployment(d, s)
