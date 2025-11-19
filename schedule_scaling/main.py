#!/usr/bin/env python3
""" Main module of kube-schedule-scaler """
import os
import json
import logging
import dateutil.tz
from datetime import datetime, timedelta
from time import sleep

import pykube
from croniter import croniter


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    datefmt='%d-%m-%Y %H:%M:%S'
)


def get_kube_api():
    """ Initiating the API from Service Account or when running locally from ~/.kube/config """
    return pykube.HTTPClient(pykube.KubeConfig.from_env())


def deployments_to_scale():
    """ Getting the deployments configured for schedule scaling """
    deployments = []
    scaling_dict = {}
    for namespace in list(pykube.Namespace.objects(api)):
        namespace = str(namespace)
        for deployment in pykube.Deployment.objects(api).filter(namespace=namespace):
            annotations = deployment.metadata.get("annotations", {})
            f_deployment = str(namespace + "/" + str(deployment))

            schedule_actions = parse_schedules(annotations.get(
                "zalando.org/schedule-actions", "[]"), f_deployment)

            if schedule_actions is None or len(schedule_actions) == 0:
                continue

            deployments.append([deployment.metadata["name"]])
            scaling_dict[f_deployment] = schedule_actions
    if not deployments:
        logging.info("No deployment is configured for schedule scaling")

    return scaling_dict


def parse_schedules(schedules, identifier):
    """ Parse the JSON schedule """
    try:
        return json.loads(schedules)
    except (TypeError, json.decoder.JSONDecodeError) as err:
        logging.error("%s - Error in parsing JSON %s", identifier, schedules)
        logging.exception(err)
        return []


def get_delta_sec(schedule, timezone_name=None):
    """ Returns the number of seconds passed since last occurence of the given cron expression """
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
    now = now.timestamp()
    # return the delta
    return now - time


def get_wait_sec():
    """ Return the number of seconds to wait before the next minute """
    now = datetime.now()
    future = datetime(now.year, now.month, now.day, now.hour, now.minute) + timedelta(minutes=1)
    return (future - now).total_seconds()


def process_deployment(deployment, schedules):
    """ Determine actions to run for the given deployment and list of schedules """
    namespace, name = deployment.split("/")
    for schedule in schedules:
        # when provided, convert the values to int
        replicas = schedule.get("replicas", None)
        if replicas:
            replicas = int(replicas)
        min_replicas = schedule.get("minReplicas", None)
        if min_replicas:
            min_replicas = int(min_replicas)
        max_replicas = schedule.get("maxReplicas", None)
        if max_replicas:
            max_replicas = int(max_replicas)

        schedule_expr = schedule.get("schedule", None)
        schedule_timezone = schedule.get("tz", None)
        logging.debug("%s %s", deployment, schedule)

        # if less than 60 seconds have passed from the trigger
        if get_delta_sec(schedule_expr, schedule_timezone) < 60:
            # replicas might equal 0 so we check that is not None
            if replicas is not None:
                scale_deployment(name, namespace, replicas)
            # these can't be 0 by definition so checking for existence is enough
            if min_replicas or max_replicas:
                scale_hpa(name, namespace, min_replicas, max_replicas)


def scale_deployment(name, namespace, replicas):
    """ Scale the deployment to the given number of replicas """
    try:
        deployment = pykube.Deployment.objects(api).filter(
            namespace=namespace).get(name=name)
    except pykube.exceptions.ObjectDoesNotExist:
        logging.warning("Deployment %s/%s does not exist", namespace, name)
        return

    if replicas is None or replicas == deployment.replicas:
        return

    # Retry logic: attempt up to 3 times with 2 second intervals
    max_retries = 3
    retry_delay = 2
    for attempt in range(max_retries):
        try:
            try:
                try:
                    deployment.patch({"spec": {"replicas": replicas}}, subresource="scale")
                finally:
                    deployment.reload()  # reload to fetch whole updated Deployment
            except pykube.exceptions.HTTPError as e:
                # XXX: In previous version deployment.update() was always used. It was unnecessary, but after moving to a
                # subresource patch we've lost backward compatibility, as previous RBAC "patch" on "deployments" doesn't
                # work. So if we get an 403 pring a warning and try again, this time with full resource update.
                if e.code == 403:
                    logging.warning(
                            "Failed to apply patch on a 'scale' subresource, failing back to a full update. "
                            "Consider upgrading RBAC deployment.")
                    deployment.replicas = replicas
                    deployment.update()
                else:
                    raise
            logging.info("Deployment %s/%s scaled to %s replicas", namespace, name, replicas)
            break  # Success, exit retry loop
        except pykube.exceptions.HTTPError as err:
            if attempt < max_retries - 1:
                logging.warning("Exception raised while updating deployment %s/%s (attempt %d/%d). Retrying in %d seconds...",
                               namespace, name, attempt + 1, max_retries, retry_delay)
                logging.exception(err)
                sleep(retry_delay)
            else:
                logging.error("Exception raised while updating deployment %s/%s after %d attempts. Giving up.",
                             namespace, name, max_retries)
                logging.exception(err)


def scale_hpa(name, namespace, min_replicas, max_replicas):
    """ Adjust hpa min and max number of replicas """
    try:
        hpa = pykube.HorizontalPodAutoscaler.objects(
            api).filter(namespace=namespace).get(name=name)
    except pykube.exceptions.ObjectDoesNotExist:
        logging.warning("HPA %s/%s does not exist", namespace, name)
        return

    # return if no values are provided
    if not min_replicas and not max_replicas:
        return

    # return when both are provided but hpa is already up-to-date
    if (hpa.obj["spec"]["minReplicas"] == min_replicas and
            hpa.obj["spec"]["maxReplicas"] == max_replicas):
        return

    # return when only one of them is provided but hpa is already up-to-date
    if ((not min_replicas and max_replicas == hpa.obj["spec"]["maxReplicas"]) or
            (not max_replicas and min_replicas == hpa.obj["spec"]["minReplicas"])):
        return

    if min_replicas:
        hpa.obj["spec"]["minReplicas"] = min_replicas

    if max_replicas:
        hpa.obj["spec"]["maxReplicas"] = max_replicas

    # Retry logic: attempt up to 3 times with 2 second intervals
    max_retries = 3
    retry_delay = 2
    for attempt in range(max_retries):
        try:
            hpa.update()
            if min_replicas:
                logging.info("HPA %s/%s minReplicas set to %s", namespace, name, min_replicas)
            if max_replicas:
                logging.info("HPA %s/%s maxReplicas set to %s", namespace, name, max_replicas)
            break  # Success, exit retry loop
        except pykube.exceptions.HTTPError as err:
            if attempt < max_retries - 1:
                logging.warning("Exception raised while updating HPA %s/%s (attempt %d/%d). Retrying in %d seconds...",
                               namespace, name, attempt + 1, max_retries, retry_delay)
                logging.exception(err)
                sleep(retry_delay)
            else:
                logging.error("Exception raised while updating HPA %s/%s after %d attempts. Giving up.",
                             namespace, name, max_retries)
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
