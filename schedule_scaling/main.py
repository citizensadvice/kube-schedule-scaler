#!/usr/bin/env python3
""" Main module of kube-schedule-scaler """
import os
import json
import logging
from datetime import datetime, timezone, timedelta
from time import sleep

import pykube
from croniter import croniter
from resources import Deployment


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    datefmt='%d-%m-%Y %H:%M:%S'
)


def get_kube_api():
    """ Initiating the API from Service Account or when running locally from ~/.kube/config """
    try:
        config = pykube.KubeConfig.from_service_account()
    except FileNotFoundError:
        # local testing
        config = pykube.KubeConfig.from_file(
            os.path.expanduser("~/.kube/config"))
    return pykube.HTTPClient(config)


api = get_kube_api()


def deployments_to_scale():
    """ Getting the deployments configured for schedule scaling """
    deployments = []
    scaling_dict = {}
    for namespace in list(pykube.Namespace.objects(api)):
        namespace = str(namespace)
        for deployment in Deployment.objects(api).filter(namespace=namespace):
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


def get_delta_sec(schedule, now):
    """ Returns the number of seconds passed since last occurence of the given cron expression """
    # get the last previous occurrence of the cron expression, add 1 second here to handle cases
    # when schedule should be executed now and delta is 0, now is rounded to full minutes, so it's
    # ok anyway
    time = croniter(schedule, now + timedelta(seconds=1)).get_prev()
    # convert now to unix timestamp
    now = now.replace(tzinfo=timezone.utc).timestamp()
    # return the delta
    return now - time


def round_to_minute(time):
    return datetime(time.year, time.month, time.day, time.hour, time.minute)


def get_wait_sec():
    """ Return the number of seconds to wait before the next minute """
    now = datetime.now()
    future = round_to_minute(now) + timedelta(minutes=1)
    return (future - now).total_seconds()


def process_deployment(deployment, schedules, now, schedule_window_sec):
    """ Determine actions to run for the given deployment and list of schedules """
    # newest cron settings to be applied, tuples with (delta seconds, desired replicas)
    last_replicas = None
    last_min_replicas = None
    last_max_replicas = None

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
        logging.debug("%s %s", deployment, schedule)

        delta_sec = get_delta_sec(schedule_expr, now)

        # if less than window seconds have passed from the trigger
        if delta_sec < schedule_window_sec:
            # replicas might equal 0 so we check that is not None
            if replicas is not None:
                if last_replicas is None or last_replicas[0] >= delta_sec:
                    last_replicas = (delta_sec, replicas)

            # these can't be 0 by definition so checking for existence is enough
            if min_replicas:
                if last_min_replicas is None or last_min_replicas[0] >= delta_sec:
                    last_min_replicas = (delta_sec, min_replicas)
            if max_replicas:
                if last_max_replicas is None or last_max_replicas[0] >= delta_sec:
                    last_max_replicas = (delta_sec, max_replicas)

    if last_replicas:
        scale_deployment(name, namespace, last_replicas[1])

    if last_min_replicas or last_max_replicas:
        scale_hpa(
            name,
            namespace,
            None if last_min_replicas is None else last_min_replicas[1],
            None if last_max_replicas is None else last_max_replicas[1]
        )


def scale_deployment(name, namespace, replicas):
    """ Scale the deployment to the given number of replicas """
    try:
        deployment = Deployment.objects(api).filter(
            namespace=namespace).get(name=name)
    except pykube.exceptions.ObjectDoesNotExist:
        logging.warning("Deployment %s/%s does not exist", namespace, name)
        return

    if replicas is None or replicas == deployment.replicas:
        return
    deployment.replicas = replicas

    try:
        deployment.update()
        logging.info("Deployment %s/%s scaled to %s replicas", namespace, name, replicas)
    except pykube.exceptions.HTTPError as err:
        logging.error("Exception raised while updating deployment %s/%s", namespace, name)
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

    try:
        hpa.update()
        if min_replicas:
            logging.info("HPA %s/%s minReplicas set to %s", namespace, name, min_replicas)
        if max_replicas:
            logging.info("HPA %s/%s maxReplicas set to %s", namespace, name, max_replicas)
    except pykube.exceptions.HTTPError as err:
        logging.error("Exception raised while updating HPA %s/%s", namespace, name)
        logging.exception(err)


if __name__ == "__main__":
    logging.info("Main loop started")
    last_process_time = round_to_minute(datetime.now())

    startup_schedule_window_sec = os.environ.get("STARTUP_SCHEDULE_WINDOW_SECONDS")
    if startup_schedule_window_sec:
        logging.info(
            "Running the first loop executing schedules old up to %s seconds",
            startup_schedule_window_sec)
        last_process_time = last_process_time - timedelta(seconds=int(startup_schedule_window_sec))

    while True:
        logging.debug("Waiting until the next minute")
        sleep(get_wait_sec())

        now = round_to_minute(datetime.now())
        schedule_window_sec = max(60, (now - last_process_time).total_seconds())
        last_process_time = now

        logging.debug("Getting deployments")
        for d, s in deployments_to_scale().items():
            process_deployment(d, s, now, schedule_window_sec)
