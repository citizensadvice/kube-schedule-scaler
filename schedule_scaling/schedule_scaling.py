#!/usr/bin/env python3
import pykube
import json
import logging
from resources import Deployment
from datetime import datetime, timedelta, timezone
from time import sleep
from croniter import croniter
from resources import Deployment

def get_kube_api():
    """ Initiating the API from Service Account or when running locally from ~/.kube/config """
    try:
        config = pykube.KubeConfig.from_service_account()
    except FileNotFoundError:
        # local testing
        config = pykube.KubeConfig.from_file(os.path.expanduser("~/.kube/config"))
    api = pykube.HTTPClient(config)
    return api

api = get_kube_api()

def deployments_to_scale():
    """
    Getting the deployments configured for schedule scaling...
    """
    deployments = []
    scaling_dict = {}
    for namespace in list(pykube.Namespace.objects(api)):
        namespace = str(namespace)
        for deployment in Deployment.objects(api).filter(namespace=namespace):
            annotations = deployment.metadata.get("annotations", {})
            f_deployment = str(namespace + "/" + str(deployment))

            schedule_actions = parse_schedules(annotations.get("zalando.org/schedule-actions", "[]"), f_deployment)

            if schedule_actions is None or len(schedule_actions) == 0:
                continue

            deployments.append([deployment.metadata["name"]])
            scaling_dict[f_deployment] = schedule_actions
    if not deployments:
        logging.info("No deployment is configured for schedule scaling")

    return scaling_dict


def parse_schedules(schedules, identifier):
    try:
        return json.loads(schedules)
    except Exception as err:
        logging.error("%s - Error in parsing JSON %s with error" % (identifier, schedules), err)
        return []


def get_delta_sec(schedule):
    # get current time
    now = datetime.now()
    # get the last previous occurrence of the cron expression
    t = croniter(schedule, now).get_prev()
    # convert now to unix timestamp
    now = now.replace(tzinfo=timezone.utc).timestamp()
    # return the delta
    return now - t


def process_deployment(deployment, schedules):
    namespace, name = deployment.split("/")
    for schedule in schedules:
        replicas = schedule.get("replicas", None)
        min_replicas = schedule.get("minReplicas", None)
        max_replicas = schedule.get("maxReplicas", None)
        schedule_expr = schedule.get("schedule", None)
        logging.debug("Deployment: %s, Namespace: %s, Replicas: %s, MinReplicas: %s, MaxReplicas: %s, Schedule: %s" % (name, namespace, replicas, min_replicas, max_replicas, schedule_expr))
        # if less than 60 seconds have passed from the trigger
        if get_delta_sec(schedule_expr) < 60:
            if replicas != None:
                scale_deployment(name, namespace, int(replicas))
            if min_replicas or max_replicas:
                scale_hpa(name, namespace, int(min_replicas), int(max_replicas))


def scale_deployment(name, namespace, replicas):
    try:
        deployment = Deployment.objects(api).filter(namespace=namespace).get(name=name)
    except pykube.exceptions.ObjectDoesNotExist:
        logging.warning("Deployment {}/{} does not exist".format(namespace, name))
        return

    if replicas == None or replicas == deployment.replicas:
        return
    deployment.replicas = replicas

    time = datetime.now().strftime("%d-%m-%Y %H:%M UTC")
    try:
        deployment.update()
        logging.info("Deployment {}/{} scaled to {} replicas at {}".format(namespace, name, replicas, time))
    except Exception as e:
        logging.error("Exception raised while updating deployment {}/{}".format(namespace, name))
        logging.exception(e)


def update_hpa_field(hpa, field, value):
    if value == None or value == hpa.obj["spec"][field]:
        return
    hpa.obj["spec"][field] = value

    time = datetime.now().strftime("%d-%m-%Y %H:%M UTC")
    try:
        hpa.update()
        logging.info("HPA {}/{} {} set to {} at {}".format(hpa.namespace, hpa.name, field, value, time))
    except Exception as e:
        logging.error("Exception raised while updating HPA {}/{}".format(hpa.namespace, hpa.name))
        logging.exception(e)


def scale_hpa(name, namespace, min_replicas, max_replicas):

    try:
        hpa = pykube.HorizontalPodAutoscaler.objects(api).filter(namespace=namespace).get(name=name)
    except pykube.exceptions.ObjectDoesNotExist:
        logging.warning("HPA {}/{} does not exist".format(namespace, name))
        return

    if hpa:
        update_hpa_field(hpa, "minReplicas", min_replicas)
        update_hpa_field(hpa, "maxReplicas", max_replicas)


if __name__ == "__main__":
    logging.info("Main loop started")
    while True:
        logging.debug("Getting deployments")
        for deployment, schedules in deployments_to_scale().items():
            process_deployment(deployment, schedules)
        logging.debug("Waiting 50 seconds")
        sleep(50)
