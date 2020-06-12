#!/usr/bin/env python3
import pykube
import json
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
        print('%s - Error in parsing JSON %s with error' % (identifier, schedules), err)
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
        print("Deployment: %s, Namespace: %s, Replicas: %s, MinReplicas: %s, MaxReplicas: %s, Schedule: %s" % (name, namespace, replicas, min_replicas, max_replicas, schedule_expr))
        # if less than 60 seconds have passed from the trigger
        if get_delta_sec(schedule_expr) < 60:
            if replicas != None:
                run_deployment_action(name, namespace, int(replicas))
            if min_replicas or max_replicas:
                run_hpa_action(name, namespace, int(min_replicas), int(max_replicas))


def run_deployment_action(deployment_name, namespace, replicas):
    time = datetime.now().strftime("%d-%m-%Y %H:%M UTC")
    deployment = Deployment.objects(api).filter(namespace=namespace).get(name=deployment_name)

    if replicas != None:
        deployment.replicas = replicas
        deployment.update()
        if deployment.replicas == replicas:
            print("Deployment {} scaled to {} replicas at {}".format(deployment_name, replicas, time))
        else:
            print("Something went wrong... deployment {} has not been scaled".format(deployment_name))


def run_hpa_action(deployment_name, namespace, min_replicas, max_replicas):
    time = datetime.now().strftime("%d-%m-%Y %H:%M UTC")

    try:
        # NOTE: this assumes that the HorizontalPodAutoscaler is called the same as the Deployment object
        hpa = pykube.HorizontalPodAutoscaler.objects(api).filter(namespace=namespace).get(name=deployment_name)
    except Exception as e:
        print('HPA for deployment %(name)s in namespace %(namespace)s not found: {}'.format(e))
        return

    if hpa:
        if min_replicas != None:
            hpa.obj["spec"]["minReplicas"] = min_replicas
            hpa.update()

            if hpa.obj["spec"]["minReplicas"] == min_replicas:
                print('HPA {} has been adjusted to minReplicas to {} at {}'.format(deployment_name, min_replicas, time))
            else:
                print('Something went wrong... HPA {} has not been scaled'.format(deployment_name))

        if max_replicas != None:
            hpa.obj["spec"]["maxReplicas"] = max_replicas
            hpa.update()

            if hpa.obj["spec"]["maxReplicas"] == max_replicas:
                print('HPA {} has been adjusted to maxReplicas to {} at {}'.format(deployment_name, max_replicas, time))
            else:
                print('Something went wrong... HPA {} has not been scaled'.format(deployment_name))


if __name__ == "__main__":
    print("Main loop started")
    while True:
        print("Getting deployments")
        for deployment, schedules in deployments_to_scale().items():
            process_deployment(deployment, schedules)
        print("Waiting 50 seconds")
        sleep(50)
