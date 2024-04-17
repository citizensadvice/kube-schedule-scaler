from collections.abc import Sequence
import datetime
import json
from os import path
import subprocess
from time import sleep


def run(args: Sequence[str]) -> None:
    """Runs a subprocess and asserts that the exit code is 0"""
    assert subprocess.call(args) == 0


def setup_module(module):
    base_dir = path.dirname(path.realpath(__file__))
    # deploy the manifest
    run(["kubectl", "apply", "-f", f"{base_dir}/manifest.yml"])
    # wait for resources to be ready
    sleep(20)


def teardown_module(module):
    base_dir = path.dirname(path.realpath(__file__))
    # delete the resources
    run(["kubectl", "delete", "-f", f"{base_dir}/manifest.yml"])


def get_cron_formatted_time():
    '''Return the cron expr prefix for the next minute, e.g. "43 14"'''
    now = datetime.datetime.now(datetime.UTC)

    # get the minute and hour of the next minute from now
    return (now + datetime.timedelta(minutes=1)).strftime("%M %H")


def get_json_patch(schedule_actions):
    """Return the JSON string used to patch the Deployment objects"""
    return json.dumps(
        {
            "metadata": {
                "annotations": {
                    # schedule action must be a JSON string, so we encode it here
                    "zalando.org/schedule-actions": json.dumps(schedule_actions)
                }
            }
        }
    )


def patch_deployment(deployment_name, patch):
    """Inject the scheduler annotation in the target Deployment"""
    res = subprocess.run(
        [
            "kubectl",
            "patch",
            "-n",
            "test-kube-schedule-scaler",
            "deployment",
            deployment_name,
            "-p",
            patch,
        ]
    )
    assert res.returncode == 0


def test_deployment_scaling():
    """Check that schedule actions are applied to the Deployment object"""
    t = get_cron_formatted_time()
    schedule_actions = [{"schedule": f"{t} * * *", "replicas": "2"}]
    patch = get_json_patch(schedule_actions)
    patch_deployment("sleep", patch)

    # wait a minute
    sleep(60)

    # check the deployment number of replicas
    res = subprocess.check_output(
        [
            "kubectl",
            "get",
            "-n",
            "test-kube-schedule-scaler",
            "deployment",
            "sleep",
            "-o",
            "json",
        ]
    )

    data = json.loads(res)

    # check the number of replicas
    assert data["spec"]["replicas"] == 2


def test_hpa_scaling():
    """Check that schedule actions are applied to the relevant HPA for the Deployment object"""
    t = get_cron_formatted_time()
    schedule_actions = [
        {"schedule": f"{t} * * *", "minReplicas": "2", "maxReplicas": "3"}
    ]
    patch = get_json_patch(schedule_actions)
    patch_deployment("sleep-with-autoscaling", patch)

    # wait a minute
    sleep(60)

    # check the deployment number of replicas
    res = subprocess.check_output(
        [
            "kubectl",
            "get",
            "-n",
            "test-kube-schedule-scaler",
            "hpa",
            "sleep-with-autoscaling",
            "-o",
            "json",
        ]
    )

    data = json.loads(res)

    # check the number of replicas
    assert data["spec"]["minReplicas"] == 2
    assert data["spec"]["maxReplicas"] == 3
