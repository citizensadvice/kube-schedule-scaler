from queue import Queue
from unittest.mock import patch

import pytest
from freezegun import freeze_time
from kubernetes.client.models import V1Deployment, V1ObjectMeta


@pytest.fixture
def ds():
    return DeploymentStore()


with (
    patch("kubernetes.config.load_incluster_config"),
    patch("kubernetes.config.load_kube_config"),
):
    from schedule_scaling.main import (
        DeploymentStore,
        ScaleTarget,
        get_delta_sec,
        parse_schedules,
        process_deployment,
        process_watch_event,
        scale_deployment,
        scale_hpa,
    )


def create_mock_event(event_type, name, namespace, annotations=None):
    """Helper to create a mock watch event."""
    return {
        "type": event_type,
        "object": V1Deployment(
            metadata=V1ObjectMeta(
                name=name,
                namespace=namespace,
                annotations=annotations or {},
                resource_version="123",
            )
        ),
    }


def test_parse_schedules_valid():
    raw_json = '[{"schedule": "0 9 * * *", "replicas": "5"}]'
    identifier = ("default", "my-app")
    result = parse_schedules(raw_json, identifier)
    assert len(result) == 1
    assert result[0]["replicas"] == "5"
    assert result[0]["schedule"] == "0 9 * * *"


def test_parse_schedules_invalid():
    raw_json = "invalid-json"
    identifier = ("default", "my-app")
    result = parse_schedules(raw_json, identifier)
    assert result == []


# Exactly 30 seconds after the cron trigger
@freeze_time("2024-01-01 09:00:30")
def test_get_delta_sec_triggered():
    schedule = "0 9 * * *"
    delta = get_delta_sec(schedule)
    assert delta == 30


# 90 seconds after the trigger
@freeze_time("2024-01-01 09:01:30")
def test_get_delta_sec_not_triggered():
    schedule = "0 9 * * *"
    delta = get_delta_sec(schedule)
    assert delta == 90


def test_process_event_added_with_schedule(ds):
    schedule_json = '[{"schedule": "0 9 * * *", "replicas": 5}]'
    event = create_mock_event(
        "ADDED", "app-1", "default", {"zalando.org/schedule-actions": schedule_json}
    )

    process_watch_event(ds, event)

    key = ("default", "app-1")
    assert key in ds.deployments
    assert ds.deployments[key][0]["replicas"] == 5


def test_process_event_modified_updates_store(ds):
    key = ("default", "app-1")
    # Initial state
    ds.deployments[key] = [{"schedule": "old", "replicas": 1}]

    # Update event
    new_json = '[{"schedule": "new", "replicas": 10}]'
    event = create_mock_event(
        "MODIFIED", "app-1", "default", {"zalando.org/schedule-actions": new_json}
    )

    process_watch_event(ds, event)

    assert ds.deployments[key][0]["replicas"] == 10


def test_process_event_deleted_removes_from_store(ds):
    # PRE-CONDITION: Manually seed the store
    key = ("default", "app-1")
    ds.deployments[key] = [{"schedule": "* * * * *", "replicas": 1}]

    # Verify it is actually there first (Sanity check)
    assert key in ds.deployments

    # ACTION: Process the DELETED event
    event = create_mock_event("DELETED", "app-1", "default")
    process_watch_event(ds, event)

    # POST-CONDITION: Verify it was removed
    assert key not in ds.deployments


def test_process_event_annotation_removed(ds):
    # PRE-CONDITION: Deployment exists with a schedule
    key = ("default", "app-1")
    ds.deployments[key] = [{"schedule": "0 9 * * *", "replicas": 5}]

    assert key in ds.deployments

    # ACTION: Process a MODIFIED event where the annotation is missing
    # (Simulates a user running 'kubectl annotate deploy app-1 zalando.org/schedule-actions-')
    event = create_mock_event("MODIFIED", "app-1", "default", annotations={})

    process_watch_event(ds, event)

    # POST-CONDITION: The store should now be empty for this key
    assert key not in ds.deployments


def test_process_event_bookmark(ds):
    # Bookmarks look like this in the watch stream
    event = {
        "type": "BOOKMARK",
        "object": {"kind": "Deployment", "metadata": {"resourceVersion": "999"}},
    }

    # This should not crash and should not modify deployments
    process_watch_event(ds, event)
    assert len(ds.deployments) == 0


@freeze_time("2024-01-01 09:00:10")
def test_process_deployment_queue_deployment():
    queue = Queue()

    deployment_key = ("prod", "web")
    # Only replicas is set
    schedule_actions = [{"schedule": "0 9 * * *", "replicas": "10"}]

    process_deployment(deployment_key, schedule_actions, queue)

    assert queue.qsize() == 1
    item = queue.get()
    assert item == (ScaleTarget.DEPLOYMENT, "web", "prod", 10)


@freeze_time("2024-01-01 09:00:10")
def test_process_deployment_queue_hpa():
    queue = Queue()

    deployment_key = ("prod", "web")
    # Only minReplicas and maxReplicas are set
    schedule_actions = [
        {"schedule": "0 9 * * *", "minReplicas": "2", "maxReplicas": "20"}
    ]

    process_deployment(deployment_key, schedule_actions, queue)

    assert queue.qsize() == 1
    item = queue.get()
    assert item == (ScaleTarget.HORIZONAL_POD_AUTOSCALER, "web", "prod", 2, 20)


@freeze_time("2024-01-01 08:55:00")
def test_process_deployment_too_early_no_queue():
    queue = Queue()
    deployment_key = ("prod", "web")
    schedule_actions = [{"schedule": "0 9 * * *", "replicas": "10"}]

    process_deployment(deployment_key, schedule_actions, queue)

    # Nothing should be in the queue because delta is 5 mins
    assert queue.qsize() == 0


@patch("schedule_scaling.main.apps_v1.patch_namespaced_deployment_scale")
def test_scale_deployment_calls_api(mock_patch):
    scale_deployment("my-deploy", "my-ns", 3)

    # Verify the k8s python client was called with correct body
    mock_patch.assert_called_once_with(
        name="my-deploy", namespace="my-ns", body={"spec": {"replicas": 3}}
    )


@patch(
    "schedule_scaling.main.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler"
)
def test_scale_hpa_calls_api(mock_patch):
    from schedule_scaling.main import scale_hpa

    # Test case: both min and max replicas provided
    scale_hpa("my-hpa", "my-ns", min_replicas=2, max_replicas=10)

    # Verify the HPA client was called with the correct nested spec body
    mock_patch.assert_called_once_with(
        name="my-hpa",
        namespace="my-ns",
        body={"spec": {"minReplicas": 2, "maxReplicas": 10}},
    )


@patch(
    "schedule_scaling.main.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler"
)
def test_scale_hpa_partial_patch_min_replicas(mock_patch):
    # Test case: Only min_replicas is provided
    scale_hpa("partial-hpa", "my-ns", min_replicas=5, max_replicas=None)

    # Verify that the patch body only contains the provided field
    mock_patch.assert_called_once_with(
        name="partial-hpa", namespace="my-ns", body={"spec": {"minReplicas": 5}}
    )


@patch(
    "schedule_scaling.main.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler"
)
def test_scale_hpa_partial_patch_max_replicas(mock_patch):
    # Test case: Only max_replicas is provided
    scale_hpa("partial-hpa", "my-ns", min_replicas=None, max_replicas=5)

    # Verify that the patch body only contains the provided field
    mock_patch.assert_called_once_with(
        name="partial-hpa", namespace="my-ns", body={"spec": {"maxReplicas": 5}}
    )


def test_deployment_store_lock():
    ds = DeploymentStore()
    assert ds.deployments == {}
    # Ensure lock exists
    assert not ds.lock.locked()
    with ds.lock:
        assert ds.lock.locked()
