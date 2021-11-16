# Kubernetes Schedule Scaler

Kubernetes Schedule Scaler allows you to change the number of running replicas
of a Deployment at specific times. It can be used is to turn on/off
applications that don't need to be always available and reduce cluster resource
utilization, or to adjust the number of replicas when it's known in advance how
the traffic volume is distributed across different time periods.

## Installation

```
$ kubectl apply -f https://github.com/citizensadvice/kube-schedule-scaler/raw/master/deploy/deployment.yaml
```

## Usage

Just add the annotation to your `Deployment`:

```yaml
  annotations:
    zalando.org/schedule-actions: '[{"schedule": "10 18 * * *", "replicas": "3"}]'
```

The following fields are available:

- `schedule` - cron expression for the schedule
- `replicas` - the number of replicas to scale to
- `minReplicas` - in combination with an `HorizontalPodAutoscaler`, will adjust the min number of replicas
- `maxReplicas` - in combination with an `HorizontalPodAutoscaler`, will adjust the max number of replicas

### Deployment Example

```yaml
kind: Deployment
metadata:
  name: nginx-deployment
  labels:
    application: nginx-deployment
  annotations:
    zalando.org/schedule-actions: |
      [
        {"schedule": "0 7 * * Mon-Fri", "replicas": "1"},
        {"schedule": "0 19 * * Mon-Fri", "replicas": "0"},
        {"schedule": "0 12 * * Mon-Fri", "minReplicas": "2", "maxReplicas": "3"},
        {"schedule": "0 16 * * Mon-Fri", "minReplicas": "1"}
      ]
```

When your `Deployment` is not managed by an `HorizontalPodAutoscaler`, setting `replicas` to the desired number of replicas is sufficient.

When an `HorizontalPodAutoscaler` is managing the `Deployment`, it will ignore the Deployment if `replicas` is set to `0`, so setting `replicas` to and `1` and `0` acts as a switch on/off for the application, while `minReplicas` and `maxReplicas` can be used to adjust the desired number of replicas.

In order for the `HorizontalPodAutoscaler` to be detected, it must be called with the same name as the `Deployment` that manages.

## Configuration

Configuration is done using environment variables:

* `LOG_LEVEL` - change default log level, use `debug` to increase verbosity

* `STARTUP_SCHEDULE_WINDOW_SECONDS` - a number of seconds to look back during the first schedule after startup, by setting this to `900` you'll make cron-schedule-scaler execute any configured schedules that happened up to 15 minutes before - this is designed to protect against missing schedules due to kube-schedule-scaler downtime (e.g. restarts, deployments, etc)

## Debugging

If your scaling action has not been executed for some reason, you can check with the steps below:

```
$ kubectl get pods -n kube-schedule-scaler
NAME                                    READY   STATUS    RESTARTS   AGE
kube-schedule-scaler-844b6d5888-p9tc4   1/1     Running   0          3m12s
```

Check the logs for your specific deployment:
```
$ kubectl logs -n kube-schedule-scaler kube-schedule-scaler-844b6d5888-p9tc4 | grep -i 'nginx'
17-06-2020 20:11:00 INFO - main.py:131 - Deployment default/nginx scaled to 1 replicas
17-06-2020 20:12:00 INFO - main.py:169 - HPA default/nginx minReplicas set to 3
17-06-2020 20:12:00 INFO - main.py:171 - HPA default/nginx maxReplicas set to 4
17-06-2020 20:13:00 INFO - main.py:169 - HPA default/nginx minReplicas set to 1
17-06-2020 20:13:00 INFO - main.py:171 - HPA default/nginx maxReplicas set to 2
```

Check for specific deployment at specific time:

```
$ kubectl logs -n kube-schedule-scaler kube-schedule-scaler-844b6d5888-p9tc4 | grep -i 'nginx' | grep '20:12'
17-06-2020 20:12:00 INFO - main.py:169 - HPA default/nginx minReplicas set to 3
17-06-2020 20:12:00 INFO - main.py:171 - HPA default/nginx maxReplicas set to 4
```

You can change the log level using the `LOG_LEVEL` environment variable (e.g. `LOG_LEVEL=DEBUG`)
