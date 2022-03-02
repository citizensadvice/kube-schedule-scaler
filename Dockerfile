FROM python:3.8-alpine

RUN apk add gcc libc-dev

RUN pip install --no-cache-dir pykube croniter

RUN adduser -u 1000 -D app && \
    mkdir /app && \
    chown app: /app

USER 1000
WORKDIR /app

COPY schedule_scaling/ /app/

CMD ["python", "-u", "main.py"]
