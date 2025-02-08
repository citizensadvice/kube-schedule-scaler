FROM python:3.13-alpine

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

RUN adduser -u 1000 -D app && \
    mkdir /app && \
    chown app: /app

USER 1000
WORKDIR /app

COPY schedule_scaling/ /app/

CMD ["python", "-u", "main.py"]
