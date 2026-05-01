FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    websocket-client \
    kafka-python \
    requests \
    prometheus-client

COPY producer.py .

CMD ["python", "producer.py"]
