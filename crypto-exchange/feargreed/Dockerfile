FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    requests \
    kafka-python \
    prometheus-client

COPY feargreed_producer.py .

CMD ["python", "feargreed_producer.py"]
