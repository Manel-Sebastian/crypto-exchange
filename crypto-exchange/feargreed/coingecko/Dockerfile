FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    requests \
    kafka-python \
    prometheus-client

COPY coingecko_producer.py .

CMD ["python", "coingecko_producer.py"]
