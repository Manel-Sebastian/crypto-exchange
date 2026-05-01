import json
import time
import logging
import requests
from kafka import KafkaProducer
from prometheus_client import Gauge, start_http_server

# ── LOGGING ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)

KAFKA_BROKER = "kafka:29092"
POLL_INTERVAL = 3600  # cada hora (el índice se actualiza 1 vez al día)
FNG_URL = "https://api.alternative.me/fng/?limit=1"

# ── MÉTRICAS PROMETHEUS ────────────────────────────────
fear_greed_value = Gauge(
    'crypto_fear_greed_index',
    'Valor del Fear & Greed Index (0-100)'
)

# ── KAFKA PRODUCER ─────────────────────────────────────
def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            log.info("✅ Conectado a Kafka correctamente")
            return producer
        except Exception as e:
            log.warning(f"⏳ Kafka no disponible, reintentando en 5s... ({e})")
            time.sleep(5)

# ── CLASIFICACIÓN ──────────────────────────────────────
def classify(value):
    if value <= 24:
        return "Extreme Fear"
    elif value <= 49:
        return "Fear"
    elif value <= 74:
        return "Greed"
    else:
        return "Extreme Greed"

# ── POLLING ────────────────────────────────────────────
def fetch_and_publish(producer):
    try:
        response = requests.get(FNG_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        entry = data["data"][0]
        value = int(entry["value"])
        classification = entry["value_classification"]
        timestamp = int(entry["timestamp"])

        payload = {
            "value":          value,
            "classification": classification,
            "timestamp":      timestamp,
            "timestamp_ms":   int(time.time() * 1000),
        }

        producer.send("feargreed-index", key="FNG", value=payload)
        fear_greed_value.set(value)

        log.info(
            f"📤 [feargreed-index] Fear & Greed: {value} "
            f"→ {classification}"
        )

    except Exception as e:
        log.error(f"❌ Error llamando a Fear & Greed API: {e}")

# ── MAIN ───────────────────────────────────────────────
def main():
    start_http_server(8002)
    log.info("📊 Servidor Prometheus arrancado en puerto 8002")

    producer = create_producer()

    log.info(f"🌐 Iniciando polling al Fear & Greed Index cada {POLL_INTERVAL}s...")

    while True:
        fetch_and_publish(producer)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
