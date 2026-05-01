import json
import time
import logging
import requests
from kafka import KafkaProducer
from prometheus_client import Gauge, Counter, start_http_server

# ── LOGGING ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)

KAFKA_BROKER = "kafka:29092"
POLL_INTERVAL = 30  # segundos entre cada petición
COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/simple/price"
    "?ids=bitcoin,ethereum"
    "&vs_currencies=usd"
    "&include_market_cap=true"
    "&include_24hr_vol=true"
    "&include_24hr_change=true"
)

# ── MÉTRICAS PROMETHEUS ────────────────────────────────
market_cap = Gauge(
    'coingecko_market_cap_usd',
    'Market cap en USD',
    ['coin']
)
volumen_global = Gauge(
    'coingecko_volumen_24h_usd',
    'Volumen global 24h en USD',
    ['coin']
)
precio_global = Gauge(
    'coingecko_precio_usd',
    'Precio global agregado por CoinGecko',
    ['coin']
)
peticiones_ok = Counter(
    'coingecko_peticiones_ok_total',
    'Total de peticiones exitosas a CoinGecko'
)
peticiones_error = Counter(
    'coingecko_peticiones_error_total',
    'Total de peticiones fallidas a CoinGecko'
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

# ── POLLING COINGECKO ──────────────────────────────────
def fetch_coingecko():
    """Hace una petición REST a CoinGecko y devuelve los datos."""
    try:
        response = requests.get(COINGECKO_URL, timeout=10)
        response.raise_for_status()
        peticiones_ok.inc()
        return response.json()
    except Exception as e:
        peticiones_error.inc()
        log.error(f"❌ Error llamando a CoinGecko: {e}")
        return None

def parse_and_publish(data, producer):
    """Parsea la respuesta y publica en Kafka."""
    mapping = {
        "bitcoin": "BTCUSDT",
        "ethereum": "ETHUSDT"
    }

    for coin, symbol in mapping.items():
        if coin not in data:
            continue

        payload = {
            "symbol":       symbol,
            "coin":         coin,
            "price_usd":    data[coin].get("usd", 0),
            "market_cap":   data[coin].get("usd_market_cap", 0),
            "volume_24h":   data[coin].get("usd_24h_vol", 0),
            "change_24h":   data[coin].get("usd_24h_change", 0),
            "timestamp":    int(time.time() * 1000),
        }

        producer.send("coingecko-market", key=symbol, value=payload)

        # Actualizar métricas Prometheus
        market_cap.labels(coin=symbol).set(payload["market_cap"])
        volumen_global.labels(coin=symbol).set(payload["volume_24h"])
        precio_global.labels(coin=symbol).set(payload["price_usd"])

        log.info(
            f"📤 [coingecko-market] {symbol} → "
            f"${payload['price_usd']:,.2f} | "
            f"MarketCap: ${payload['market_cap']:,.0f} | "
            f"Vol24h: ${payload['volume_24h']:,.0f}"
        )

# ── MAIN ───────────────────────────────────────────────
def main():
    start_http_server(8001)
    log.info("📊 Servidor Prometheus arrancado en puerto 8001")

    producer = create_producer()

    log.info(f"🌐 Iniciando polling a CoinGecko cada {POLL_INTERVAL}s...")

    while True:
        data = fetch_coingecko()
        if data:
            parse_and_publish(data, producer)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
