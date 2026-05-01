import json
import time
import logging
import websocket
from kafka import KafkaProducer
from prometheus_client import Gauge, Counter, Histogram, start_http_server

# ── LOGGING ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)

KAFKA_BROKER = "kafka:29092"
PAIRS = ["btcusdt", "ethusdt"]

# ── MÉTRICAS PROMETHEUS (3 métricas de negocio) ────────
# Métrica 1: Precio actual por par
precio_actual = Gauge(
    'crypto_precio_actual',
    'Precio actual del par en USD',
    ['par']
)
# Métrica 2: Total de mensajes publicados en Kafka
mensajes_publicados = Counter(
    'pipeline_mensajes_publicados_total',
    'Total de mensajes publicados en Kafka',
    ['topic']
)
# Métrica 3: Variación porcentual 24h
variacion_pct = Gauge(
    'crypto_variacion_24h_pct',
    'Variación porcentual del precio en las últimas 24h',
    ['par']
)
# Métrica 4: Volumen 24h
volumen_24h = Gauge(
    'crypto_volumen_24h',
    'Volumen negociado en las últimas 24h',
    ['par']
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

# ── WEBSOCKET HANDLERS ─────────────────────────────────
def on_message(ws, message, producer):
    try:
        data = json.loads(message)
        stream = data.get("stream", "")

        # ── miniTicker ──
        if "miniTicker" in stream:
            ticker = data["data"]
            symbol = ticker["s"].lower()
            topic = f"{symbol}-ticker"

            change = round(
                (float(ticker["c"]) - float(ticker["o"])) / float(ticker["o"]) * 100, 4
            )

            payload = {
                "symbol":     ticker["s"],
                "price":      float(ticker["c"]),
                "volume_24h": float(ticker["v"]),
                "change_pct": change,
                "timestamp":  ticker["E"],
            }

            producer.send(topic, key=ticker["s"], value=payload)

            # Actualizar métricas Prometheus
            precio_actual.labels(par=ticker["s"]).set(float(ticker["c"]))
            variacion_pct.labels(par=ticker["s"]).set(change)
            volumen_24h.labels(par=ticker["s"]).set(float(ticker["v"]))
            mensajes_publicados.labels(topic=topic).inc()

            log.info(f"📤 [{topic}] {payload['symbol']} → ${payload['price']:,.2f} ({change:+.2f}%)")

        # ── kline_1m ──
        elif "kline" in stream:
            kline = data["data"]["k"]
            payload = {
                "symbol":    kline["s"],
                "open":      float(kline["o"]),
                "high":      float(kline["h"]),
                "low":       float(kline["l"]),
                "close":     float(kline["c"]),
                "volume":    float(kline["v"]),
                "closed":    kline["x"],
                "timestamp": kline["t"],
            }

            producer.send("crypto-klines", key=kline["s"], value=payload)
            mensajes_publicados.labels(topic="crypto-klines").inc()

            if kline["x"]:
                log.info(f"🕯️  [crypto-klines] {payload['symbol']} vela cerrada → ${payload['close']:,.2f}")

    except Exception as e:
        log.error(f"❌ Error procesando mensaje: {e}")

def on_error(ws, error):
    log.error(f"❌ WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    log.warning("🔌 WebSocket cerrado, reconectando en 10s...")
    time.sleep(10)

def on_open(ws):
    log.info("🔗 WebSocket conectado a Binance")

# ── MAIN ───────────────────────────────────────────────
def main():
    # Arrancar servidor HTTP de Prometheus en puerto 8000
    start_http_server(8000)
    log.info("📊 Servidor Prometheus arrancado en puerto 8000")

    producer = create_producer()

    streams = []
    for pair in PAIRS:
        streams.append(f"{pair}@miniTicker")
        streams.append(f"{pair}@kline_1m")

    url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"
    log.info(f"🌐 Conectando a: {url}")

    while True:
        try:
            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=lambda ws, msg: on_message(ws, msg, producer),
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever()
        except Exception as e:
            log.error(f"❌ Error inesperado: {e}, reconectando en 10s...")
            time.sleep(10)

if __name__ == "__main__":
    main()
