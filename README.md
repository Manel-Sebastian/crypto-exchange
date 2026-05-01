# 🚀 Crypto Exchange Pipeline

Pipeline completo de ingesta, procesamiento y visualización de datos del mercado de criptomonedas en tiempo real, construido sobre tecnologías Big Data y orquestado con Docker Compose.

## 📐 Arquitectura

```
Binance WebSocket ──────────────────────────────┐
CoinGecko REST API (cada 30s) ──────────────────┤
                                                 ▼
                                    ┌─────────────────────┐
                                    │    Apache Kafka      │
                                    │  (bus de eventos)    │
                                    └──────────┬──────────┘
                                               ▼
                                    ┌─────────────────────┐
                                    │   Spark Streaming    │
                                    │  SMA | Pump & Dump   │
                                    │  Desviación BTC      │
                                    └────────┬────────┬───┘
                                             │        │
                              ┌──────────────┘        └──────────────┐
                              ▼                                       ▼
                   ┌──────────────────┐                  ┌──────────────────┐
                   │   HDFS Parquet   │                  │   Prometheus     │
                   │ (histórico)      │                  │   + Grafana      │
                   └──────────────────┘                  └──────────────────┘
```

## 🛠️ Stack tecnológico

| Componente | Tecnología | Versión |
|---|---|---|
| Fuente datos 1 | Binance WebSocket | API pública |
| Fuente datos 2 | CoinGecko REST | API pública |
| Bus de eventos | Apache Kafka | 7.5.0 |
| Procesamiento | Apache Spark Streaming | 3.5.0 |
| Almacenamiento | HDFS + Parquet | Hadoop 3.2.1 |
| Monitorización | Prometheus + Grafana | Latest |
| Infraestructura | Docker + Docker Compose | v2.x |

## 📁 Estructura del proyecto

```
crypto-exchange/
├── docker-compose.yml          # Orquestación completa
├── README.md
├── memoria.pdf                 # Documentación técnica
│
├── producer/                   # Productor Binance WebSocket
│   ├── Dockerfile
│   └── producer.py
│
├── coingecko/                  # Productor CoinGecko REST
│   ├── Dockerfile
│   └── coingecko_producer.py
│
├── spark/                      # Job de Spark Streaming
│   ├── Dockerfile
│   └── streaming_job.py
│
├── prometheus/                 # Configuración de Prometheus
│   ├── prometheus.yml
│   └── alerts.yml
│
└── grafana/                    # Dashboards de Grafana
    └── provisioning/
        ├── datasources/
        │   └── prometheus.yml
        └── dashboards/
            ├── dashboards.yml
            ├── infraestructura.json
            └── negocio.json
```

## ⚡ Arranque rápido

### Requisitos previos

- Docker Engine 24+
- Docker Compose v2+
- 8 GB RAM recomendados

### 1. Clonar el repositorio

```bash
git clone https://github.com/TU_USUARIO/crypto-exchange.git
cd crypto-exchange
```

### 2. Levantar el cluster completo

```bash
docker compose up -d
```

### 3. Crear los topics de Kafka (primera vez)

```bash
# Esperar 15s a que Kafka arranque
sleep 15

docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic btcusdt-ticker --partitions 1 --replication-factor 1

docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic ethusdt-ticker --partitions 1 --replication-factor 1

docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic crypto-klines --partitions 1 --replication-factor 1

docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic coingecko-market --partitions 1 --replication-factor 1
```

### 4. Crear directorios en HDFS (primera vez)

```bash
# Esperar a que HDFS arranque
sleep 30

docker exec namenode hdfs dfs -mkdir -p /crypto/tickers
docker exec namenode hdfs dfs -mkdir -p /crypto/klines
docker exec namenode hdfs dfs -mkdir -p /crypto/coingecko
docker exec namenode hdfs dfs -mkdir -p /checkpoints/tickers
docker exec namenode hdfs dfs -mkdir -p /checkpoints/klines
docker exec namenode hdfs dfs -mkdir -p /checkpoints/coingecko
docker exec namenode hdfs dfs -chmod -R 777 /crypto
docker exec namenode hdfs dfs -chmod -R 777 /checkpoints
```

## 🌐 Servicios disponibles

| Servicio | URL | Credenciales |
|---|---|---|
| Kafka UI | http://localhost:8080 | Sin autenticación |
| Spark Master | http://localhost:8081 | Sin autenticación |
| HDFS NameNode | http://localhost:9870 | Sin autenticación |
| Prometheus | http://localhost:9090 | Sin autenticación |
| Grafana | http://localhost:3000 | admin / admin |
| Métricas Productor | http://localhost:8000/metrics | Sin autenticación |
| Métricas CoinGecko | http://localhost:8001/metrics | Sin autenticación |

## 📊 Indicadores calculados

### Fase 1 — Obligatorio

| Indicador | Ventana | Deslizamiento | Descripción |
|---|---|---|---|
| SMA (Media Móvil) | 5 minutos | 1 minuto | Precio medio suavizado para detectar tendencia |
| Volatilidad | 5 minutos | 1 minuto | Desviación estándar del precio en la ventana |
| Pump & Dump | 2 minutos | 30 segundos | Alerta si variación > 2% en ventana corta |

### Fase 2 — Ampliación

| Indicador | Fuente | Descripción |
|---|---|---|
| Desviación Binance vs CoinGecko | Binance + CoinGecko | Diferencia % entre precio local y precio global |
| Market Cap | CoinGecko | Capitalización total del mercado |

## 🗄️ Estructura HDFS

```
/crypto/
├── tickers/
│   ├── par=BTCUSDT/
│   │   └── fecha=2026-05-01/
│   │       └── part-00000.snappy.parquet
│   └── par=ETHUSDT/
│       └── fecha=2026-05-01/
│           └── part-00000.snappy.parquet
├── klines/
│   ├── par=BTCUSDT/fecha=2026-05-01/
│   └── par=ETHUSDT/fecha=2026-05-01/
├── coingecko/
│   ├── par=BTCUSDT/fecha=2026-05-01/
│   └── par=ETHUSDT/fecha=2026-05-01/
```

## 📈 Métricas de negocio (Prometheus)

| Métrica | Tipo | Descripción |
|---|---|---|
| `crypto_precio_actual` | Gauge | Precio actual del par en USD |
| `crypto_variacion_24h_pct` | Gauge | Variación porcentual 24h |
| `crypto_volumen_24h` | Gauge | Volumen negociado 24h |
| `pipeline_mensajes_publicados_total` | Counter | Mensajes publicados en Kafka |
| `coingecko_market_cap_usd` | Gauge | Market cap global por coin |
| `coingecko_volumen_24h_usd` | Gauge | Volumen global 24h |

## 🚨 Alertas configuradas

| Alerta | Condición | Duración | Severidad |
|---|---|---|---|
| BTCPumpDetectado | variación 24h > 3% | 1 minuto | warning |
| BTCDumpDetectado | variación 24h < -3% | 1 minuto | critical |
| ProductorCaido | up{job="crypto-producer"} == 0 | 30 segundos | critical |

## 🔧 Comandos útiles

```bash
# Ver estado de todos los servicios
docker compose ps

# Ver logs en tiempo real de un servicio
docker logs -f producer
docker logs -f spark-job
docker logs -f coingecko

# Ver ficheros en HDFS
docker exec namenode hdfs dfs -ls -R /crypto

# Ver tamaño del histórico
docker exec namenode hdfs dfs -du -h /crypto

# Listar topics de Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Parar el cluster completo
docker compose down

# Parar y eliminar volúmenes (borra el histórico)
docker compose down -v
```


## ⚠️ Requisitos técnicos

- **Docker Compose obligatorio**: no se aceptan servicios instalados directamente en la máquina.
- **Factor de replicación HDFS = 1**: entorno de desarrollo con 1 DataNode. En producción se aumentaría a 3.
- **Sin API Keys**: todas las fuentes de datos son públicas y no requieren registro.
