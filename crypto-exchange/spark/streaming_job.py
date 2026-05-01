from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, avg, max, min, stddev,
    window, current_timestamp, when, round as spark_round,
    to_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    FloatType, LongType, BooleanType, DoubleType
)

# ── SCHEMAS ────────────────────────────────────────────
ticker_schema = StructType([
    StructField("symbol",     StringType(), True),
    StructField("price",      FloatType(),  True),
    StructField("volume_24h", FloatType(),  True),
    StructField("change_pct", FloatType(),  True),
    StructField("timestamp",  LongType(),   True),
])

kline_schema = StructType([
    StructField("symbol",    StringType(),  True),
    StructField("open",      FloatType(),   True),
    StructField("high",      FloatType(),   True),
    StructField("low",       FloatType(),   True),
    StructField("close",     FloatType(),   True),
    StructField("volume",    FloatType(),   True),
    StructField("closed",    BooleanType(), True),
    StructField("timestamp", LongType(),    True),
])

coingecko_schema = StructType([
    StructField("symbol",     StringType(),  True),
    StructField("coin",       StringType(),  True),
    StructField("price_usd",  DoubleType(),  True),
    StructField("market_cap", DoubleType(),  True),
    StructField("volume_24h", DoubleType(),  True),
    StructField("change_24h", DoubleType(),  True),
    StructField("timestamp",  LongType(),    True),
])

feargreed_schema = StructType([
    StructField("value",          LongType(),   True),
    StructField("classification", StringType(), True),
    StructField("timestamp",      LongType(),   True),
    StructField("timestamp_ms",   LongType(),   True),
])

# ── SPARK SESSION ──────────────────────────────────────

# ── LEER FEAR & GREED ──────────────────────────────────
feargreed_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "feargreed-index") \
    .option("startingOffsets", "latest") \
    .load()

feargreed = feargreed_raw \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), feargreed_schema).alias("d")) \
    .select("d.*") \
    .withColumn("event_time", current_timestamp()) \
    .withColumn("fecha", to_date(current_timestamp()))

# ── HDFS: Fear & Greed ─────────────────────────────────
feargreed_hdfs = feargreed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/crypto/feargreed") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/feargreed") \
    .partitionBy("fecha") \
    .trigger(processingTime="1 minute") \
    .start()

# Mostrar en consola
feargreed_query = feargreed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="60 seconds") \
    .start()

spark = SparkSession.builder \
    .appName("CryptoStreamingJob") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

KAFKA_BROKER = "kafka:29092"

# ── LEER TICKERS BINANCE ───────────────────────────────
ticker_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "btcusdt-ticker,ethusdt-ticker") \
    .option("startingOffsets", "latest") \
    .load()

tickers = ticker_raw \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), ticker_schema).alias("d")) \
    .select("d.*") \
    .withColumn("event_time", current_timestamp()) \
    .withColumn("fecha", to_date(current_timestamp())) \
    .withColumn("par", col("symbol"))

# ── LEER KLINES ────────────────────────────────────────
kline_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "crypto-klines") \
    .option("startingOffsets", "latest") \
    .load()

klines = kline_raw \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), kline_schema).alias("d")) \
    .select("d.*") \
    .withColumn("event_time", current_timestamp()) \
    .withColumn("fecha", to_date(current_timestamp())) \
    .withColumn("par", col("symbol"))

# ── LEER COINGECKO ─────────────────────────────────────
coingecko_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "coingecko-market") \
    .option("startingOffsets", "latest") \
    .load()

coingecko = coingecko_raw \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), coingecko_schema).alias("d")) \
    .select("d.*") \
    .withColumn("event_time", current_timestamp()) \
    .withColumn("fecha", to_date(current_timestamp())) \
    .withColumn("par", col("symbol"))

# ── INDICADOR 1: MEDIA MÓVIL + VOLATILIDAD ─────────────
sma_metrics = tickers \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute"),
        col("symbol")
    ) \
    .agg(
        spark_round(avg("price"), 2).alias("sma_5min"),
        spark_round(max("price"), 2).alias("price_max"),
        spark_round(min("price"), 2).alias("price_min"),
        spark_round(stddev("price"), 4).alias("volatility"),
        spark_round(avg("change_pct"), 4).alias("change_avg"),
    )

# ── INDICADOR 2: DETECCIÓN PUMP & DUMP ─────────────────
pump_dump = tickers \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window(col("event_time"), "2 minutes", "30 seconds"),
        col("symbol")
    ) \
    .agg(
        spark_round(avg("change_pct"), 4).alias("change_avg"),
        spark_round(max("price"), 2).alias("price_max"),
        spark_round(min("price"), 2).alias("price_min"),
    ) \
    .withColumn(
        "alerta",
        when(col("change_avg") > 2.0,  "🚀 PUMP")
        .when(col("change_avg") < -2.0, "💥 DUMP")
        .otherwise("normal")
    )

# ── INDICADOR 3: DESVIACIÓN BINANCE vs COINGECKO ───────
# Join entre precio Binance (tick a tick) y precio global CoinGecko
# Ventana de 1 min para alinear ambos flujos
binance_windowed = tickers \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("symbol")
    ) \
    .agg(spark_round(avg("price"), 2).alias("binance_price"))

coingecko_windowed = coingecko \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("symbol")
    ) \
    .agg(
        spark_round(avg("price_usd"), 2).alias("coingecko_price"),
        spark_round(avg("market_cap"), 0).alias("market_cap"),
    )

price_deviation = binance_windowed.join(
    coingecko_windowed,
    on=["window", "symbol"],
    how="inner"
) .withColumn(
    "deviation_pct",
    spark_round(
        (col("binance_price") - col("coingecko_price"))
        / col("coingecko_price") * 100, 4
    )
)

# ── OUTPUTS CONSOLA ────────────────────────────────────
sma_query = sma_metrics.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="30 seconds") \
    .start()

pump_query = pump_dump.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="30 seconds") \
    .start()

deviation_query = price_deviation.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="60 seconds") \
    .start()

# ── HDFS: tickers particionado por par y fecha ─────────
ticker_hdfs = tickers.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/crypto/tickers") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/tickers") \
    .partitionBy("par", "fecha") \
    .trigger(processingTime="1 minute") \
    .start()

# ── HDFS: klines particionado por par y fecha ──────────
kline_hdfs = klines \
    .filter(col("closed") == True) \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/crypto/klines") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/klines") \
    .partitionBy("par", "fecha") \
    .trigger(processingTime="1 minute") \
    .start()

# ── HDFS: coingecko particionado por par y fecha ───────
coingecko_hdfs = coingecko.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/crypto/coingecko") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/coingecko") \
    .partitionBy("par", "fecha") \
    .trigger(processingTime="1 minute") \
    .start()

spark.streams.awaitAnyTermination()
