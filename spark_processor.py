from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from configs import kafka_config

# Створення Spark сесії з Kafka
spark = SparkSession.builder \
    .appName("IoT_Alert_Analysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# Зчитування CSV conditions
alert_conditions = spark.read.csv("alerts_conditions.csv", header=True, inferSchema=True)

# Зчитування потоку з Kafka
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True)
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0]) \
    .option("subscribe", "goit_building_sensors") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .load()

# Десеріалізація
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", from_unixtime(col("timestamp")).cast("timestamp"))

# Агрегація: Sliding Window (1 хв довжина, 30 сек крок)
windowed_avg = parsed_df \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(window(col("event_time"), "1 minute", "30 seconds")) \
    .agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_hum")
    )

# Робимо cross join, щоб перевірити середні значення вікна з правилами CSV
alerts_df = windowed_avg.crossJoin(alert_conditions) \
    .filter(
        # Умова для температури (якщо max_temp не -999 і середня > max_temp)
        ((col("avg_temp") > col("temperature_max")) & (col("temperature_max") != -999)) |
        # Або якщо min_temp не -999 і середня < min_temp
        ((col("avg_temp") < col("temperature_min")) & (col("temperature_min") != -999)) |
        # Умова для вологості (якщо max_hum не -999 і середня > max_hum)
        ((col("avg_hum") > col("humidity_max")) & (col("humidity_max") != -999)) |
        # Або якщо min_hum не -999 і середня < min_hum
        ((col("avg_hum") < col("humidity_min")) & (col("humidity_min") != -999))
    )

# Формуємо фінальне повідомлення для Kafka
final_alerts = alerts_df.select(
    to_json(struct(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temp"),
        col("avg_hum"),
        col("message"),
        col("code")
    )).alias("value")
)

# Старт стримінгу
query = final_alerts.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0]) \
    .option("topic", "goit_alerts_output") \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .start()

query.awaitTermination()