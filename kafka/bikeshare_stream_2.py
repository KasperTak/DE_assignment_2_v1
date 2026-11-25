# Pipeline 2: stream processing
# # # Q: net flow per station, per hour, per consumer-type (member/casual)

import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, unix_timestamp, lit, window, sum as sum_, broadcast
from pyspark.sql.types import StructType, StructField, StringType

KAFKA_BOOTSTRAP = 'EXTERN_IP_OF_KAFKA-VM' # bijv. "34.09.54.12:9092"
KAFKA_TOPIC = 'bikeshare_rides'
BUCKET = 'gs://deassignment2_data_bucket'
TEMP_BUCKET = 'deassignment2_data_bucket_temp'
CHECKPOINT = f"{BUCKET}/checkpoints/stream_netflow"
BQ_TABLE = 'deassignment2-515613.assignment2.stations_hourly_flow'

MAX_DURATION_MIN = 180 # trips longer than 3 hours are probably not returned.
WATERMARK = "3 hours"

sparkConf = SparkConf()
sparkConf.setAppName("Bikeshare_Stream_2")
sparkConf.set("spark.driver.memory", "2g")
spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

# -------------------------------------------------Read stream form Kafka--------------------------------------------------------------------------------------------------------------------------------

dataSchema = StructType(
        [StructField("ride_id", StringType(), True),
         StructField("rideable_type", StringType(), True),
         StructField("started_at", StringType(), True),
         StructField("ended_at", StringType(), True),
         StructField("start_station_name", StringType(), True),
         StructField("start_station_id", StringType(), True),
         StructField("end_station_name", StringType(), True),
         StructField("end_station_id", StringType(), True),
         StructField("member_casual", StringType(), True)
         ])

kafkaStream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).option("subscribe", KAFKA_TOPIC) \
                                .option("startingOffsets", "earliest").load()

rides = kafkaStream.selectExpr("CAST(value AS STRING) AS value").select(from_json(col("value"), dataSchema).alias("r")).select("r.*")


#---------------------------------------------------Cleaning / featuring--------------------------------------------------------------------------------------------------------------------------------

rides = rides.withColumn("started_ts", to_timestamp(col("started_at"))) \
                .withColumn("ended_ts", to_timestamp(col("ended_at"))) \
                .withColumn("duration_min", (unix_timestamp("ended_ts")-unix_timestamp("started_ts"))/60)

valid = rides.where(col("ride_id").isNotNull()
                    & col("started_ts").isNotNull()
                    & col("ended_ts").isNotNull()
                    & (col("duration_min") >= 1)
                    & (col("duration_min") <= MAX_DURATION_MIN)
                    & col("member_casual").isin("member", "casual")
                    )

# each trip becomes two events: depart from started_ts and arrival on ended_ts
departures = valid.where(col("start_station_id").isNotNull()).select(
    col("started_ts").alias("event_time"),
    col("start_station_id").alias("station_id"),
    "member_casual",
    lit(1).alias("departures"),
    lit(0).alias("arrivals")
)

arrivals = valid.where(col("end_station_id").isNotNull()).select(
    col("ended_ts").alias("event_time"),
    col("end_station_id").alias("station_id"),
    "member_casual",
    lit(0).alias("departures"),
    lit(1).alias("arrivals")
)

events = departures.unionByName(arrivals)

# Stream static join: station_list.csv gives an uniform station name per station_id
stations = spark.read.format("csv").option("header", "true").load(f"{BUCKET}/station_list.csv") \
                .select(col("station_id").cast("string").alias("station_id"), "station_name") \
                .dropDuplicates(["station_id"])

enriched = events.join(broadcast(stations), "station_id", "inner")

#-----------------------------------------------------------window aggregation----------------------------------------------------------------------------------------------------------------------------
# departure-events are till max_duration_min earlier in event_time than the message (trip only get send by arrival) -> watermark = 3 hours
flow = enriched.withWatermark("event_time", WATERMARK) \
                .groupBy(window(col("event_time"), "1 hour"), "station_id", "station_name", "member_casual") \
                .agg(sum_("departures").alias("departures"), sum_("arrivals").alias("arrivals")) \
                .select(col("window.start").alias("window_start"), col("window.end").alias("window_end"),
                        "station_id", "station_name", "member_casual", "departures", "arrivals",
                        (col("arrivals") - col("departures")).alias("net_flow"))

#-----------------------------------------------------------Write to BigQuery----------------------------------------------------------------------------------------------------------------------------

def write_to_bigquery(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    batch_df.write.format("bigquery") \
                .option("table", BQ_TABLE) \
                .option("temporaryGcsBucket", TEMP_BUCKET) \
                .mode("append").save()

# append mode: een venster wordt pas weggeschreven als de watermark het sluit, dus elk venster komt precies 1 keer in bigquery
writer = flow.writeStream.queryName("stations_hourly_flow") \
                            .outputMode("append") \
                            .foreachBatch(write_to_bigquery) \
                            .option("checkpointLocation", CHECKPOINT)

if "--once" in sys.argv:
    writer = writer.trigger(availableNow=True)

else:
    writer = writer.trigger(processingTime="30 seconds")

query = writer.start()
try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
    spark.stop()
    print("Stopped the streaming query and the spark context.")


