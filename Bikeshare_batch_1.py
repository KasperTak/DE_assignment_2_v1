# ------------------------------------------------------------------------------Configuration----------------------------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark import SparkConf

sparkConf = SparkConf()
# sparkConf.setMaster("spark://spark-master:7077")
sparkConf.setAppName("Bikeshare_Batch_1")
sparkConf.set("spark.driver.memory", "2g")
sparkConf.set("spark.executor.cores", "1")
sparkConf.set("spark.driver.cores", "1")

# create the Spark session, which is the entry point to the Spark SQL engine.
spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()
# load data
# Setup Hadoop FS configuration for schema gs://
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

#  Google Storage File Path
BUCKET = "gs://deassignment2_data_bucket"
# ---------------------------------------------------- load data from GCS------------------------------------------------------------------------------------------------------------------------------
# Weather dataframe (weather.csv)
weather_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
       .load(f"{BUCKET}/weather.csv")
weather_df.printSchema()

# Frequency dataframe (usage_frequency.csv)
frequency_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
       .load(f"{BUCKET}/usage_frequency.csv")
frequency_df = frequency_df.withColumn("date", to_date(col("date"))) \
                          .withColumn("pickup_counts", col("pickup_counts").cast("int"))
frequency_df.printSchema()
frequency_df.show(5)


# # # ---------------------------------------------------------------------Data cleaning / featuring---------------------------------------------------------------------------------------------------
from pyspark.sql.functions import *
from pyspark.sql import Row, Window


# weather data columns drop (irrelevant columns)
cols_to_drop = ["feelslikemax", "feelslikemin", "feelslike",
                "precipprob", "precipcover", "preciptype", 
                "sealevelpressure", "cloudcover", "solarradiation",
                "solarenergy", "severerisk", "description", "stations"]

weather_df_clean = weather_df.drop(*cols_to_drop)

# datetime formats checking & adding temperature category (cold, mild, hot) & adding rain category (rain, snow, no rain)
weather_df_clean = weather_df_clean.withColumn("weather_date", to_date(col("datetime"))) \
                                    .withColumn("temp", col("temp").cast("double")) \
                                    .withColumn("temp_category", 
                                                when(col("temp") < 10, "cold")
                                                .when(col("temp") < 20, "mild")
                                                .otherwise("hot")) \
                                    .withColumn("rain_category", 
                                                when(col("icon") == "rain", "rain")
                                                .when(col("icon") == "snow", "snow")
                                                .otherwise("no rain")) \
                                    .drop("datetime", "name")

# Join op datuum
joinExpression = weather_df_clean['weather_date'] == frequency_df['date'] # keys per column for join-function

frequency_merged = frequency_df.join(weather_df_clean, joinExpression, "inner").drop("weather_date")
frequency_merged.printSchema()
frequency_merged.show(10)

# # #-----------------------------------------------Question 1: Geef per temperatuurscategory de top-3 stations met gemiddeld de meeste pickups-----------------------------------------------------------------
# Average pickups per station per temperature category
# # minimum of 30 days data, otherwise a station with 1 day data wins the "most calm" (rustige) category.
station_avg = frequency_merged.groupBy("temp_category", "station_name").agg(
                                avg("pickup_counts").alias("avg_pickups"),
                                count("date").alias("n_days")
                                ).where(col("n_days") >= 30)

# Rank the stations in terms of average pickups, per temperature category
windowdesc = Window.partitionBy(col("temp_category")).orderBy(col("avg_pickups").desc())
windowasc = Window.partitionBy(col("temp_category")).orderBy(col("avg_pickups").asc())

station_windowed = station_avg.withColumn("rank_desc", dense_rank().over(windowdesc)) \
                                .withColumn("rank_asc", dense_rank().over(windowasc))
station_windowed.show(20)

# Busiest and quietest station per category
top_bottom = station_windowed.where((col("rank_desc") <= 3) | (col("rank_asc") <= 3)) \
                  .withColumn("usage_category", when(col("rank_desc") <= 3, "Busiest").otherwise("Quietest")) \
                  .withColumn("rank", when(col("rank_desc") <= 3, col("rank_desc")).otherwise(col("rank_asc"))) \
                  .select("temp_category", "usage_category", "rank", "station_name", "avg_pickups", "n_days")

top_bottom.orderBy("temp_category", "usage_category", "rank").show(30)



# # # ----------------------------------------------Question 2: Which station has the most pickups on rainy days and days without rain?----------------------------------------------------------------------------
station_avg_rain = frequency_merged.groupBy("rain_category", "station_name").agg(
                                    avg("pickup_counts").alias("avg_pickups"),
                                    count("date").alias("n_days")
                                    ).where(col("n_days") >= 30)

# Rank the staions in terms of average pickups, per rain category
window_rain = Window.partitionBy(col("rain_category")).orderBy(col("avg_pickups").desc())

# select the busiest station per rain category
busiest_rain = station_avg_rain.withColumn("rank", dense_rank().over(window_rain)).where(col("rank") == 1)
busiest_rain.show(20)

# -------------------------------------------------------------GCS bucket for temp. BigQuery---------------------------------------------------------------------------------------------------------------------------------------------
# Cloud Storage bucket for temporary BigQuery export data that the connector uses.
temp_bucket = "deassignment2_data_bucket_temp"
spark.conf.set('temporaryGcsBucket', temp_bucket)
# Saving the data (busiest_quietest (Q1)) to BigQuery
top_bottom.write.format('bigquery') \
                .option('table', 'deassignment2-515613.assignment2.stations_selection').mode("overwrite").save()

# Saving the data (busiest_rain (Q2)) to BigQuery
busiest_rain.write.format('bigquery') \
                .option('table', 'deassignment2-515613.assignment2.stations_rain_selection').mode("overwrite").save()

# Stop the spark context
spark.stop()


