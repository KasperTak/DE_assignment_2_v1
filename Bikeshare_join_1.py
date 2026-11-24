# %%
from pyspark.sql import SparkSession
from pyspark import SparkConf

sparkConf = SparkConf()
sparkConf.setMaster("spark://spark-master:7077")
sparkConf.setAppName("Bikeshare_Join_1")
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
gsc_file_path = 'gs://deassignment2_data_bucket/weather.csv'  #  use your gcp bucket name. Also, upload sales.csv first
# Create data frame
weather_df = spark.read.format("csv").option("header", "true").option("delimiter", ",") \
       .load(gsc_file_path)
weather_df.printSchema()
weather_df.show(5)

frequency_df = spark.read \
  .format("bigquery") \
  .load("deassignment2-515613.assignment2.frequency")    # project_id.datatset.tablename. Use your project id
frequency_df.printSchema()
frequency_df.show(4)

# %% [markdown]
# Question 1: Give per temperature category (cold, mid, hot) the top-3 stations with on average the most pick ups per day.

# %%
from pyspark.sql.functions import *
from pyspark.sql import Row, Window


# weather data columns drop (irrelevant columns)
cols_to_drop = ["feelslikemax", "feelslikemin", "feelslike",
                "precipprob", "precipcover", "preciptype", 
                "sealevelpressure", "cloudcover", "solarradiation",
                "solarenergy", "severerisk", "description", "stations"]

weather_df_clean = weather_df.drop(*cols_to_drop)

# datetime formats checking & adding temperature category (cold, mild, hot)
weather_df_clean = weather_df_clean.withColumn("weather_date", to_date(col("datetime"))) \
                                    .withColumn("temp_category", when(col("temp") < 5, "cold")
                                                .when(col("temp") < 18, "mild")
                                                .otherwise("hot")) \
                                    .drop("datetime", "name")

# Join op datuum
joinExpression = weather_df_clean['weather_date'] == frequency_df['date'] # keys per column for join-function

frequency_merged = frequency_df.join(weather_df_clean, joinExpression, "inner").drop("weather_date")
frequency_merged.printSchema()
frequency_merged.show(10)

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
busiest_quietest = station_windowed.where((col("rank_desc") == 1) | (col("rank_asc") == 1))

busiest_quietest2 = busiest_quietest.withColumn("usage_category", when(col("rank_desc") == 1, "Busiest").otherwise("Quietest")).select("temp_category", "usage_category", "station_name", "avg_pickups", "n_days")
busiest_quietest2.orderBy("temp_category", "usage_category").show(20)


# %%
# Cloud Storage bucket for temporary BigQuery export data that the connector uses.
bucket = "deassignment2_data_bucket_temp"
spark.conf.set('temporaryGcsBucket', bucket)
# Saving the data to BigQuery
busiest_quietest2.write.format('bigquery') \
                .option('table', 'deassignment2-515613.assignment2.stations_selection').mode("overwrite").save()

# %%
# Stop the spark context
spark.stop()


