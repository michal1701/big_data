from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count, from_unixtime

# Initialize Spark session
spark = SparkSession.builder.appName("EarthquakeData").getOrCreate()

# Define the file path (HDFS path)
file_path = "hdfs://localhost:9000/user/project/nifi_out/earthquake/*.csv"

# Read the CSV files
df = spark.read.option("delimiter", "|").csv(file_path, header=False, inferSchema=True)

# Assign column names
df = df.toDF("magnitude", "time", "latitude", "longitude", "type")

# Convert 'time' from milliseconds to seconds and then to TIMESTAMP
df = df.withColumn("timestamp", from_unixtime(col("time") / 1000).cast("timestamp"))

# 1. Extract hour from `timestamp` and count earthquakes for each hour
hourly_earthquakes = df.withColumn("hour", hour(col("timestamp"))) \
                       .groupBy("hour").agg(count("*").alias("earthquake_count")) \
                       .orderBy("hour")

# Convert to Pandas for visualization
hourly_earthquakes_pd = hourly_earthquakes.toPandas()

# Plot earthquakes by hour
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))
plt.bar(hourly_earthquakes_pd["hour"], hourly_earthquakes_pd["earthquake_count"], alpha=0.7)
plt.xlabel("Hour of the Day")
plt.ylabel("Number of Earthquakes")
plt.title("Number of Earthquakes by Hour of Day")
plt.xticks(range(0, 24))
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.tight_layout()
plt.show()

# 2. Count the most occurring earthquake types
top_types = df.groupBy("type").agg(count("*").alias("earthquake_count")) \
             .orderBy(col("earthquake_count").desc()).limit(10)

# Convert to Pandas for visualization
top_types_pd = top_types.toPandas()

# Plot top earthquake types
plt.figure(figsize=(10, 6))
plt.barh(top_types_pd["type"], top_types_pd["earthquake_count"], color="skyblue")
plt.xlabel("Number of Earthquakes")
plt.ylabel("Earthquake Type")
plt.title("Top 10 Most Frequent Earthquake Types")
plt.gca().invert_yaxis()  # Invert y-axis to show the highest count at the top
plt.grid(axis="x", linestyle="--", alpha=0.7)
plt.tight_layout()
plt.show()
