from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder.appName("FlightsData").getOrCreate()

# Define the file path (HDFS path)
file_path = "hdfs://localhost:9000/user/project/nifi_out/flights/*.csv"

# Read the CSV files
df = spark.read.option("delimiter", "|").csv(file_path, header=False, inferSchema=True)

# Assign column names
df = df.toDF(
    "ignore_col", "arrival_delay", "arrival_iata", "arrival_scheduled",
    "departure_actual", "departure_delay", "departure_iata", "departure_scheduled", 
    "flight_status"
).drop("ignore_col")

# Show the DataFrame
df.show()



# save the processed data back to HDFS in a structured format (e.g., Parquet):
# output_path = "hdfs://localhost:9000/user/project/nifi_out/processed_flights/"
# df.write.mode("overwrite").parquet(output_path)



# 1. Extract hour of departure and count flights for each hour
hourly_flights = df.withColumn("departure_hour", hour(col("departure_scheduled"))) \
                   .groupBy("departure_hour").agg(count("*").alias("flight_count")) \
                   .orderBy("departure_hour")

# Convert to Pandas for visualization
hourly_flights_pd = hourly_flights.toPandas()

# Plot flights by hour
plt.figure(figsize=(10, 6))
plt.bar(hourly_flights_pd["departure_hour"], hourly_flights_pd["flight_count"], alpha=0.7)
plt.xlabel("Hour of the Day")
plt.ylabel("Number of Flights")
plt.title("Number of Flights by Hour of Day")
plt.xticks(range(0, 24))
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.tight_layout()
plt.show()


# 2. Count most occurring departure airports
top_airports = df.groupBy("departure_iata").agg(count("*").alias("flight_count")) \
                 .orderBy(col("flight_count").desc()).limit(10)

# Convert to Pandas for visualization
top_airports_pd = top_airports.toPandas()

# Plot top airports
plt.figure(figsize=(10, 6))
plt.barh(top_airports_pd["departure_iata"], top_airports_pd["flight_count"], color="skyblue")
plt.xlabel("Number of Flights")
plt.ylabel("Departure Airport (IATA)")
plt.title("Top 10 Most Frequent Departure Airports")
plt.gca().invert_yaxis()  # Invert y-axis to show highest count at the top
plt.grid(axis="x", linestyle="--", alpha=0.7)
plt.tight_layout()
plt.show()