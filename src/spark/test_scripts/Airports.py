from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder.appName("AirportsData").getOrCreate()

# Define the file path (local path)
file_path = "hdfs://localhost:9000/user/project/nifi_out/airports/*.csv"

# Read the CSV file
airports_df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)

# Show the DataFrame
airports_df.show()

# Save the processed data to HDFS
output_path = "hdfs://localhost:9000/user/project/nifi_out/processed_airports/"
airports_df.write.mode("overwrite").parquet(output_path)

# Verify the data
print(f"Airports data saved to HDFS at {output_path}")

# Analyze the data (group by city)
# Count the number of airports in each city
airports_by_city = airports_df.groupBy("CITY").count().orderBy(col("count").desc())

# Convert to Pandas for visualization
airports_by_city_pd = airports_by_city.toPandas()

# Plot the top 10 cities with the most airports
plt.figure(figsize=(10, 6))
plt.barh(airports_by_city_pd["CITY"][:10], airports_by_city_pd["count"][:10], color="lightblue")
plt.xlabel("Number of Airports")
plt.ylabel("City")
plt.title("Top 10 Cities with the Most Airports")
plt.gca().invert_yaxis()
plt.grid(axis="x", linestyle="--", alpha=0.7)
plt.tight_layout()
plt.show()
