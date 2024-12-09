from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max

# Initialize SparkSession
spark = SparkSession.builder.appName("VehicleCasualtyAnalysis").getOrCreate()

# Load data
vehicles = spark.read.csv("data/Vehicles.csv", header=True, inferSchema=True)
casualties = spark.read.csv("data/Casualties.csv", header=True, inferSchema=True)

# Display schemas and record counts
print("Schema of Vehicles dataset:")
vehicles.printSchema()
print(f"Number of records in Vehicles: {vehicles.count()}")

print("Schema of Casualties dataset:")
casualties.printSchema()
print(f"Number of records in Casualties: {casualties.count()}")

# Calculate average driver age by vehicle type
avg_driver_age = vehicles.groupBy("Vehicle_Type").agg(avg("Driver_Age").alias("Avg_Driver_Age"))
avg_driver_age.show()

# Count casualties by vehicle type
casualties_by_vehicle = vehicles.join(casualties, "Accident_Index").groupBy("Vehicle_Type") \
    .sum("Number_of_Casualties").alias("Total_Casualties")
casualties_by_vehicle.createOrReplaceTempView("CasualtiesByVehicle")
spark.sql("SELECT * FROM CasualtiesByVehicle").show()

# Maximum speed limit for each casualty type
max_speed_limit = casualties.groupBy("Casualty_Type").agg(max("Speed_Limit").alias("Max_Speed_Limit"))
max_speed_limit.write.json("hdfs://path/to/output/MaxSpeedLimit_JSON", mode="overwrite")
max_speed_limit.write.csv("hdfs://path/to/output/MaxSpeedLimit_CSV", header=True, mode="overwrite")

# Stop Spark session
spark.stop()