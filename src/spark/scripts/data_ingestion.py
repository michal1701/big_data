from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def ingest_all_world_airport_data(spark, input_path):
    """
    Load additional airport data (without header) and format it to match the first airport dataset.
    """
    # Read the CSV without a header
    additional_df = spark.read.option("header", False).option("inferSchema", True).csv(input_path)

    # Rename columns based on the structure of the second dataset
    additional_df = additional_df.toDF(
        "Airport_ID", "Airport", "City", "Country", "IATA", "ICAO", 
        "Latitude", "Longitude", "Altitude", "Timezone", "DST", 
        "Tz_database_timezone", "Type", "Source"
    )

    # Select and reorder only the 7 required columns
    formatted_df = additional_df.select(
        col("IATA").alias("IATA"),             # IATA code
        col("Airport").alias("AIRPORT"),       # Airport name
        col("City").alias("CITY"),             # City name
        lit(None).alias("STATE"),              # Add a NULL column for STATE
        col("Country").alias("COUNTRY"),       # Country name
        col("Latitude").alias("LATITUDE"),     # Latitude
        col("Longitude").alias("LONGITUDE")    # Longitude
    )

    return formatted_df

def merge_airport_datasets(first_df, second_df):
    """
    Align the columns of the two airport datasets and merge them into one DataFrame.
    """
    # Ensure both DataFrames have the same schema
    second_df_aligned = second_df.selectExpr(
        "IATA",               # IATA code
        "AIRPORT",            # Airport name
        "CITY",               # City name
        "STATE",              # State (already NULL in second dataset)
        "COUNTRY",            # Country name
        "LATITUDE",           # Latitude
        "LONGITUDE"           # Longitude
    )

    # Merge the two DataFrames
    merged_df = first_df.union(second_df_aligned)
    
    return merged_df

def ingest_earthquake_data(spark, input_path):
    """
    Load earthquake data from HDFS and return a Spark DataFrame.
    """
    df = spark.read.option("delimiter", "|").csv(input_path, header=False, inferSchema=True)
    df = df.toDF("magnitude", "time", "latitude", "longitude", "type")
    return df

def ingest_flight_data(spark, input_path):
    """
    Load flight data from HDFS and return a Spark DataFrame.
    """
    df = spark.read.option("delimiter", "|").csv(input_path, header=False, inferSchema=True)
    df = df.toDF(
        "ignore_col", "arrival_delay", "arrival_iata", "arrival_scheduled",
        "departure_actual", "departure_delay", "departure_iata", "departure_scheduled", 
        "flight_status"
    ).drop("ignore_col")
    return df

def ingest_airport_data(spark, input_path):
    """
    Load airport data from HDFS and return a Spark DataFrame.
    """
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    return df

def save_to_hdfs(df, output_path):
    """
    Save a Spark DataFrame to HDFS in Parquet format.
    """
    df.write.mode("overwrite").parquet(output_path)
    print(f"Data saved to HDFS: {output_path}")

def ingest_data():
    """
    Main function for loading all datasets and saving processed data to HDFS.
    """
    # Initialize Spark session
    spark = SparkSession.builder.appName("DataIngestion").getOrCreate()

    # File paths
    earthquake_path = "hdfs://localhost:9000/user/project/nifi_out/earthquake/*.csv"
    flights_path = "hdfs://localhost:9000/user/project/nifi_out/flights/*.csv"
    airports_path = "hdfs://localhost:9000/user/project/nifi_out/airports/airports.csv"
    all_world_airports_path = "hdfs://localhost:9000/user/project/nifi_out/airports/airports_all.csv"
    
    all_world_airports_df = ingest_all_world_airport_data(spark, all_world_airports_path)
    print("All World Airports Data:", all_world_airports_df.columns)
    # Load datasets
    print("Loading earthquake data...")
    earthquake_df = ingest_earthquake_data(spark, earthquake_path)
    #save_to_hdfs(earthquake_df, "hdfs://localhost:9000/user/project/nifi_out/processed_earthquake/")

    print("Loading flight data...")
    flights_df = ingest_flight_data(spark, flights_path)
    #save_to_hdfs(flights_df, "hdfs://localhost:9000/user/project/nifi_out/processed_flights/")

    print("Loading airport data...")
    airports_df = ingest_airport_data(spark, airports_path)
    print("Airports Data:", airports_df.columns)
    #save_to_hdfs(airports_df, "hdfs://localhost:9000/user/project/nifi_out/processed_airports/")
    
    # Merge the two datasets
    merged_airport_df = merge_airport_datasets(airports_df, all_world_airports_df)
    merged_airport_df.show(5, truncate=False)

    print("Data ingestion complete.")

    print("Earthquake Data:")
    earthquake_df.show(5, truncate=False)

    print("Flight Data:")
    flights_df.show(5, truncate=False)

    print("Airport Data:")
    airports_df.show(5, truncate=False)

    return earthquake_df, flights_df, airports_df

if __name__ == "__main__":
    ingest_data()
