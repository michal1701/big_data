from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from data_ingestion import ingest_all_world_airport_data, merge_airport_datasets, ingest_earthquake_data, ingest_flight_data, ingest_airport_data, save_to_hdfs 
import os
from pyspark.sql.functions import col, lit, abs, count, avg, max, when, regexp_replace
import pandas as pd
from pyspark.sql.functions import lit
from pyspark.sql.functions import monotonically_increasing_id


import happybase

def check_hbase_table_structure(table_name):
    """
    Check the structure of an HBase table and its column families.

    Args:
    - table_name: Name of the HBase table to inspect.
    """
    # Connect to HBase Thrift server
    connection = happybase.Connection(host='localhost', port=9090)
    
    if table_name.encode() in connection.tables():
        print(f"Table '{table_name}' exists.")
        table = connection.table(table_name)
        
        # Fetch column families
        families = table.families()
        print(f"Column Families in '{table_name}':")
        for family, attributes in families.items():
            print(f"  - Family: {family.decode()} | Attributes: {attributes}")
    else:
        print(f"Table '{table_name}' does not exist.")
    
    connection.close()


def save_to_hbase_with_happybase(pandas_df, table_name):
    """
    Save a Pandas DataFrame to HBase using HappyBase.
    
    Args:
    - pandas_df: Pandas DataFrame to save.
    - table_name: Name of the HBase table.
    """
    # Connect to HBase Thrift server
    connection = happybase.Connection(host='localhost', port=9090)
    
    # Create the table if it doesn't exist
    if table_name.encode() not in connection.tables():
        connection.create_table(
            table_name,
            {
                'info': dict()  # Column family
            }
        )

    table = connection.table(table_name)

    # Write each row to the HBase table
    for _, row in pandas_df.iterrows():
        # Use a unique key, e.g., "departure_iata+arrival_iata"
        row_key = f"{row['departure_iata']}_{row['arrival_iata']}"
        # Prepare data in the column family format
        data = {
            f"info:{col}": str(row[col]) if row[col] is not None else ""  # Convert all values to strings
            for col in pandas_df.columns
        }
        # Save the row
        table.put(row_key, data)

    # Close the connection
    connection.close()

def enrich_flights_with_earthquakes(enriched_flights_df, earthquake_df, spatial_radius_km=25):
    """
    Enrich flights data with earthquake information:
    - Join flights with earthquakes within a spatial radius.
    - Add columns indicating earthquake occurrence, type, magnitude, and location.
    
    Args:
    - enriched_flights_df: DataFrame containing enriched flight data with airport details.
    - earthquake_df: DataFrame containing earthquake data.
    - spatial_radius_km: Radius in kilometers to consider proximity.

    Returns:
    - DataFrame with flights enriched with earthquake occurrence details.
    """
    # Convert spatial radius from kilometers to degrees (~111.32 km per degree latitude)
    proximity_threshold = spatial_radius_km / 111.32

    # Join flights and earthquakes based on proximity
    flights_with_earthquakes = enriched_flights_df.join(
        earthquake_df,
        (
            abs(enriched_flights_df["arrival_latitude"] - earthquake_df["latitude"]) < proximity_threshold
        ) & (
            abs(enriched_flights_df["arrival_longitude"] - earthquake_df["longitude"]) < proximity_threshold
        ),
        how="left"  # Keep all flights
    )

    # Add column to indicate if an earthquake occurred
    flights_with_earthquakes = flights_with_earthquakes.withColumn(
        "earthquake_occurred",
        when(col("magnitude").isNotNull(), True).otherwise(False)
    )

    # Select necessary columns and include earthquake details
    enriched_flights = flights_with_earthquakes.select(
        enriched_flights_df["*"],  # Include all columns from flights data
        col("earthquake_occurred"),
        col("type").alias("earthquake_type"),
        col("magnitude").alias("earthquake_magnitude"),
        col("latitude").alias("earthquake_latitude"),
        col("longitude").alias("earthquake_longitude")
    )

    return enriched_flights



def calculate_distance(lat1, lon1, lat2, lon2):
    """
    Haversine formula to calculate the great-circle distance between two points
    on a sphere given their latitude and longitude.
    """
    from math import radians, sin, cos, sqrt, atan2

    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    R = 6371  # Radius of Earth in kilometers
    return R * c

def transform_data(enriched_flights_df, earthquake_df, spatial_radius_km=50):
    """
    Perform transformations in Spark:
    - Identify airports impacted by earthquakes within a spatial radius.
    - Count earthquakes and flights impacted for each airport.

    Args:
    - enriched_flights_df: DataFrame containing enriched flight data with airport details.
    - earthquake_df: DataFrame containing earthquake data.
    - spatial_radius_km: Radius in kilometers to consider proximity.

    Returns:
    - DataFrame summarizing earthquake and flight impacts by airport.
    """
    # Convert spatial radius from kilometers to degrees (~111.32 km per degree latitude)
    proximity_threshold = spatial_radius_km / 111.32

    # Identify impacted airports by comparing proximity to earthquakes
    impacted_airports = enriched_flights_df.join(
        earthquake_df,
        (
            abs(enriched_flights_df["arrival_latitude"] - earthquake_df["latitude"]) < proximity_threshold
        ) & (
            abs(enriched_flights_df["arrival_longitude"] - earthquake_df["longitude"]) < proximity_threshold
        ),
        how="inner"
    )

    # Aggregate data: count earthquakes and find max magnitude per airport
    aggregated_airports = impacted_airports.groupBy(
        "arrival_iata", "arrival_airport", "arrival_city", "arrival_country"
    ).agg(
        count("*").alias("earthquake_count"),
        max("magnitude").alias("max_magnitude")
    )

    # Aggregate flight impact data
    flights_impact_summary = impacted_airports.groupBy(
        "arrival_iata", "arrival_airport"
    ).agg(
        count("*").alias("flights_impacted_count")
    )

    # Join earthquake and flight impact summaries
    final_summary = aggregated_airports.join(
        flights_impact_summary,
        on=["arrival_iata", "arrival_airport"],
        how="left"
    )

    return final_summary

def analyze_earthquake_impact_on_flights(enriched_flights_df, earthquake_df):
    """
    Analyze the potential impact of earthquakes on flights based on geographic proximity and time.
    
    Args:
    - enriched_flights_df: Enriched flights DataFrame with airport details.
    - earthquake_df: Earthquake DataFrame.
    
    Returns:
    - DataFrame of flights potentially impacted by earthquakes.
    """
    # Define a threshold for proximity (e.g., 500 km) and time window (e.g., 1 hour)
    proximity_threshold = 5.0  # Adjust as needed (degrees of lat/lon ~500 km)
    time_threshold = 3600 * 1000  # 1 hour in milliseconds

    # Join flights and earthquake data based on geographic proximity and time
    impacted_flights = enriched_flights_df.join(
        earthquake_df,
        (
            abs(enriched_flights_df["arrival_latitude"] - earthquake_df["latitude"]) < proximity_threshold
        ) & (
            abs(enriched_flights_df["arrival_longitude"] - earthquake_df["longitude"]) < proximity_threshold
        ) & (
            abs(col("arrival_scheduled").cast("long") - col("time").cast("long")) < time_threshold
        ),
        how="inner"
    ).select(
        col("departure_iata"),
        col("arrival_iata"),
        col("arrival_scheduled"),
        col("arrival_airport"),
        col("latitude").alias("earthquake_latitude"),
        col("longitude").alias("earthquake_longitude"),
        col("magnitude").alias("earthquake_magnitude"),
        col("type").alias("earthquake_type")
    )

    return impacted_flights


def aggregate_flight_data(enriched_flights_df):
    """
    Perform aggregations on enriched flight data.
    """
    # Most common departure airports
    top_departure_airports = enriched_flights_df.groupBy("departure_iata").agg(
        count("*").alias("total_flights")
    ).orderBy(col("total_flights").desc())

    # Most common arrival airports
    top_arrival_airports = enriched_flights_df.groupBy("arrival_iata").agg(
        count("*").alias("total_flights")
    ).orderBy(col("total_flights").desc())

    # Average delay per departure airport
    avg_departure_delay = enriched_flights_df.groupBy("departure_iata").agg(
        avg("departure_delay").alias("avg_departure_delay")
    ).orderBy(col("avg_departure_delay").desc())

    return top_departure_airports, top_arrival_airports, avg_departure_delay

def analyze_iata_coverage(flights_df, airports_df):
    """
    Analyze IATA coverage and compute percentages of matches.
    """
    # Count distinct IATA codes in Flights and Airports
    arrival_iata_count = flights_df.select("arrival_iata").distinct().count()
    departure_iata_count = flights_df.select("departure_iata").distinct().count()
    airports_iata_count = airports_df.select("IATA").distinct().count()

    print(f"Distinct Arrival IATA Codes in Flights: {arrival_iata_count}")
    print(f"Distinct Departure IATA Codes in Flights: {departure_iata_count}")
    print(f"Distinct IATA Codes in Airports: {airports_iata_count}")

    # Calculate percentage coverage for Arrival IATA
    matched_arrival_iata = flights_df.join(
        airports_df, flights_df["arrival_iata"] == airports_df["IATA"], "left"
    ).select("arrival_iata").distinct().count()
    arrival_iata_coverage = (matched_arrival_iata / arrival_iata_count) * 100 if arrival_iata_count > 0 else 0

    print(f"Matched Arrival IATA Codes: {matched_arrival_iata} ({arrival_iata_coverage:.2f}%)")

    # Calculate percentage coverage for Departure IATA
    matched_departure_iata = flights_df.join(
        airports_df, flights_df["departure_iata"] == airports_df["IATA"], "left"
    ).select("departure_iata").distinct().count()
    departure_iata_coverage = (matched_departure_iata / departure_iata_count) * 100 if departure_iata_count > 0 else 0

    print(f"Matched Departure IATA Codes: {matched_departure_iata} ({departure_iata_coverage:.2f}%)")

    return {
        "arrival_iata_count": arrival_iata_count,
        "departure_iata_count": departure_iata_count,
        "airports_iata_count": airports_iata_count,
        "matched_arrival_iata": matched_arrival_iata,
        "arrival_iata_coverage": arrival_iata_coverage,
        "matched_departure_iata": matched_departure_iata,
        "departure_iata_coverage": departure_iata_coverage
    }

def test_enrich_flights_with_earthquakes(spark):
    # Przykładowe dane lotów
    flights_data = [
        {"arrival_latitude": 40.0, "arrival_longitude": -75.0, "arrival_iata": "JFK"},
        {"arrival_latitude": 34.0, "arrival_longitude": -118.0, "arrival_iata": "LAX"},
    ]
    flights_df = spark.createDataFrame(flights_data)

    # Przykładowe dane trzęsień ziemi
    earthquake_data = [
        {"latitude": 40.1, "longitude": -75.1, "magnitude": 5.0},
        {"latitude": 35.0, "longitude": -118.0, "magnitude": 4.5},
    ]
    earthquake_df = spark.createDataFrame(earthquake_data)

    # Wywołanie testowanej funkcji
    enriched_df = enrich_flights_with_earthquakes(flights_df, earthquake_df, spatial_radius_km=15)

    # Pobranie wyników do Pythonowego list dla porównania
    results = enriched_df.select("arrival_iata", "earthquake_occurred").collect()
    expected_results = [
        {"arrival_iata": "JFK", "earthquake_occurred": True},
        {"arrival_iata": "LAX", "earthquake_occurred": False},
    ]

def enrich_flights_with_earthquakes(flights_df, earthquake_df, spatial_radius_km=25):
    proximity_threshold = spatial_radius_km / 111.32
    flights_with_earthquakes = flights_df.join(
        earthquake_df,
        (
            abs(flights_df["arrival_latitude"] - earthquake_df["latitude"]) < proximity_threshold
        ) & (
            abs(flights_df["arrival_longitude"] - earthquake_df["longitude"]) < proximity_threshold
        ),
        how="left"
    ).withColumn(
        "earthquake_occurred",
        when(col("magnitude").isNotNull(), True).otherwise(False)
    )
    return flights_with_earthquakes


def enrich_flights_with_airports(flights_df, airports_df):
    """
    Join flights with airports data to add details for both arrival and departure airports.
    
    Args:
    - flights_df: DataFrame containing flights data.
    - airports_df: DataFrame containing airports data.
    
    Returns:
    - Enriched DataFrame with additional columns for arrival and departure airport details.
    """
    # Rename columns in airports_df for joining with arrival and departure separately
    arrival_airports = airports_df.selectExpr(
        "IATA as arrival_iata",
        "AIRPORT as arrival_airport",
        "CITY as arrival_city",
        "STATE as arrival_state",
        "COUNTRY as arrival_country",
        "LATITUDE as arrival_latitude",
        "LONGITUDE as arrival_longitude"
    )
    
    departure_airports = airports_df.selectExpr(
        "IATA as departure_iata",
        "AIRPORT as departure_airport",
        "CITY as departure_city",
        "STATE as departure_state",
        "COUNTRY as departure_country",
        "LATITUDE as departure_latitude",
        "LONGITUDE as departure_longitude"
    )

    # Join flights_df with arrival airports - todo change to left
    enriched_df = flights_df.join(
        arrival_airports, on="arrival_iata", how="left"
    )
    
    # Join the resulting DataFrame with departure airports - todo change to left
    enriched_df = enriched_df.join(
        departure_airports, on="departure_iata", how="left"
    )

    # Replace 'United States' with 'USA' in country-related columns
    country_columns = ['arrival_country', 'departure_country']
    for column in country_columns:
        enriched_df = enriched_df.withColumn(column, when(col(column) == 'United States', 'USA').otherwise(col(column)))

    # Remove the word 'airport' (case insensitive) from arrival_airport and departure_airport
    airport_columns = ['arrival_airport', 'departure_airport']
    for column in airport_columns:
        enriched_df = enriched_df.withColumn(column, regexp_replace(col(column), '(?i)\\s*airport', ''))

    # Remove duplicates
    enriched_df = enriched_df.dropDuplicates()
    
    return enriched_df

# Initialize Spark session
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("BatchProcessing") \
        .config("spark.jars", "/usr/local/hbase/lib/shc-core-1.1.0-2.1-s_2.11.jar") \
        .config("spark.files", "/usr/local/hbase/conf/hbase-site.xml") \
        .getOrCreate()
    
    # File paths
    earthquake_path = "hdfs://localhost:9000/user/project/nifi_out/earthquake/*.csv"
    flights_path = "hdfs://localhost:9000/user/project/nifi_out/flights/*.csv"
    airports_path = "hdfs://localhost:9000/user/project/nifi_out/airports/airports.csv"
    all_world_airports_path = "hdfs://localhost:9000/user/project/nifi_out/airports/airports_all.csv"
    
    # Load datasets    
    print("Loading airports_all data...")
    all_world_airports_df = ingest_all_world_airport_data(spark, all_world_airports_path)
    print("All World Airports Data:", all_world_airports_df.columns)

    print("Loading earthquake data...")
    earthquake_df = ingest_earthquake_data(spark, earthquake_path)
    print("Earthquake Data:", earthquake_df.columns)
    #save_to_hdfs(earthquake_df, "hdfs://localhost:9000/user/project/nifi_out/processed_earthquake/")

    print("Loading flight data...")
    flights_df = ingest_flight_data(spark, flights_path)
    print("Flights Data:", flights_df.columns)
    #save_to_hdfs(flights_df, "hdfs://localhost:9000/user/project/nifi_out/processed_flights/")

    print("Loading airport data...")
    airports_df = ingest_airport_data(spark, airports_path)
    print("Airports Data:", airports_df.columns)
    #save_to_hdfs(airports_df, "hdfs://localhost:9000/user/project/nifi_out/processed_airports/")
    
    # Merge the two datasets
    airports_df = merge_airport_datasets(airports_df, all_world_airports_df)
    airports_df.show(5, truncate=False)

    print("Earthquake Data:")
    earthquake_df.show(5, truncate=False)

    print("Flight Data:")
    flights_df.show(5, truncate=False)

    print("Data ingestion complete.")

    # Enrich flights data with airport details
    enriched_flights_df = enrich_flights_with_airports(flights_df, airports_df)
    print("Enriched Flights Data:")
    print(enriched_flights_df.columns)
    enriched_flights_df.show(5, truncate=False)

    # Save enriched data back to HDFS (optional)
    #output_path = "hdfs://localhost:9000/user/project/nifi_out/enriched_flights/"
    #enriched_flights_df.write.mode("overwrite").parquet(output_path)

    # Analyze IATA coverage
    print("IATA Coverage Analysis:")
    results = analyze_iata_coverage(flights_df, airports_df)
    print(results)

    # Analyze earthquake impact on flights
    print("Analyzing earthquake impact on flights...")
    impacted_flights_df = analyze_earthquake_impact_on_flights(enriched_flights_df, earthquake_df)
    impacted_flights_df.show(5, truncate=False)

    # Perform aggregations
    print("Performing aggregations on flight data...")
    top_departure_airports, top_arrival_airports, avg_departure_delay = aggregate_flight_data(enriched_flights_df)

    print("Top Departure Airports:")
    top_departure_airports.show(5, truncate=False)

    print("Top Arrival Airports:")
    top_arrival_airports.show(5, truncate=False)

    print("Average Departure Delay by Airport:")
    avg_departure_delay.show(5, truncate=False)

    final_summary_df = transform_data(enriched_flights_df, earthquake_df, spatial_radius_km=25)
    final_summary_df.show(5, truncate=False)

    # Enrich flights data with earthquake details
    flights_with_earthquakes_df = enrich_flights_with_earthquakes(enriched_flights_df, earthquake_df, spatial_radius_km=25)

    # Show the resulting DataFrame
    flights_with_earthquakes_df.show(5, truncate=False)

    # Filtering the DataFrame to display only rows where an earthquake occurred
    flights_with_earthquakes_df_filtered = flights_with_earthquakes_df.filter(
        col("earthquake_occurred") == True
    )

    # Displaying the first 5 rows where an earthquake occurred
    flights_with_earthquakes_df_filtered.show(5, truncate=False)

    # Calculating the number of flights impacted by earthquakes
    flights_disturbed_count = flights_with_earthquakes_df.filter(
        col("earthquake_occurred") == True
    ).count()

    print(flights_disturbed_count)

    # Total number of flights
    total_flights_count = flights_with_earthquakes_df.count()

    # Number of flights impacted by earthquakes
    flights_disturbed_count = flights_with_earthquakes_df.filter(
        col("earthquake_occurred") == True
    ).count()

    # Example usage
    pandas_df = flights_with_earthquakes_df.toPandas()

    # Save to HBase
    save_to_hbase_with_happybase(pandas_df, "flights_with_earthquakes")
    # Call the function
    check_hbase_table_structure("flights_with_earthquakes")