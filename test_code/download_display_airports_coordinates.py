#https://medium.com/@pagrutia/setting-up-a-big-data-environment-on-macos-hadoop-kafka-hive-and-zookeeper-312cf3fbafe3

import requests
import pandas as pd

# Replace with your actual API key
API_KEY = "4d494f33bafe446f3e1b7101a6a5cada"
url = "https://api.aviationstack.com/v1/airports"

# Parameters to retrieve airport data
params = {
    'access_key': API_KEY,
    'limit': 5,  # Adjust limit as needed
}

# Fetch data
response = requests.get(url, params=params)

if response.status_code == 200:
    # Convert the JSON data to a DataFrame
    airports_data = response.json().get('data', [])
    df = pd.json_normalize(airports_data)  # Normalize JSON into flat table format

    # Display the first 5 rows
    print("First 5 rows of data:")
    print(df.head())

    # Display data types of each column
    print("\nData Types of Each Column:")
    print(df.dtypes)
else:
    print("Failed to retrieve data:", response.json())