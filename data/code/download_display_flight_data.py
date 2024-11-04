import requests
import pandas as pd

# Replace with your actual API key
API_KEY = "00de11eb3903efd40b93d649920d94fc"
url = "https://api.aviationstack.com/v1/flights"

# Parameters to retrieve flights data
params = {
    'access_key': API_KEY,
    'limit': 5,  # Adjust limit to fetch a small sample of data
}

# Fetch data
response = requests.get(url, params=params)

if response.status_code == 200:
    # Convert the JSON data to a DataFrame
    flights_data = response.json().get('data', [])
    df = pd.json_normalize(flights_data)  # Normalize JSON into flat table format

    # Print all columns and data for the first 5 rows
    print("First 5 rows of data:")
    print(df.head())

    # Print data types of each column
    print("\nData Types of Each Column:")
    print(df.dtypes)
else:
    print("Failed to retrieve data:", response.json())
