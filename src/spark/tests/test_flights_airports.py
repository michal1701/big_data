# this test has been conducted on batch_preprocessing.py - this is the visualisation for the function analyze_iata_coverage

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
