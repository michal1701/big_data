import happybase

def create_table(connection, table_name):
    """
    Create a table with three column families in HBase.
    """
    families = {
        "details": dict(),  # Informacje o pojeździe
        "accident": dict(), # Szczegóły wypadku
        "owner": dict(),    # Informacje o właścicielu
    }
    # Sprawdź, czy tabela już istnieje
    if table_name in connection.tables():
        print(f"Table '{table_name}' already exists.")
    else:
        connection.create_table(table_name, families)
        print(f"Table '{table_name}' created.")

def insert_data(connection, table_name):
    """
    Insert sample data into the HBase table.
    """
    vehicles_data = {
        "1": {  # Row ID
            "details:make": "Toyota",
            "details:model": "Corolla",
            "details:year": "2020",
            "accident:date": "2024-11-25",
            "accident:location": "Warsaw",
            "owner:name": "John Doe",
        },
        "2": {
            "details:make": "Honda",
            "details:model": "Civic",
            "details:year": "2018",
            "accident:date": "2023-10-12",
            "accident:location": "Krakow",
            "owner:name": "Jane Smith",
        },
        "3": {
            "details:make": "Ford",
            "details:model": "Focus",
            "details:year": "2019",
            "accident:date": "2022-06-30",
            "accident:location": "Gdansk",
            "owner:name": "Alice Brown",
        },
    }

    table = connection.table(table_name)
    for key, data in vehicles_data.items():
        table.put(key, data)
    print("Data inserted into the table.")

def read_rows(connection, table_name, limit=2):
    """
    Read and display the first 'limit' rows from the table.
    """
    table = connection.table(table_name)
    print(f"Displaying the first {limit} rows:")
    for key, data in table.scan(limit=limit):
        print(f"Row key: {key.decode()}, Data: {data}")

def update_row(connection, table_name):
    """
    Update the first row of the table and display the updated table.
    """
    table = connection.table(table_name)
    # Aktualizacja pierwszego wiersza
    table.put("1", {"details:model": "Camry"})
    print("First row updated.")

    # Wyświetlenie całej tabeli
    print("Updated table contents:")
    for key, data in table.scan():
        print(f"Row key: {key.decode()}, Data: {data}")

def log_results(connection, table_name):
    """
    Log table contents into a file.
    """
    table = connection.table(table_name)
    with open("hbase_log.txt", "w") as log_file:
        log_file.write("Table contents:\n")
        for key, data in table.scan():
            log_file.write(f"Row key: {key.decode()}, Data: {data}\n")
    print("Results logged to hbase_log.txt.")

def main():
    # Nazwa tabeli
    table_name = "Vehicles"

    # Połączenie z HBase
    connection = happybase.Connection(host='192.168.137.194', port=2222)

    try:
        # Utworzenie tabeli
        create_table(connection, table_name)

        # Wstawianie danych
        insert_data(connection, table_name)

        # Odczytanie i wyświetlenie pierwszych dwóch wierszy
        read_rows(connection, table_name, limit=2)

        # Aktualizacja i wyświetlenie zawartości tabeli
        update_row(connection, table_name)

        # Logowanie wyników do pliku
        log_results(connection, table_name)

    finally:
        # Zamknięcie połączenia z HBase
        connection.close()
        print("Connection to HBase closed.")

if __name__ == "__main__":
    main()