from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import Error
import pandas as pd
import os

app = Flask(__name__)

# Sample route to simulate an API connection
@app.route('/api/data', methods=['GET', 'POST'])
def api_data():
    if request.method == 'GET':
        # Handle GET request
        return jsonify({"message": "GET request received", "data": None})
    elif request.method == 'POST':
        # Handle POST request
        data = request.json  # Expecting JSON data
        return jsonify({"message": "POST request received", "data": data})

# Function to establish server connectivity
def server_connectivity(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password
        )
        print("MySQL connection is established successfully")
    except Error as err:
        print(f"Error: '{err}'")
    return connection

# Function to create a database
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

# Function to connect to a specific database
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name
        )
        print("Connected to database successfully")
    except Error as err:
        print(f"Error: '{err}'")
    return connection

# Function to execute an SQL query
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as err:
        print(f"Error: '{err}'")

# Validation function
def check_validate(row):
    required_types = {
        "SHG Loan A/c No": int,
        "Date of Closer": str,
        "Name of the SHG": str,
        "SHG SB Account No": int,
        "IFSC Code": int,
        "Branch code": int,
    }
    for column, expected_type in required_types.items():
        if not isinstance(row[column], expected_type):
            print("Not Validated")
            return False
    print("Validated")
    return True

@app.route('/api/backup', methods=['GET'])
def backup_endpoint():
    # Database credentials
    pw = "1234"
    db = "bank_2"

    # Establish server connection
    connection = server_connectivity('localhost', 'root', pw)

    if connection:
        # Create database
        create_database_query = "CREATE DATABASE IF NOT EXISTS bank_2"
        create_database(connection, create_database_query)

        # Connect to the database
        connection = create_db_connection('localhost', 'root', pw, db)

        # Create table
        create_orders_table = """
        CREATE TABLE IF NOT EXISTS banks (
            `SHG Loan A/c No` int primary key,
            `Date of Closer` date not null,
            `Name of the SHG` varchar(30) not null,
            `SHG SB Account No` int not null,
            `IFSC Code` int not null,
            `Branch code` int
        );
        """
        execute_query(connection, create_orders_table)

        # Read and validate data
        file_path = "Banks1.csv"
        if not os.path.exists(file_path):
            return jsonify({"message": "File not found", "error": f"'{file_path}' does not exist"})

        try:
            df5 = pd.read_csv(file_path)
            print(df5)
            df5.apply(check_validate, axis=1)
        except Exception as e:
            print(f"Error processing CSV: {e}")
            return jsonify({"message": "Error processing CSV", "error": str(e)})

    return jsonify({"message": "Backup endpoint reached", "status": "OK"})

if __name__ == '__main__':
    app.run(debug=True)
