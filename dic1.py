from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import Error
import pandas as pd

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
        "Loan_A/C_No": int,
        "Transaction_Date": str,
        "Particulars": str,
        "Type_of_Transaction": str,
        "Debit": int,
        "Credit": int,
        "Closing_Balance": int,
        "Branch_Code or IFSC": str
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
    db = "bank_1"

    # Establish server connection
    connection = server_connectivity('localhost', 'root', pw)

    if connection:
        # Create database
        create_database_query = "CREATE DATABASE IF NOT EXISTS bank_1"
        create_database(connection, create_database_query)

        # Connect to the database
        connection = create_db_connection('localhost', 'root', pw, db)

        # Create table
        create_orders_table = """
        CREATE TABLE IF NOT EXISTS banks (
            `Loan_A/C_No` INT PRIMARY KEY,
            `Transaction_Date` DATETIME NOT NULL,
            `Particulars` VARCHAR(20) NOT NULL,
            `Type_of_Transaction` VARCHAR(20) NOT NULL,
            `Debit` INT NOT NULL,
            `Credit` INT NOT NULL,
            `Closing_Balance` INT NOT NULL,
            `Branch_Code or IFSC` VARCHAR(20)
        );
        """
        execute_query(connection, create_orders_table)

        # Read and validate data
        try:
            df1 = pd.read_csv("Banks.csv")
            print(df1)
            df1.apply(check_validate, axis=1)
        except Exception as e:
            print(f"Error processing CSV: {e}")
            return jsonify({"message": "Error processing CSV", "error": str(e)})

    return jsonify({"message": "Backup endpoint reached", "status": "OK"})

    df1 = pd.read_csv("Banks.csv")
    print(df1)

if __name__ == '__main__':
    app.run(debug=True)




