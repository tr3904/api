from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)

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
    finally:
        cursor.close()

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
    finally:
        cursor.close()

# Function to read SQL query results
def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")
    finally:
        cursor.close()

@app.route('/api/data', methods=['GET', 'POST'])
def api_data():
    if request.method == 'GET':
        # Handle GET request
        return jsonify({"message": "GET request received", "data": None})
    elif request.method == 'POST':
        # Handle POST request
        try:
            data = request.json  # Expecting JSON data
            return jsonify({"message": "POST request received", "data": data})
        except Exception as e:
            return jsonify({"message": "Error processing data", "error": str(e)}), 400

@app.route('/api/backup', methods=['GET'])
def backup_endpoint():
    # Database credentials
    pw = "1234"
    db = "extract_1"

    # Establish server connection
    connection = server_connectivity('localhost', 'root', pw)

    if connection:
        # Create database
        create_database_query = "CREATE DATABASE IF NOT EXISTS extract_1"
        create_database(connection, create_database_query)

        # Connect to the database
        connection = create_db_connection('localhost', 'root', pw, db)

        # Create table
        create_orders_table = """
        CREATE TABLE IF NOT EXISTS banks (
            Digits INT PRIMARY KEY
        );
        """
        execute_query(connection, create_orders_table)

        # Insert data into the table
        insert_orders_table = """
        INSERT INTO banks (Digits) VALUES
        (900000), 
        (950000), 
        (890000), 
        (990000), 
        (1090000), 
        (1190000), 
        (1290000),
        (1990), 
        (1890), 
        (1790), 
        (1690), 
        (1590), 
        (1490), 
        (1390),
        (1330), 
        (1340), 
        (1350), 
        (1360), 
        (1370), 
        (1380), 
        (1390);
        """
        execute_query(connection, insert_orders_table)

        # Using the SELECT statement
        query = "SELECT * FROM banks;"
        results = read_query(connection, query)

        # Convert results to a list for easier JSON conversion
        data = [{"Digits": result[0]} for result in results]

        return jsonify({"message": "Backup completed", "data": data})
    else:
        return jsonify({"message": "Database connection failed"}), 500

if __name__ == '__main__':
    app.run(debug=True)
