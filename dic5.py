import os
import glob
import pandas as pd
from flask import Flask, request, jsonify
from sqlalchemy import create_engine

app = Flask(__name__)

# Database connection settings
db_username = 'root'
db_password = '1234'
db_host = 'localhost'
db_port = '5432'
db_name = 'split_data'

# Create connection to PostgreSQL
engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

# Function to validate data (you can customize this further)
def validate_data(df):
    # Example validation rules
    if df.isnull().values.any():
        return {"status": "fail", "message": "Validation failed: Missing data found."}
    if not pd.api.types.is_numeric_dtype(df.get('some_column', pd.Series(dtype=float))):
        return {"status": "fail", "message": "'some_column' is not numeric or does not exist."}
    return {"status": "success", "message": "Validation passed."}

# Flask API endpoints

@app.route('/api/upload-excel', methods=['POST'])
def upload_excel():
    """
    Endpoint to upload and process Excel files.
    Expects:
        - A POST request with form-data containing Excel files.
    """
    # Ensure 'files' are included in the request
    if 'files' not in request.files:
        return jsonify({"status": "fail", "message": "No files part in the request."}), 400

    files = request.files.getlist('files')
    if not files:
        return jsonify({"status": "fail", "message": "No files uploaded."}), 400

    responses = []

    for file in files:
        try:
            # Read Excel file into DataFrame
            df = pd.read_excel(file)
            validation_result = validate_data(df)

            if validation_result['status'] == 'fail':
                responses.append({
                    "file": file.filename,
                    "status": "fail",
                    "message": validation_result['message']
                })
                continue

            # Insert valid data into PostgreSQL
            table_name = 'post'  # Change to your table name
            df.to_sql(table_name, engine, if_exists='append', index=False)

            responses.append({
                "file": file.filename,
                "status": "success",
                "message": f"Successfully inserted data into table '{table_name}'."
            })

        except Exception as e:
            responses.append({
                "file": file.filename,
                "status": "error",
                "message": str(e)
            })

    return jsonify(responses), 200

@app.route('/api/list-excel-files', methods=['GET'])
def list_excel_files():
    """
    Endpoint to list all Excel files in a specified directory.
    """
    directory_path = request.args.get('directory', '/Users/ACER/Desktop/Bulk_Excel_files')

    if not os.path.exists(directory_path):
        return jsonify({
            "status": "fail",
            "message": f"Directory does not exist: {directory_path}"
        }), 400

    # Get all Excel files
    excel_files = glob.glob(os.path.join(directory_path, "*.xlsx")) + glob.glob(os.path.join(directory_path, "*.xls"))

    return jsonify({
        "status": "success",
        "directory": directory_path,
        "files": [os.path.basename(file) for file in excel_files]
    }), 200

if __name__ == '__main__':
    app.run(debug=True)
