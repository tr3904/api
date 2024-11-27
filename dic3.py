from flask import Flask, request, jsonify
import pandas as pd

app = Flask(__name__)

# Define a GET endpoint
@app.route('/api/backup', methods=['GET'])
def get_backup():
    return jsonify({
        "status": "success",
        "message": "API is running!",
        "data": None
    }), 200

# Define a POST endpoint
@app.route('/api/backup', methods=['POST'])
def post_backup():
    try:
        # Get the JSON data sent in the request
        data = request.get_json()

        # Validate input data
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400

        # Example: expecting file paths for CSVs
        if 'Banks' not in data or 'Banks1' not in data:
            return jsonify({"status": "error", "message": "File paths for CSVs are missing"}), 400

        file1_path = data['Banks.csv']
        file2_path = data['Banks1.csv']
        # Load the CSV files
        try:
            df1 = pd.read_csv(file1_path)
            df2 = pd.read_csv(file2_path)
        except Exception as e:
            return jsonify({"status": "error", "message": f"Error reading CSV files: {str(e)}"}), 500

        # Process the data
        df3 = pd.DataFrame({
            "SHG Loan A/c No": df1["Loan_A/C_No"],
            "Closing_Balance": df1["Closing_Balance"],
            "Branch_Code or IFSC": df1["Branch_Code or IFSC"],
            "IFSC Code": df2["IFSC Code"],
            "Branch code": df2["Branch code"]
        })
        print(df3)

        # Convert columns to lists
        closing_balance_list = df3["Closing_Balance"].tolist()
        ifsc_code_list = df3["IFSC Code"].tolist()
        branch_code_list = df3["Branch code"].tolist()

        # Return the processed data as a response
        return jsonify({
            "status": "success",
            "message": "Data processed successfully",
            "Closing_Balance": closing_balance_list,
            "IFSC_Code": ifsc_code_list,
            "Branch_Code": branch_code_list
        }), 201

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    # Run the Flask app on localhost
    app.run(debug=True)
