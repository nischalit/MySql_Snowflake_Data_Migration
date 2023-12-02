from flask import Flask, render_template, request
from connectors.mysql_connector import DatabaseConnector as MySQLConnector
from datetime import datetime
from snowflake.connector import connect 
import json

app = Flask(__name__)

# Function to create the Snowflake database and schema
def create_database_and_schema():
    try:
        # Connect to Snowflake
        snowflake_conn = connect(
            user='sftestjeevan',
            password='Ntsoft@7578',
            account='TDQMPCW-QSB26155',
            warehouse='COMPUTE_WH',
            database='MULTIPLESOURCETOSNOWFLAKECONNDB',
            schema='PUBLIC'
        )

        
        # Create the transfer_log_db database if it doesn't exist
        snowflake_conn.cursor().execute("CREATE DATABASE IF NOT EXISTS TRANSFER_LOG_DB")

        # Switch to the transfer_log_db database
        snowflake_conn.cursor().execute("USE DATABASE TRANSFER_LOG_DB")

        # Create the data_transfer_log_schema schema if it doesn't exist
        snowflake_conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS DATA_TRANSFER_LOG_SCHEMA")

        # Switch to the data_transfer_log_schema schema
        snowflake_conn.cursor().execute("USE SCHEMA DATA_TRANSFER_LOG_SCHEMA")

        # Create the data_transfer_log table if it doesn't exist
        snowflake_conn.cursor().execute('''
                CREATE OR REPLACE TABLE TRANSFER_LOG_DB.DATA_TRANSFER_LOG_SCHEMA.DATA_TRANSFER_LOG (
                    ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 ORDER,
                    TIMESTAMP TIMESTAMP_NTZ(9),
                    SOURCE VARCHAR(255),
                    TARGET VARCHAR(255),
                    USER VARCHAR(255),
                    STATUS VARCHAR(255),
                    DETAILS VARCHAR(1000),
                    SQL_QUERIES VARCHAR(100),
                    ROW_COUNTS VARCHAR(100),
                    PRIMARY KEY (ID)
                );
            ''')

        # Close the connection
        snowflake_conn.close()

    except Exception as e:
        print(f"Error creating database and schema: {str(e)}")

# Function to update the transfer log in Snowflake

# ... (existing code)

# Function to update the transfer log in Snowflake
def update_transfer_log(transfer_log):
    try:
        # Connect to Snowflake
        snowflake_conn = connect(
            user='sftestjeevan',
            password='Ntsoft@7578',
            account='TDQMPCW-QSB26155',
            warehouse='COMPUTE_WH',
            database='TRANSFER_LOG_DB',
            schema='DATA_TRANSFER_LOG_SCHEMA'  # Update to the schema where DATA_TRANSFER_LOG table exists
        )

        # Create the data_transfer_log table if it doesn't exist
        snowflake_conn.cursor().execute('''
            CREATE TABLE IF NOT EXISTS data_transfer_log (
                id               INTEGER,
                timestamp        TIMESTAMP,
                source           VARCHAR(255),
                target           VARCHAR(255),
                user             VARCHAR(255),
                status           VARCHAR(255),
                details          VARCHAR(1000),
                sql_queries      VARCHAR(100),
                row_counts       VARCHAR(100)
            );
        ''')

        # Insert log entry into data_transfer_log table
        snowflake_conn.cursor().execute("""
            INSERT INTO data_transfer_log (
                timestamp,
                source,
                target,
                user,
                status,
                details,
                sql_queries,
                row_counts
            ) 
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            transfer_log["timestamp"],
            transfer_log["source"],
            transfer_log["target"],
            transfer_log["user"],
            transfer_log["status"],
            transfer_log["details"],
            json.dumps(transfer_log["sql_queries"]),  # Ensure proper JSON formatting
            json.dumps(transfer_log["row_counts"])    # Ensure proper JSON formatting
        ))

        # Commit the transaction
        snowflake_conn.cursor().execute("COMMIT")

    except Exception as e:
        print(f"Error updating transfer log: {str(e)}")

# ... (rest of the existing code)

@app.route('/transfer_data', methods=['GET', 'POST'])
def transfer_data():
    status = None

    if request.method == 'POST':
        source = 'mysql'
        target = 'snowflake'

        # Update credentials (Consider using a configuration file or environment variables)
        snowflake_user = 'sftestjeevan'
        snowflake_password = 'Ntsoft@7578'
        snowflake_account = 'TDQMPCW-QSB26155'
        snowflake_warehouse = 'COMPUTE_WH'
        snowflake_database = 'MULTIPLESOURCETOSNOWFLAKECONNDB'
        snowflake_schema = 'MULTIPLESOURCETOSNOWFLAKECONNSCHEMA'
        snowflake_table = 'JEEVAN_MYSQL_CONN_TABLE'
        snowflake_role = 'ACCOUNTADMIN'
        mysql_host = 'localhost'
        mysql_user = 'root'
        mysql_password = '1234'
        mysql_db = 'jeevantechnologies'
        mysql_table = 'jeevan_mysql_sf'

        mysql_connector = MySQLConnector(
            snowflake_user=snowflake_user,
            snowflake_password=snowflake_password,
            snowflake_account=snowflake_account,
            snowflake_warehouse=snowflake_warehouse,
            snowflake_database=snowflake_database,
            snowflake_schema=snowflake_schema,
            snowflake_table=snowflake_table,
            snowflake_role=snowflake_role,
            mysql_host=mysql_host,
            mysql_user=mysql_user,
            mysql_password=mysql_password,
            mysql_db=mysql_db,
            mysql_table=mysql_table
        )

        # Get timestamp
        timestamp_value = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Example data (replace with actual data from your request)
        transfer_log_data = {
            "timestamp": timestamp_value,
            "source": source,
            "target": target,
            "user": snowflake_user,
            "status": "success",  # You may adjust this based on your actual status
            "details": "Example details",
            "sql_queries": "Example SQL queries",
            "row_counts": "Example row counts"
        }

        status = mysql_connector.copy_data()

        # Update transfer log in Snowflake
        transfer_log_data["status"] = status
        update_transfer_log(transfer_log_data)

    return render_template('index.html', status=status)

if __name__ == '__main__':
    create_database_and_schema()
    app.run(debug=True, port=5001)
