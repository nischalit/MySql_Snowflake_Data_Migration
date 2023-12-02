import mysql.connector
from snowflake.connector import connect
from datetime import datetime
import json

class DatabaseConnector:
    def __init__(self, snowflake_user, snowflake_password, snowflake_account, snowflake_warehouse, snowflake_database,
                 snowflake_schema, snowflake_table, snowflake_role, mysql_host, mysql_user, mysql_password, mysql_db, mysql_table):
        # Snowflake credentials
        self.snowflake_user = 'sftestjeevan'
        self.snowflake_password = 'Ntsoft@7578'
        self.snowflake_account = 'TDQMPCW-QSB26155'
        self.snowflake_warehouse = 'COMPUTE_WH'
        self.snowflake_database = 'MULTIPLESOURCETOSNOWFLAKECONNDB'
        self.snowflake_schema = 'MULTIPLESOURCETOSNOWFLAKECONNSCHEMA'
        self.snowflake_table = 'JEEVAN_MYSQL_CONN_TABLE'
        self.snowflake_role = 'ACCOUNTADMIN'

        # MySQL connection parameters
        self.mysql_host = 'localhost'
        self.mysql_user =  'root'
        self.mysql_password =  '1234'
        self.mysql_db = 'jeevantechnologies'
        self.mysql_table =  'jeevan_mysql_sf'

        # Additional properties to capture SQL queries and row counts
        self.executed_sql_queries = []
        self.row_counts = {'source': 0, 'destination': 0}

        # Initialize connections
        self.snowflake_connection = self.connect_to_snowflake()
        self.mysql_connection = self.connect_to_mysql()

        # Create the data_transfer_log table if it doesn't exist
        self.create_data_transfer_log_table()

    def connect_to_snowflake(self):
        conn = connect(
            user=self.snowflake_user,
            password=self.snowflake_password,
            account=self.snowflake_account,
            warehouse=self.snowflake_warehouse,
            database=self.snowflake_database,
            schema=self.snowflake_schema,
            role=self.snowflake_role
        )
        print("Connected to Snowflake")
        return conn

    def connect_to_mysql(self):
        conn = mysql.connector.connect(
            host=self.mysql_host,
            user=self.mysql_user,
            password=self.mysql_password,
            database=self.mysql_db
        )
        print("Connected to MySQL")
        return conn

    def create_data_transfer_log_table(self):
        try:
            # Connect to Snowflake
            snowflake_conn = connect(
                user=self.snowflake_user,
                password=self.snowflake_password,
                account=self.snowflake_account,
                warehouse=self.snowflake_warehouse,
                database=self.snowflake_database,
                schema=self.snowflake_schema
            )

            # Create the data_transfer_log table if it doesn't exist
            snowflake_conn.cursor().execute('''
                CREATE TABLE IF NOT EXISTS data_transfer_log(
                    id               INTEGER,
                    timestamp        TIMESTAMP,
                    source           VARCHAR(255),
                    target           VARCHAR(255),
                    user             VARCHAR(255),
                    status           VARCHAR(255),
                    details          VARCHAR(1000),
                    sql_queries      VARIANT,
                    row_counts       VARIANT
                )
            ''')

            # Close the connection
            snowflake_conn.close()

        except Exception as e:
            print(f"Error creating data_transfer_log table: {str(e)}")

    
    def create_data_transfer_log_entry(self, status):
        try:
            snowflake_cursor = self.snowflake_connection.cursor()

            # Create a log entry
            log_entry = {
                'timestamp': datetime.now(),
                'source': self.mysql_table,
                'target': f"{self.snowflake_database}.{self.snowflake_schema}.{self.snowflake_table}",
                'user': self.snowflake_user,
                'status': 'Success' if status == "Data transfer completed successfully." else 'Error',
                'details': status,
                'sql_queries': self.executed_sql_queries,  # Removed json.dumps here
                'row_counts': self.row_counts  # Removed json.dumps here
            }

            # Insert log entry into data_transfer_log table
            snowflake_cursor.execute("""
                INSERT INTO DATA_TRANSFER_LOG_SCHEMA.DATA_TRANSFER_LOG(
                    TIMESTAMP,
                    SOURCE,
                    TARGET,
                    USER,
                    STATUS,
                    DETAILS,
                    SQL_QUERIES,
                    ROW_COUNTS
                ) 
                VALUES(
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                log_entry['timestamp'],
                log_entry['source'],
                log_entry['target'],
                log_entry['user'],
                log_entry['status'],
                log_entry['details'],
                json.dumps(log_entry['sql_queries']),  # Added json.dumps here
                json.dumps(log_entry['row_counts'])  # Added json.dumps here
            ))

            # Commit the transaction
            snowflake_cursor.execute("COMMIT")

        except Exception as e:
            print(f"Error logging data transfer details: {str(e)}")

    def copy_data(self):
        try:
            # Your existing data transfer logic here

            # For illustration purposes, let's assume you're fetching data from MySQL and inserting into Snowflake
            mysql_cursor = self.mysql_connection.cursor()
            snowflake_cursor = self.snowflake_connection.cursor()

            # Example: Fetch data from MySQL
            mysql_query = f"SELECT * FROM {self.mysql_table}"
            mysql_cursor.execute(mysql_query)
            mysql_data = mysql_cursor.fetchall()

            # Example: Insert data into Snowflake
            snowflake_query = f"INSERT INTO TRANSFER_LOG_DB.DATA_TRANSFER_LOG_SCHEMA.DATA_TRANSFER_LOG VALUES (%s, %s, %s)"  # Adjust as per your schema
            snowflake_cursor.executemany(snowflake_query, mysql_data)

            # Commit the transaction in both databases
            self.mysql_connection.commit()
            self.snowflake_connection.commit()

            # Update row counts
            self.row_counts['source'] = len(mysql_data)
            self.row_counts['destination'] = len(mysql_data)  # Adjust if the row count is different in Snowflake

            # Log executed SQL queries
            self.executed_sql_queries.append(mysql_query)
            self.executed_sql_queries.append(snowflake_query)

            return "Data transfer completed successfully."

        except Exception as e:
            return f"Error during data transfer: {str(e)}"


  

