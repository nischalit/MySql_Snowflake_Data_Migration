import cx_Oracle
from snowflake.connector import connect, ProgrammingError
import tempfile
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConnector:
    def __init__(self, snowflake_user, snowflake_password, snowflake_account, snowflake_warehouse, snowflake_database,
                 snowflake_schema, snowflake_table, oracle_user, oracle_password, oracle_dsn, oracle_table):
        # Snowflake credentials
        self.snowflake_user = snowflake_user
        self.snowflake_password = snowflake_password
        self.snowflake_account = snowflake_account
        self.snowflake_warehouse = snowflake_warehouse
        self.snowflake_database = snowflake_database
        self.snowflake_schema = snowflake_schema
        self.snowflake_table = snowflake_table

        # Oracle credentials
        self.oracle_user = oracle_user
        self.oracle_password = oracle_password
        self.oracle_dsn = oracle_dsn
        self.oracle_table = oracle_table

        # Identify the LOB column(s)
        self.lob_columns = ["Inclusion Criteria", "Exclusion Criteria"]

        # Specify the columns to be removed
        self.columns_to_remove = ["Inclusion Criteria", "Exclusion Criteria", "Public title", "Scientific title"]

        # Define a mapping of Oracle data types to Snowflake data types
        self.data_type_mapping = {
            cx_Oracle.NUMBER: "NUMBER",
            cx_Oracle.STRING: "STRING",
            cx_Oracle.DATETIME: "TIMESTAMP",
            cx_Oracle.CLOB: "STRING",  # You can adjust this based on your needs
            cx_Oracle.BLOB: "BINARY",  # You can adjust this based on your needs
        }

        # Initialize Snowflake connection
        self.snowflake_conn = self.connect_to_snowflake()
        logger.info("Connecting to Snowflake...")

        # Initialize Oracle connection
        self.oracle_conn = self.connect_to_oracle()
        logger.info("Connected to Oracle")

        # Create a temporary directory for storing LOB data files
        self.temp_dir = tempfile.mkdtemp()

        # Create Snowflake cursor
        self.snowflake_cursor = self.snowflake_conn.cursor()

        # Create Oracle cursor
        self.oracle_cursor = self.oracle_conn.cursor()

    def connect_to_snowflake(self):
        return connect(
            user=self.snowflake_user,
            password=self.snowflake_password,
            account=self.snowflake_account,
            warehouse=self.snowflake_warehouse,
            database=self.snowflake_database,
            schema=self.snowflake_schema
        )

    def connect_to_oracle(self):
        return cx_Oracle.connect(self.oracle_user, self.oracle_password, self.oracle_dsn)

    def copy_data(self):
        try:
            # Define the query to transfer non-LOB data
            non_lob_query = f"SELECT * FROM {self.oracle_table}"

            # Execute the query for non-LOB data
            self.oracle_cursor.execute(non_lob_query)

            data_transferred = 0

            # Transfer non-LOB data
            for row in self.oracle_cursor:
                try:
                    # Exclude specified columns from the row
                    row = [value for idx, value in enumerate(row) if idx not in self.columns_to_remove]

                    # Handle 'All' values in numeric columns
                    for idx in range(len(row)):
                        if row[idx] == 'All':
                            # Replace 'All' with a default numeric value (e.g., -1)
                            row[idx] = -1

                    # Convert Oracle data types to Snowflake data types
                    converted_row = []
                    for idx, value in enumerate(row):
                        snowflake_data_type = self.data_type_mapping.get(self.oracle_cursor.description[idx][1])
                        converted_value = value if snowflake_data_type is None else (snowflake_data_type, value)
                        converted_row.append(converted_value)

                    self.snowflake_cursor.execute(
                        f'INSERT INTO {self.snowflake_table} VALUES ({", ".join([f"%s" for i in range(len(converted_row))])})',
                        converted_row)

                    data_transferred += 1
                except Exception as e:
                    logger.error(f"Error transferring non-LOB data: {str(e)}")

            # Commit changes to Snowflake
            self.snowflake_conn.commit()

            # Transfer LOB data
            for lob_column in self.lob_columns:
                # Fetch LOB data from Oracle
                self.oracle_cursor.execute(f"SELECT {lob_column} FROM {self.oracle_table}")
                lob_data = self.oracle_cursor.fetchone()[0].read()

                # Write LOB data to a temporary file
                lob_file = os.path.join(self.temp_dir, lob_column + ".txt")
                with open(lob_file, "wb") as f:
                    f.write(lob_data)

                # Use the Snowflake COPY INTO command to load LOB data
                self.snowflake_cursor.execute(f'PUT file://{lob_file} @%{self.snowflake_table} ("{lob_column}")')

            # Commit changes to Snowflake again
            self.snowflake_conn.commit()

            # Clean up temporary directory
            for file in os.listdir(self.temp_dir):
                os.remove(os.path.join(self.temp_dir, file))
            os.rmdir(self.temp_dir)

            logger.info(f"Transferred {data_transferred} rows of non-LOB data and LOB data from Oracle to Snowflake")

        except Exception as e:
            logger.error(f"Error during data transfer: {str(e)}")

    def close_connections(self):
        # Close connections
        self.snowflake_cursor.close()
        self.oracle_cursor.close()
        self.snowflake_conn.close()
        self.oracle_conn.close()

if __name__ == '__main__':
    # Provide your Snowflake and Oracle credentials
    db_connector = DatabaseConnector(
        snowflake_user='sftestjeevan',
        snowflake_password='Ntsoft@7578',
        snowflake_account='TDQMPCW-QSB26155',
        snowflake_warehouse='COMPUTE_WH',
        snowflake_database='MULTIPLESOURCETOSNOWFLAKECONNDB',
        snowflake_schema='MULTIPLESOURCETOSNOWFLAKECONNSCHEMA',
        snowflake_table='JEEVAN_ORACLEDB_CONN_TABLE_NEW',
        oracle_user='system',
        oracle_password='1234',
        oracle_dsn='localhost:1521/xe',
        oracle_table='jeevantechnologies_oracle_table'
    )

    # Example Oracle to Snowflake data transfer
    db_connector.copy_data()

    # Close connections
    db_connector.close_connections()
