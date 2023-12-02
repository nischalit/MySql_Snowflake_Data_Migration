import psycopg2
from snowflake.connector import connect
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConnector:
    def __init__(self, snowflake_user, snowflake_password, snowflake_account, snowflake_warehouse, snowflake_database,
                 snowflake_schema, snowflake_table, postgres_host, postgres_user, postgres_password, postgres_db,
                 postgres_schema, postgres_table):
        # Snowflake connection details
        self.snowflake_user = snowflake_user
        self.snowflake_password = snowflake_password
        self.snowflake_account = snowflake_account
        self.snowflake_warehouse = snowflake_warehouse
        self.snowflake_database = snowflake_database
        self.snowflake_schema = snowflake_schema
        self.snowflake_table = snowflake_table

        # PostgreSQL connection parameters
        self.postgres_host = postgres_host
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.postgres_db = postgres_db
        self.postgres_schema = postgres_schema
        self.postgres_table = postgres_table

        # Initialize connections
        logger.info("Connecting to Snowflake...")
        self.snowflake_connection = self.connect_to_snowflake()
        logger.info("Connecting to PostgreSQL...")
        self.postgres_connection = self.connect_to_postgres()

    def connect_to_snowflake(self):
        return connect(
            user=self.snowflake_user,
            password=self.snowflake_password,
            account=self.snowflake_account,
            warehouse=self.snowflake_warehouse,
            database=self.snowflake_database,
            schema=self.snowflake_schema
        )

    def connect_to_postgres(self):
        return psycopg2.connect(
            host=self.postgres_host,
            user=self.postgres_user,
            password=self.postgres_password,
            database=self.postgres_db
        )

    def copy_data(self):
        try:
            # Create Snowflake cursor
            snowflake_cursor = self.snowflake_connection.cursor()

            # Define your Snowflake INSERT statement based on your Snowflake table structure
            # Enclose the table name in double quotes
            insert_statement = f'INSERT INTO "{self.snowflake_table}" ("customer_id", "credit_score", "country", "gender", "age", "tenure", "balance", "products_number", "credit_card", "active_member", "estimated_salary", "churn") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

            # Create PostgreSQL cursor
            postgres_cursor = self.postgres_connection.cursor()

            # Fetch data from PostgreSQL and convert column names to uppercase
            postgres_cursor.execute(
                f"SELECT customer_id, credit_score, country, gender, age, tenure, balance, products_number, credit_card, active_member, estimated_salary, churn FROM {self.postgres_schema}.{self.postgres_table}")

            batch_size = 1000  # Set your preferred batch size
            data = []

            for row in postgres_cursor.fetchall():
                data.append(row)
                if len(data) >= batch_size:
                    snowflake_cursor.executemany(insert_statement, data)
                    data = []  # Clear the batch
                    self.snowflake_connection.commit()

            # Insert any remaining data
            if data:
                snowflake_cursor.executemany(insert_statement, data)
                self.snowflake_connection.commit()

            logger.info("Data transferred successfully")

        except Exception as e:
            logger.error(f"Error during data transfer: {str(e)}")

    def close_connections(self):
        # Close connections
        self.snowflake_connection.close()
        self.postgres_connection.close()

if __name__ == '__main__':
    # Provide your PostgreSQL and Snowflake credentials
    db_connector = DatabaseConnector(
        snowflake_user='sftestjeevan',
        snowflake_password='Ntsoft@7578',
        snowflake_account='TDQMPCW-QSB26155',
        snowflake_warehouse='COMPUTE_WH',
        snowflake_database='MULTIPLESOURCETOSNOWFLAKECONNDB',
        snowflake_schema='MULTIPLESOURCETOSNOWFLAKECONNSCHEMA',
        snowflake_table='JEEVAN_POSTGRES_CONN_TABLE',
        postgres_host='localhost',
        postgres_user='postgres',
        postgres_password='1234',
        postgres_db='jeevantechnologies_postgres_db',
        postgres_schema='jeevantechnologies_postgres_schema',
        postgres_table='jeevantechnologies_postgres_table'
    )

    # Example PostgreSQL to Snowflake data transfer
    db_connector.copy_data()

    # Close connections
    db_connector.close_connections()
