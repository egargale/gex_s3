"""Configuration parameters"""
import os
import duckdb
from dotenv import load_dotenv

CONFIG = {
    'CBOE_TICKER':'SPX',
    'YFIN_TICKER':'^SPX',
    'CLOUDINARY_TAG':'GEX',
    'MONGODB_COLECTION_UPLOADS':'gex_uploads',
    'MONGODB_COLECTION_QUOTE_INFO':'gex_quote_info',
    'MONGODB_COLECTION_GEX_STRIKES':'gex_strikes',
    'MONGODB_COLECTION_GEX_PROFILE':'gex_profile',
    'MONGODB_COLECTION_GEX_ZERO_GAMMA':'gex_zero',
}

duckdb_conn = None

def get_duckdb_connection():
    """
    Singleton pattern to manage the DuckDB connection.
    """
    global duckdb_conn
    if duckdb_conn is None:
        try:
            # Attempt to connect to the persistent DuckDB file
            duckdb_conn = duckdb.connect('/tmp/test.duckdb')
            print("Connected to persistent DuckDB.")
        except Exception as e:
            print(f"Failed to open DuckDB file: {e}. Falling back to in-memory database.")
            duckdb_conn = duckdb.connect(':memory:')  # Use in-memory DB
       
        print("DuckDB connection initialized.")
         # Get env variables
        load_dotenv()
        AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', 'AWS_ACCESS_KEY_ID is missing')
        AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', 'AWS_SECRET_ACCESS_KEY is missing')
        S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL', 'S3_ENDPOINT_URL is missing')
        # Create or replace secret using environment variables
        duckdb_conn.execute(f"""
        CREATE OR REPLACE SECRET s3_secret (
            TYPE s3,
            PROVIDER config,
            KEY_ID "{AWS_ACCESS_KEY_ID}",
            SECRET "{AWS_SECRET_ACCESS_KEY}",
            REGION 'eu-central-1',
            ENDPOINT "{S3_ENDPOINT_URL}",
            URL_STYLE 'path'
        );
        """)
        # duckdb_conn.sql(
        #     "attach 's3://lbr-files/GEX/gexdb.duckdb' as external_db"
        # )
        # duckdb_conn.sql("use external_db")
        # for testing use in memory 
        # duckdb_conn.sql("USE memory;")
        duckdb_conn.sql("INSTALL httpfs;")
        duckdb_conn.sql("LOAD httpfs;")
        duckdb_conn.sql("INSTALL delta;")
        duckdb_conn.sql("LOAD delta;")
    return duckdb_conn