"""Configuration parameters"""
import os
import duckdb
from dotenv import load_dotenv
from threading import Lock

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
connection_lock = Lock()

def get_duckdb_connection():
    """
    Singleton pattern to manage the DuckDB connection.
    """
    global duckdb_conn
    with connection_lock:
        # Get env variables
        load_dotenv()
        AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', 'AWS_ACCESS_KEY_ID is missing')
        AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', 'AWS_SECRET_ACCESS_KEY is missing')
        S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL', 'S3_ENDPOINT_URL is missing')
        if duckdb_conn is None:
            try:
                duckdb_conn = duckdb.connect("md:gexapp")
                print("Connected to persistent DuckDB.")
            except Exception as e:
                print(f"Failed to open DuckDB file: {e}. Falling back to in-memory database.")
                duckdb_conn = duckdb.connect(':memory:')
            print("DuckDB connection initialized.")
            #  Configure modules    
            duckdb_conn.sql("INSTALL httpfs;")
            duckdb_conn.sql("LOAD httpfs;")
            duckdb_conn.sql("INSTALL delta;")
            duckdb_conn.sql("LOAD delta;")
            #  Create or replace secret using environment variables
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
    return duckdb_conn