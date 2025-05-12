import duckdb
import pandas as pd
import os
from dotenv import load_dotenv
from cboe_data import get_ticker_info
from config import CONFIG
from config import get_duckdb_connection
from deltalake import write_deltalake

# Get env variables
load_dotenv()

# Create connection for AWS S3
# ================================
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', 'AWS_ACCESS_KEY_ID is missing')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', 'AWS_SECRET_ACCESS_KEY is missing')
S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL', 'S3_ENDPOINT_URL is missing')
S3_BUCKET = os.environ.get('S3_BUCKET', 'S3_BUCKET is missing')
DELTA_TABLE = os.environ.get('DELTA_TABLE', 'DELTA_TABLE is missing')

def load_db():
    # Get the singleton DuckDB connection
    duckdb_conn = get_duckdb_connection()
    
    option_chain_ticker_info = get_ticker_info(symbol=CONFIG['CBOE_TICKER'])[0]
    # Read S3 parquet files and setup a DB
    # Ensure the DataFrame has a 'close' row and extract its value
    if 'close' in option_chain_ticker_info.index:
        spotPrice = option_chain_ticker_info.loc['close'].values[0]  # Extract the value from the 'close' row
    else:
        raise
    print(f"SPX close price: {spotPrice}")
    
    # Create or replace table `db_chains`
    #  CREATE OR REPLACE TABLE db_chains AS FROM read_parquet('s3://lbr-files/GEX/GEXARCHIVE/*/*/*.parquet', hive_partitioning = true) WHERE last_trade_date >= Now() - INTERVAL '3 DAYS' ORDER by last_trade_date;
    duckdb_conn.execute(r"""
CREATE OR REPLACE TABLE db_chains AS FROM read_parquet('s3://lbr-files/GEX/GEXARCHIVE/*/*/*.parquet', hive_partitioning = true)
    WHERE last_trade_date >= Now() - INTERVAL '3 DAYS'
    ORDER by last_trade_date;
    """)
    print(f"Records loaded: {duckdb_conn.execute("SELECT COUNT(*) FROM db_chains").fetchone()[0]}")
    #
    # Create or replace table `db_chains_test`
    duckdb_conn.execute(r"""
CREATE OR REPLACE TABLE db_chains_test AS
    SELECT
        *,
        ? AS spotPrice,
        DATEDIFF('day', CURRENT_DATE, expiration_date) AS dte,
        (CASE WHEN "right" = 'C' THEN "gamma" * open_interest * 100 * spotPrice * spotPrice * 0.01 ELSE 0 END) AS CallGEX,
        (CASE WHEN "right" = 'P' THEN "gamma" * open_interest * 100 * spotPrice * spotPrice * 0.01 * -1 ELSE 0 END) AS PutGEX
    FROM db_chains;
    """, [spotPrice])
    print(f"Records loaded: {duckdb_conn.execute('SELECT COUNT(*) FROM db_chains_test').fetchone()[0]}")
    #
    # Create an index on the `last_trade_date`, `expiration_date`, and `strike` columns
    duckdb_conn.execute(r"""
CREATE INDEX option_idx ON db_chains_test (last_trade_date, expiration_date, strike);
    """)
    
    return duckdb_conn

def store_option_chains_fromdf(chain_data):
    # Get the singleton DuckDB connection
    duckdb_conn = get_duckdb_connection()
    
    # Create table `option_chains_df` from DF
    duckdb_conn.execute(r"""
        CREATE OR REPLACE TABLE option_chains AS 
        SELECT * FROM chain_data;
        """)
    
    return duckdb_conn
def store_option_chains():
    
    # Get the singleton DuckDB connection
    duckdb_conn = get_duckdb_connection()
    
    # Create table `option_chains` by reading JSON data
    duckdb_conn.execute(r"""
    CREATE OR REPLACE TABLE option_chains AS 
    SELECT * FROM read_json_auto('https://cdn.cboe.com/api/global/delayed_quotes/options/_SPX.json');
    """)
    
    return duckdb_conn
def updated_option_chains_gex():
    # Get the singleton DuckDB connection
    duckdb_conn = get_duckdb_connection()
    
    # Create table option_chains_processed with transformations
    duckdb_conn.execute(r"""
CREATE OR REPLACE TABLE option_chains_processed AS
  WITH src AS (SELECT *
                FROM option_chains)
    SELECT 
      symbol,
      last_trade_date,
      spotPrice,
      u.*,
      STRPTIME(regexp_extract(u.option, '(\d{6})(?:P|C)',1), '%y%m%d') AS expiration_date,
      TRY_CAST(regexp_extract(u.option, '[PC](\d+)', 1) AS integer) / 1000 AS strike,
      regexp_extract(u.option, '\d{6}([PC])', 1) AS right,
      DATEDIFF('day', CURRENT_DATE, STRPTIME(regexp_extract(u.option, '(\d{6})[PC]', 1), '%y%m%d')) AS dte,
      (CASE WHEN "right" = 'C' THEN u."gamma" * u.open_interest * 100 * spotPrice * spotPrice * 0.01 ELSE 0 END) AS CallGEX,
      (CASE WHEN "right" = 'P' THEN u."gamma" * u.open_interest * 100 * spotPrice * spotPrice * 0.01 * -1 ELSE 0 END) AS PutGEX
    FROM (
      SELECT symbol, "timestamp" AS last_trade_date, data.current_price AS spotPrice, UNNEST(data.options) AS u FROM src);
    """)

    duckdb_conn.execute(r"""
COPY (
    SELECT (* EXCLUDE (spotPrice, dte, CallGEX, PutGEX)),
           YEAR(last_trade_date) AS year, 
           MONTH(last_trade_date) AS month 
    FROM option_chains_processed
) TO 's3://lbr-files/GEX/GEXARCHIVE'(FORMAT PARQUET, PARTITION_BY (year, month), APPEND);
    """)
    
    duckdb_conn.execute(r"""
COPY option_chains_processed TO 's3://lbr-files/GEX/test.xlsx' WITH (FORMAT xlsx, HEADER true);    
    """)
    
    return duckdb_conn
def calculate_gex_levels_df() -> pd.DataFrame:
    # Get the singleton DuckDB connection
    duckdb_conn = get_duckdb_connection()
    """
    Executes a SQL query to aggregate GEX levels data and returns it as a DataFrame.

    Parameters:
        con: DuckDB connection object.

    Returns:
        pd.DataFrame: Aggregated GEX levels data.
    """
    query = r"""
    SELECT
        dte,
        strike,
        SUM(CallGEX) / 1e9 AS total_call_gex,
        SUM(PutGEX) / 1e9 AS total_put_gex,
        (SUM(CallGEX) + SUM(PutGEX)) / 1e9 AS total_gamma
    FROM option_chains_processed
    WHERE open_interest >= 100
    GROUP BY dte, strike
    ORDER BY dte, strike;
    """
    # Execute the query and fetch the result as a DataFrame
    df = duckdb_conn.execute(query).fetchdf()
    
    # Print the DataFrame for debugging purposes
    # print("Quiery from duckDB Table option_chains_processed")
    # print(df)
    
    # Return the DataFrame
    return df
def write_gex_levels_to_deltatable(file_path: str):
    """
    Writes GEX levels data to an S3 DeltaTable.

    Parameters:
        file_path (str): Path to S3  DeltaTable.
    """
    # add s3a:// to file_path
    file_path = 's3a://' + file_path
    # Get GEX Levels
    df = calculate_gex_levels_df()
    storage_options = {
        'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
        'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY,
        'AWS_ENDPOINT_URL': 'https://s3.eu-central-1.wasabisys.com',
    }
    write_deltalake(
        file_path,
        df,
        mode="overwrite",
        storage_options=storage_options
    )
    
    print(f"Written {df.count()} records to {file_path}")
    return
def get_gex_levels_from_deltatable() -> pd.DataFrame:
    """
    Reads GEX levels data from an S3 DeltaTable.

    Returns:
        pd.DataFrame: GEX levels data.
    """
     # Get the singleton DuckDB connection
    duckdb_conn = get_duckdb_connection()
    # add s3a:// to DELTA_TABLE
    delta_table_url = 's3://' + DELTA_TABLE
    df = duckdb_conn.sql(f"SELECT * FROM delta_scan('{delta_table_url}')").to_df()
    return df
def main():
    # Get the singleton DuckDB connection
    duckdb_conn = get_duckdb_connection()
    load_db()
    store_option_chains()
    updated_option_chains_gex()
    write_gex_levels_to_deltatable(DELTA_TABLE)
    # print duckdb records
    # test_read_parquet = duckdb_conn.sql("SELECT * FROM option_chains_processed").to_df()
    # print(test_read_parquet)
    print(DELTA_TABLE)
    # delta_table_url = 's3://' + DELTA_TABLE
    # test_read_deltatable = duckdb_conn.sql(f"SELECT * FROM delta_scan('{delta_table_url}')").to_df()
    # print(test_read_deltatable)
    df = get_gex_levels_from_deltatable()
    print(df)
    duckdb_conn.close()
       
if __name__ == "__main__":
    main()