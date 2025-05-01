import duckdb
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from cboe_data import get_ticker_info
from config import CONFIG
from config import get_duckdb_connection


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
    print(spotPrice)
    # Create or replace table `db_chains`
    duckdb_conn.execute(r"""
    CREATE OR REPLACE TABLE db_chains AS FROM read_parquet('s3://lbr-files/GEX/GEXARCHIVE/*/*/*.parquet', hive_partitioning = true) ORDER by last_trade_date;
    CREATE INDEX idx ON db_chains (last_trade_date);
    CREATE OR REPLACE TABLE db_chains_test AS
        WITH src AS (SELECT * FROM db_chains)
    SELECT
      *,
      DATEDIFF('day', CURRENT_DATE, STRPTIME(regexp_extract(option, '(\d{6})[PC]', 1), '%y%m%d')) AS dte,
      (CASE WHEN "right" = 'C' THEN "gamma" * open_interest * 100 * spotPrice * spotPrice * 0.01 ELSE 0 END) AS CallGEX,
      (CASE WHEN "right" = 'P' THEN "gamma" * open_interest * 100 * spotPrice * spotPrice * 0.01 * -1 ELSE 0 END) AS PutGEX
    FROM (
      SELECT 
        strike,
        open_interest, 
        ? AS spotPrice, 
        option, 
        expiration_date, 
        last_trade_date, 
        "right", 
        "gamma" 
      FROM src
    );
    """, [spotPrice])
    return duckdb_conn

def store_option_chains_fromdf(chain_data):
    # Get the singleton DuckDB connection
    duckdb_conn = get_duckdb_connection()
    
    # Create table `option_chains_df` from DF
    duckdb_conn.execute("""
        CREATE OR REPLACE TABLE option_chains_df AS 
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
def get_gex_levels_data_df() -> pd.DataFrame:
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
    print("Quiery from duckDB Table option_chains_processed")
    print(df)
    
    # Return the DataFrame
    return df

def main():
    # Get the singleton DuckDB connection
    duckdb_conn = get_duckdb_connection()
    load_db()
    store_option_chains()
    updated_option_chains_gex()
    # print duckdb records
    test = duckdb_conn.sql("SELECT * FROM option_chains_processed").to_df()
    print(test)
    duckdb_conn.close()
       
if __name__ == "__main__":
    main()