import duckdb
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from cboe_data import get_ticker_info
from config import CONFIG


def get_duckdb_conn():
    duckdb_conn = duckdb.connect()

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
    duckdb_conn.sql("USE memory;")
    duckdb_conn.sql("INSTALL httpfs;")
    duckdb_conn.sql("LOAD httpfs;")
    
    return duckdb_conn

def load_db(con):
    option_chain_ticker_info = get_ticker_info(symbol=CONFIG['CBOE_TICKER'])[0]
    # Read S3 parquet files and setup a DB
    # Ensure the DataFrame has a 'close' row and extract its value
    if 'close' in option_chain_ticker_info.index:
        spotPrice = option_chain_ticker_info.loc['close'].values[0]  # Extract the value from the 'close' row
    else:
        raise
    print(spotPrice)
    # Create or replace table `db_chains`
    con.execute(r"""
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
    return con

def store_option_chains_fromdf(con, chain_data):
    # Create table `option_chains_df` from DF
    con.execute("""
        CREATE OR REPLACE TABLE option_chains_df AS 
        SELECT * FROM chain_data;
        """)
    
    return con
def store_option_chains(con):
    # Create table `option_chains` by reading JSON data
    con.execute(r"""
    CREATE OR REPLACE TABLE option_chains AS 
    SELECT * FROM read_json_auto('https://cdn.cboe.com/api/global/delayed_quotes/options/_SPX.json');
    """)
    
    return con

def updated_option_chains_gex(con):

    # Create table option_chains_processed with transformations
    con.execute(r"""
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

    con.execute(r"""
COPY (
    SELECT (* EXCLUDE (spotPrice, dte, CallGEX, PutGEX)),
           YEAR(last_trade_date) AS year, 
           MONTH(last_trade_date) AS month 
    FROM option_chains_processed
) TO 's3://lbr-files/GEX/GEXARCHIVE'(FORMAT PARQUET, PARTITION_BY (year, month), APPEND);
    """)
    
    return con

def main():
    # Initialize variables
    duckdb_conn = None
    # Create a connection to DuckDB
    duckdb_conn = get_duckdb_conn()
    duckdb_conn = load_db(duckdb_conn)
    duckdb_conn = store_option_chains(duckdb_conn)
    duckdb_conn = updated_option_chains_gex(duckdb_conn)
    # print duckdb records
    test = duckdb_conn.sql("SELECT * FROM option_chains_processed").to_df()
    print(test)
    duckdb_conn.close()
       
if __name__ == "__main__":
    main()