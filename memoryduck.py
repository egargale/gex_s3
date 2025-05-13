import duckdb
import pandas as pd
import os
from dotenv import load_dotenv
from cboe_data import get_ticker_info
from config import CONFIG
from config import get_duckdb_connection
from deltalake import write_deltalake, DeltaTable

# Get env variables
load_dotenv()

# Create connection for AWS S3
# ================================
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', 'AWS_ACCESS_KEY_ID is missing')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', 'AWS_SECRET_ACCESS_KEY is missing')
S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL', 'S3_ENDPOINT_URL is missing')
S3_BUCKET = os.environ.get('S3_BUCKET', 'S3_BUCKET is missing')
DELTA_TABLE = os.environ.get('DELTA_TABLE', 'DELTA_TABLE is missing')
SIERRA_FILE_PATH  = os.environ.get('SIERRA_FILE_PATH', 'SIERRA_FILE_PATH is missing')

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
    print(f"History Records loaded: {duckdb_conn.execute("SELECT COUNT(*) FROM db_chains").fetchone()[0]}")
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
    print(f"Records recalculated: {duckdb_conn.execute('SELECT COUNT(*) FROM db_chains_test').fetchone()[0]}")
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
    records_written = int(df.shape[0])
    print(f"Written {records_written} records to {file_path}")
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

def update_database_duckdb():
    """
    Updates the DuckDB database with option chains data.
    
    """
    print("Getting past records from S3 parquet files")
    load_db()
    print("Getting last data from CBOE")
    store_option_chains()
    print("Calculating GEX data and updating S3 parquet file")
    updated_option_chains_gex()
    print("Calculating GEX levels and writing to S3 DeltaTable")
    write_gex_levels_to_deltatable(DELTA_TABLE)

def load_raschke_db():
    """
    Loads the Delta table Raschke from S3 into DuckDB.
    Returns the connection or raises an error if loading fails.
    """
    duckdb_conn = get_duckdb_connection()
    table_path = 's3://lbr-files/GEX/raschke_table'

    query = r'''
    CREATE OR REPLACE TABLE raschke AS
        SELECT * FROM delta_scan(?);
    '''
    try:
        # Attempt to load the Delta table
        duckdb_conn.execute(query, [table_path])
        print("Successfully loaded Raschke Delta table into DuckDB")
    except Exception as e:
        print(f"Failed to load Delta table from {table_path}: {e}")
        # Optionally re-raise or handle accordingly
        # raise
    return duckdb_conn
def create_delta_table_from_csv_history():
    """
    Reads 200 days of historical data from S3 CSVs,
    stores it in DuckDB, and creates (or overwrites)
    a Delta table with the result.
    """
    duckdb_conn = get_duckdb_connection()
    sierra_files_path = os.environ.get('SIERRA_FILES_PATH', 's3://lbr-files/LBR')
    csv_path = f"s3://{sierra_files_path}/*.csv"
    table_path = 's3a://lbr-files/GEX/raschke_table'
    
    storage_options = {
        'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
        'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY,
        'AWS_ENDPOINT_URL': 'https://s3.eu-central-1.wasabisys.com',
    }

    # SQL query to load and deduplicate CSV data
    query = r'''
    CREATE OR REPLACE TABLE raschke AS
    WITH RankedData AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY filename ORDER BY Date DESC) AS rn
        FROM read_csv(?, union_by_name=true, filename = true)
    )
    SELECT 
        regexp_extract(filename, '([^\/]+)\.csv', 1) AS filename,
        Date, Open, High, Low, Last,
        IB, WR7, NR7, TP, "3BT", "Pure 3BT", BO, HBO, LBO, PF3, "PF3[1]",
        "5SMA", ERun, STPvt, SIG, LongoBO, ShortBO, ROC, "ROC[1]",
        STPvt_1, "Highest ROC", "Lowest ROC", RANK, ADX
    FROM RankedData
    WHERE rn <= 200;
    '''

    # Step 1: Execute transformation and store in DuckDB
    duckdb_conn.execute(query, [csv_path])
    print("Loaded and stored 200-day history in DuckDB")

    # Step 2: Export to Arrow
    df_arrow = duckdb_conn.execute("SELECT * FROM raschke").fetch_arrow_table()

    # Step 3: Write to Delta Lake (Overwrite mode)
    write_deltalake(
        table_path,
        df_arrow,
        mode="overwrite",
        storage_options=storage_options
    )

    print(f"Successfully created Delta table at {table_path}")
def update_raschke_from_s3():
    """
    Loads CSV files from S3, transforms them using DuckDB,
    and upserts the result into a Delta table.
    """
    duckdb_conn = get_duckdb_connection()
    sierra_files_path = os.environ.get('SIERRA_FILES_PATH', 's3://lbr-files/LBR')
    csv_path = f"s3://{sierra_files_path}/*.csv"
    
    # SQL query to transform data
    query = r'''
    WITH RankedData AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY filename ORDER BY Date DESC) AS rn
        FROM read_csv(?, union_by_name=true, filename = true)
    )
    SELECT 
        regexp_extract(filename, '([^\/]+)\.csv', 1) AS filename,
        Date, Open, High, Low, Last,
        IB, WR7, NR7, TP, "3BT", "Pure 3BT", BO, HBO, LBO, PF3, "PF3[1]",
        "5SMA", ERun, STPvt, SIG, LongoBO, ShortBO, ROC, "ROC[1]",
        STPvt_1, "Highest ROC", "Lowest ROC", RANK, ADX
    FROM RankedData
    WHERE rn = 1;
    '''

    # Step 1: Transform data with DuckDB and fetch as Arrow Table
    raschketable = duckdb_conn.execute(query, [csv_path]).fetch_arrow_table()
    # print("Schema of source Arrow table:")
    # print(raschketable.schema)

    # Step 2: Define or open DeltaTable
    table_path = 's3a://lbr-files/GEX/raschke_table'
    storage_options = {
        'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
        'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY,
        'AWS_ENDPOINT_URL': 'https://s3.eu-central-1.wasabisys.com',
    }

    try:
        # Try opening existing Delta table
        dt = DeltaTable(table_path, storage_options=storage_options)
    except Exception:
        # If not exists, create with function
        print("Delta table does not exist. Creating...")
        create_delta_table_from_csv_history()
        return

    # Step 3: Perform UPSERT (Merge)
    (
        dt.merge(
            source=raschketable,
            predicate="target.filename = source.filename AND target.Date = source.Date",
            source_alias="source",
            target_alias="target"
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    print(f"Successfully merged data into Delta table at {table_path}")
    
    # Step 4: Update in-memory DuckDB `raschke` table
    # Convert Arrow Table to Pandas (DuckDB can work with Pandas easily)
    df_new = raschketable.to_pandas()

    # Register new data in DuckDB
    duckdb_conn.register("df_new", df_new)

    # Delete matched rows in DuckDB
    duckdb_conn.execute("""
        DELETE FROM raschke
        WHERE EXISTS (
            SELECT 1
            FROM df_new
            WHERE raschke.filename = df_new.filename AND raschke.Date = df_new.Date
        )
    """)

    # Insert all new/upserted rows
    duckdb_conn.execute("INSERT INTO raschke SELECT * FROM df_new")

    print("In-memory DuckDB `raschke` table updated")
    return
def read_last_record_from_raschke() -> pd.DataFrame:
    """
    Reads the last record from the raschke table.
    
    Returns:
        pd.DataFrame: The last record from the raschke table.
    """
    # Get the singleton DuckDB connection
    duckdb_conn = get_duckdb_connection()
    # Query the last record from the raschke table
    query = r'''
    SELECT * FROM raschke WHERE Date = (SELECT MAX(Date) FROM raschke);
    '''
    df = duckdb_conn.execute(query).fetch_df()
    return df
def main():
    # Get the singleton DuckDB connection
    duckdb_conn = get_duckdb_connection()
    # update_database_duckdb()
    # print duckdb records
    # test_read_parquet = duckdb_conn.sql("SELECT * FROM option_chains_processed").to_df()
    # print(test_read_parquet)
    # print(DELTA_TABLE)
    # delta_table_url = 's3://' + DELTA_TABLE
    # test_read_deltatable = duckdb_conn.sql(f"SELECT * FROM delta_scan('{delta_table_url}')").to_df()
    # print(test_read_deltatable)
    # df = get_gex_levels_from_deltatable()
    # print(df)
    load_raschke_db()
    update_raschke_from_s3()
    df = read_last_record_from_raschke()
    print(df)
    
    duckdb_conn.close()
       
if __name__ == "__main__":
    main()