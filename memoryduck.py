import duckdb
import pandas as pd
import datetime
import os
import json
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
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
# Retry on connection errors or timeouts
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, max=10),
    retry=retry_if_exception_type((requests.exceptions.ConnectionError, requests.exceptions.Timeout))
)
def fetch_cboe_json(url):
    print(f"Fetching data from {url}")
    response = requests.get(url, timeout=10)
    response.raise_for_status()  # Raise exception for HTTP errors
    return json.dumps(response.json())

def get_options_chain_data(ticker: str = None, days: int = None) -> pd.DataFrame:
    # Get the singleton DuckDB connection
    duckdb_conn = get_duckdb_connection()

    # Base query
    query = """
        SELECT * FROM option_db
        WHERE 1=1
    """

    # Add ticker filter if provided
    if ticker:
        query += f" AND symbol = '{ticker}'"

    # Add time filter if days is provided
    if days:
        query += f" AND fetchTime >= CURRENT_DATE - INTERVAL '{days} days'"

    # Finalize query
    query += " ORDER BY fetchTime DESC;"

    # Execute the query and fetch the result as a DataFrame
    df = duckdb_conn.execute(query).fetchdf()

    return df

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
    FROM option_db
    WHERE open_interest >= 100
    GROUP BY dte, strike
    ORDER BY dte, strike;
    """
    # Execute the query and fetch the result as a DataFrame
    df = duckdb_conn.execute(query).fetchdf()
    
    # Print the DataFrame for debugging purposes
    # print(df)
    
    # Return the DataFrame
    return df

def update_database_duckdb():
    """
    Updates the DuckDB database with option chains data.
    
    """
    create_gex_delta_table_from_api()

def load_option_db():
    """
    Loads the Delta table from S3 into DuckDB.
    Returns the connection or raises an error if loading fails.
    """
    duckdb_conn = get_duckdb_connection()
    table_path = 's3://lbr-files/GEX/options_table'

    query = r'''
    CREATE OR REPLACE TABLE option_db AS
    SELECT * FROM delta_scan(?);
    '''
    try:
        # Attempt to load the Delta table
        duckdb_conn.execute(query, [table_path])
        print("Successfully loaded Delta table into DuckDB")
        duckdb_conn.execute("CREATE INDEX option_idx ON option_db (fetchTime, expiration_date, strike);")
    except Exception as e:
        print(f"Failed to load Option Delta table from {table_path}: {e}")
        # Optionally re-raise or handle accordingly
        # raise
    
    # Print sample from the table
    sample_df = duckdb_conn.execute("SELECT * FROM option_db USING SAMPLE 10 ROWS ORDER BY expiration_date;").fetchdf()
    print("Sample from the Option_db table:")
    print(sample_df)
    
    return duckdb_conn

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

def create_gex_delta_table_from_api():
    """
    Processes CBOE Option data and appends only new records to a Delta table in S3.
    Uses DuckDB filtering to avoid loading full datasets into memory.
    """
    duckdb_conn = get_duckdb_connection()

    # Step 1: Fetch raw JSON from CBOE manually
    url = 'https://cdn.cboe.com/api/global/delayed_quotes/options/_SPX.json'

    query = r'''
    WITH src AS (SELECT * FROM read_json(?))
    SELECT
      fetchTime,
      symbol,
      spotPrice,
      u.*,
      STRPTIME(regexp_extract(u.option, '(\d{6})(?:P|C)',1), '%y%m%d') AS expiration_date,
      TRY_CAST(regexp_extract(u.option, '[PC](\d+)', 1) AS integer) / 1000 AS strike,
      regexp_extract(u.option, '\d{6}([PC])', 1) AS right,
      DATEDIFF('day', CURRENT_DATE, STRPTIME(regexp_extract(u.option, '(\d{6})[PC]', 1), '%y%m%d')) AS dte,
      (CASE WHEN "right" = 'C' THEN u."gamma" * u.open_interest * 100 * spotPrice * spotPrice * 0.01 ELSE 0 END) AS CallGEX,
      (CASE WHEN "right" = 'P' THEN u."gamma" * u.open_interest * 100 * spotPrice * spotPrice * 0.01 * -1 ELSE 0 END) AS PutGEX
    FROM ( SELECT symbol, "timestamp" AS fetchTime, data.current_price AS spotPrice, UNNEST(data.options) AS u FROM src);
    '''
    
    try:
        # json_data = fetch_cboe_json(url)
        transformed = duckdb_conn.execute(query, [url]).fetchdf()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch JSON from {url}: {e}")
        return

    # Step 2: Register as DuckDB table for comparison
    duckdb_conn.register("new_data", transformed)

    # Step 3: Define Delta Table Path
    table_path = 's3a://lbr-files/GEX/options_table'
    storage_options = {
        'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
        'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY,
        'AWS_ENDPOINT_URL': 'https://s3.eu-central-1.wasabisys.com',
    }

    try:
        # Try reading existing Delta table schema
        existing_df = duckdb_conn.sql(f"SELECT * FROM delta_scan('{table_path.replace('s3a://', 's3://')}') LIMIT 0").df()
        existing_columns = set(existing_df.columns)
        new_columns = set(transformed.columns)

        if not new_columns.issubset(existing_columns):
            raise ValueError("Schema mismatch between source and Delta table")

        # Step 4: Use DuckDB to filter out existing records
        duckdb_conn.execute(f"""
            CREATE OR REPLACE TABLE new_records AS
            SELECT nd.*
            FROM new_data nd
            WHERE NOT EXISTS (
                SELECT 1
                FROM delta_scan('{table_path.replace('s3a://', 's3://')}') t
                WHERE t.spotPrice = nd.spotPrice AND t.symbol = nd.symbol
            )
        """)

        # Step 5: Fetch filtered DataFrame and write to Delta Lake
        new_records_df = duckdb_conn.execute("SELECT * FROM new_records").fetchdf()
        if not new_records_df.empty:
            write_deltalake(
                table_path,
                new_records_df,
                partition_by=['symbol'],
                mode="append",
                storage_options=storage_options
            )
            print(f"Appended {len(new_records_df)} new records to Delta table at {table_path}")
        else:
            print("No new records to append.")

    except Exception as e:
        # If Delta table doesn't exist yet, create it
        print("Creating new Delta table...")
        write_deltalake(
            table_path,
            transformed,
            partition_by=['symbol'],
            mode="overwrite",
            storage_options=storage_options
        )
        print(f"Created new Delta table at {table_path}")
    
    # Step 4: Update in-memory DuckDB `option_db` table

    # Step 6: Update in-memory DuckDB `option_db` table
    try:
        # Try to load existing option_db table
        duckdb_conn.execute("SELECT * FROM option_db LIMIT 1")
        print("Existing 'option_db' table found. Appending new records.")
        
        # Append only new records to existing table
        if not new_records_df.empty:
            duckdb_conn.register("new_records_df", new_records_df)
            duckdb_conn.execute("INSERT INTO option_db SELECT * FROM new_records_df")
            print(f"Updated 'option_db' with {len(new_records_df)} new records.")
    except Exception:
        # If table doesn't exist, create it with all current data
        print("Creating new 'option_db' table.")
        duckdb_conn.register("all_data", transformed)
        duckdb_conn.execute("CREATE TABLE option_db AS SELECT * FROM all_data")
        print(f"'option_db' created with {len(transformed)} initial records.")
    
    return

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
    # load_raschke_db()
    # update_raschke_from_s3()
    # df = read_last_record_from_raschke()
    # print(df)
    load_option_db()
    calculate_gex_levels_df()
    # create_gex_delta_table_from_api()
    
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
    # 
    duckdb_conn.close()
       
if __name__ == "__main__":
    main()