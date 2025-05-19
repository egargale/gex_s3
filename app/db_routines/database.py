"""Functions to connect and retrieve data from databases"""

import os 
from io import BytesIO, TextIOWrapper
import gzip
import datetime
from dotenv import load_dotenv
import pandas as pd
import yfinance as yf

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient
from bson.objectid import ObjectId
import boto3

from .cboe_data import get_quotes, get_ticker_info
from .gamma_exposure import calculate_gamma_profile, calculate_spot_total_gamma_call_puts
from .config import CONFIG

# Get env variables
load_dotenv()

# Create engine for PostgreSQL
# ============================
PG_USER_NAME = os.environ.get('PG_USER_NAME','Unable to retrieve PG_USER_NAME')
PG_USER_PWD = os.environ.get('PG_USER_PWD','Unable to retrieve PG_USER_PWD')
PG_REF_ID = os.environ.get('PG_REF_ID','Unable to retrieve PG_REF_ID')
PG_REGION = os.environ.get('PG_REGION','Unable to retrieve PG_REGION')
PG_URL_PORT = os.environ.get('PG_URL_PORT','Unable to retrieve PG_URL_PORT')

# pool_connection = f"postgresql://{PG_USER_NAME}.{PG_REF_ID}:{PG_USER_PWD}@{PG_REGION}:{PG_URL_PORT}/postgres"
pool_connection = f"postgresql://{PG_USER_NAME}:{PG_USER_PWD}@{PG_REGION}:{PG_URL_PORT}/postgres"
engine = create_engine(pool_connection,pool_pre_ping=True, pool_size=15)
engine.connect()

Base = declarative_base()

# Create a session to interact with the database
SessionLocal = sessionmaker(autocommit=False,autoflush=False,bind=engine)
session = SessionLocal()


# Create connection for MongoDB
# =============================
MONGO_USER = os.environ.get('MONGO_USER','Unable to retrieve MONGO_USER')
MONGO_PWD = os.environ.get('MONGO_PWD','Unable to retrieve MONGO_PWD')
MONGO_DB_URL = os.environ.get('MONGO_DB_URL','Unable to retrieve MONGO_DB_URL')
MONGO_DB_NAME = os.environ.get('MONGO_DB_NAME','Unable to retrieve MONGO_DB_NAME')

mongo_url = f"mongodb+srv://{MONGO_USER}:{MONGO_PWD}@{MONGO_DB_URL}/?retryWrites=true&w=majority"

# Create connection for AWS booto3
# ================================
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', 'AWS_ACCESS_KEY_ID is missing')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', 'AWS_SECRET_ACCESS_KEY is missing')
S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL', 'S3_ENDPOINT_URL is missing')
S3_BUCKET = os.environ.get('S3_BUCKET', 'S3_BUCKET is missing')

# Initialize S3 client
s3 = boto3.client(
    's3',
    endpoint_url=f"https://{S3_ENDPOINT_URL}",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def store_raw_option_chains() -> dict:
    """
    Retrieve (delayed) option chains from CBOE and store them
    """
    
    # Fetch data from CBOE
    option_chain = get_quotes(symbol=CONFIG['CBOE_TICKER'])
    option_chain['expiration'] = pd.to_datetime(option_chain['expiration'], format='%Y-%m-%d')
    # Get timestamp when query was performed
    query_timestamp = datetime.datetime.now(datetime.timezone.utc).astimezone()
    
    # Get additional ticker info
    option_chain_ticker_info = get_ticker_info(symbol=CONFIG['CBOE_TICKER'])[0]
    delayed_timestamp = option_chain_ticker_info.loc['lastTradeTimestamp',:].item()
    
    # Store compressed .tag.gz file in object storage
    
    with BytesIO() as buf:
        with gzip.GzipFile(fileobj=buf, mode='w') as gz_file:
            # Write the option chain DataFrame to the gzip file
            option_chain.to_csv(TextIOWrapper(gz_file, 'utf8'), index=False, header=True)

        # Upload file to AWS S3
        try:
            s3_key = f"cboe_opt_chain_timestamp_{query_timestamp.strftime('%Y-%m-%dT%H:%M:%S%z')}_delayed_{delayed_timestamp}.tar.gz"
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=buf.getvalue()
            )
            print(f"File uploaded to S3: {s3_key}")

            # Store the S3 object URL in the response
            response_compressed = {
                'secure_url': f"s3://{S3_BUCKET}/{s3_key}",
                'query_timestamp': query_timestamp.strftime("%Y-%m-%dT%H:%M:%S%z"),
                'delayed_timestamp': delayed_timestamp
            }

        except Exception as e:
            print(f"Error uploading file to S3: {e}")
            raise
        
    # Append additional info
    response_compressed['query_timestamp'] = query_timestamp.strftime("%Y-%m-%dT%H:%M:%S%z")
    response_compressed['delayed_timestamp'] = delayed_timestamp

    # Create MongoDB document with upload information
    with MongoClient(mongo_url) as mongodb_client:
        mongo_database = mongodb_client[MONGO_DB_NAME]
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_UPLOADS']]
        
        # Insert execution details in mongo
        upload_id = mongo_collection.insert_one(response_compressed).inserted_id
        
        # Add post_id info to dict
        response_compressed['mongodb_upload_id'] = str(upload_id)
        
        # Insert quote info in mongo
        dict_option_chain_ticker_info = option_chain_ticker_info.to_dict()[CONFIG['CBOE_TICKER']]
        dict_option_chain_ticker_info['ticker'] = CONFIG['CBOE_TICKER']
        dict_option_chain_ticker_info['mongodb_upload_id'] = response_compressed['mongodb_upload_id']
        
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_QUOTE_INFO']]
        info_id = mongo_collection.insert_one(dict_option_chain_ticker_info).inserted_id
        

    # Return dict with upload id
    return response_compressed

def store_execution_details_sql(response_compressed:dict) -> int:
    from .data_models import IdTable
    # Create new instance of IdTable
    id_table = IdTable(
        execution_timestamp=pd.to_datetime(response_compressed['query_timestamp']),
        delayed_timestamp=pd.to_datetime(response_compressed['delayed_timestamp']),
        raw_data=response_compressed['secure_url'],
        mongodb_id=str(response_compressed['_id']),
    )
    
    # Write to sql
    session.add(id_table)
    session.commit()
    
    # Fetch id of new record
    result = session.query(IdTable).filter(IdTable.mongodb_id == str(response_compressed['_id']))
    df_id = pd.read_sql(result.statement,session.bind)
    
    id_sql = df_id['id'].item()
    
    return id_sql
        
    
def store_gamma_profile(secure_url:str, spot_price:float, last_trade_date:pd.Timestamp, mongodb_upload_id:str):
    
    # Retrieve option chain from url
    option_chain_long = get_df_from_storage(secure_url=secure_url)
    option_chain_long['expiration'] = pd.to_datetime(option_chain_long['expiration'])
    
    print(f" - GAMMA PROFILE: option chain retrieved (shape {option_chain_long.shape})")
    
    gex_profile, zero_gamma = calculate_gamma_profile(
        option_chain_long=option_chain_long, 
        spot_price=spot_price, 
        last_trade_date=last_trade_date,
        pct_from=0.8, 
        pct_to=1.2)
    
    print(f" - GAMMA PROFILE: gex_profile obtained (shape {gex_profile.shape})")
    
    df_zero_gamma = pd.DataFrame({'Zero Gamma':[zero_gamma]}, index=[last_trade_date])
    
    # Store it in mongodb
    mongo_doc = {
        mongodb_upload_id:gex_profile.reset_index().to_dict('list')
    }
    
    mongo_doc_zero_gamma = {
        mongodb_upload_id:df_zero_gamma.reset_index().to_dict('list')
    }
    # Fix date format
    mongo_doc_zero_gamma[mongodb_upload_id]['index'] = [last_trade_date]
    
    with MongoClient(mongo_url) as mongodb_client:
        mongo_database = mongodb_client[MONGO_DB_NAME]
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_GEX_PROFILE']]
        
        # Insert execution details in mongo
        upload_id = mongo_collection.insert_one(mongo_doc).inserted_id
        
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_GEX_ZERO_GAMMA']]
        
        # Insert execution details in mongo
        upload_id_zero = mongo_collection.insert_one(mongo_doc_zero_gamma).inserted_id
    
    return upload_id, upload_id_zero
    
    
def store_total_gamma(secure_url:str, spot_price:float, mongodb_upload_id:str):
    
    # Retrieve option chain from url
    option_chain_long = get_df_from_storage(secure_url=secure_url)
    option_chain_long['expiration'] = pd.to_datetime(option_chain_long['expiration'])
    
    gamma_strikes = calculate_spot_total_gamma_call_puts(
        option_chain_long=option_chain_long, 
        spot_price=spot_price)
    
    # Store it in mongodb
    mongo_doc = {
        mongodb_upload_id:gamma_strikes.reset_index().to_dict('list')
    }
    
    with MongoClient(mongo_url) as mongodb_client:
        mongo_database = mongodb_client[MONGO_DB_NAME]
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_GEX_STRIKES']]
        
        # Insert execution details in mongo
        upload_id = mongo_collection.insert_one(mongo_doc).inserted_id
        
    return upload_id  

def get_df_from_storage(secure_url: str) -> pd.DataFrame:
    """
    Retrieve a (compressed) dataframe from S3 object storage.

    Args:
        secure_url (str): S3 URL of the object stored (e.g., s3://bucket-name/key)

    Returns:
        pd.DataFrame: uncompressed dataframe from storage
    """
    try:
        # Parse the S3 URL to extract bucket and key
        if not secure_url.startswith("s3://"):
            raise ValueError("Invalid S3 URL format. Expected format: s3://bucket-name/key")

        bucket, key = secure_url.replace("s3://", "").split("/", 1)

        # Retrieve the object from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read()

        # Decompress the file content and load it into a DataFrame
        with gzip.open(BytesIO(file_content), 'rt') as gzip_file:
            df = pd.read_csv(gzip_file, sep=',', skiprows=0, index_col=None)

        return df

    except Exception as e:
        print(f"Error retrieving file from S3: {e}")
        raise
    
def get_quote_info_from_mongo(mongodb_upload_id:str) -> pd.DataFrame:
    """
    Retrieve quote information from object storage for a given execution

    Args:
        mongodb_upload_id (str): _id of the mongodb execution

    Returns:
        pd.DataFrame: quote information for that execution
    """
    with MongoClient(mongo_url) as mongodb_client:
        mongo_database = mongodb_client[MONGO_DB_NAME]
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_QUOTE_INFO']]
        
        dict_info = mongo_collection.find_one(filter={'mongodb_upload_id':str(mongodb_upload_id)},)
        
        df_info = pd.DataFrame({CONFIG['CBOE_TICKER']:dict_info})
        
    return df_info

def get_upload_info_from_mongo(mongodb_upload_id:str) -> pd.DataFrame:
    """
    Retrieve quote information from object storage for a given execution

    Args:
        mongodb_upload_id (str): _id of the mongodb execution

    Returns:
        pd.DataFrame: quote information for that execution
    """
    with MongoClient(mongo_url) as mongodb_client:
        mongo_database = mongodb_client[MONGO_DB_NAME]
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_UPLOADS']]
        
        dict_info = mongo_collection.find_one(filter={'_id':ObjectId(mongodb_upload_id)},)
        
    return dict_info
    
    
def get_execution_id() -> pd.DataFrame:
    from .data_models import IdTable
    result = session.query(IdTable)
    df_id = pd.read_sql(result.statement,session.bind)
    
    return df_id
    
    
def get_ohlc_data() -> pd.DataFrame:
    yfin_ticker = yf.Ticker(CONFIG['YFIN_TICKER'])
    
    # get historical market data
    yfin_hist = yfin_ticker.history(start='2021-01-01')
    # yfin_hist = yfin_ticker.history(period='1mo')
    
    return yfin_hist

def get_gex_levels_data() -> dict:
    dict_gex_levels = dict()
    with MongoClient(mongo_url) as mongodb_client:
        mongo_database = mongodb_client[MONGO_DB_NAME]
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_GEX_STRIKES']]
         # Define filter, sort, and limit
        filter = {}
        sort = [('_id', -1)]  # Sort by _id in descending order
        limit = 1

        # Query MongoDB with filter, sort, and limit
        cursor = mongo_collection.find(filter=filter).sort(sort).limit(limit)
        # cursor = mongo_collection.find({})
        for document in cursor:
            doc_keys = list(document.keys())
            dict_gex_levels[doc_keys[-1]] = document[doc_keys[-1]]
    
    return dict_gex_levels

def get_gex_profile_data() -> dict:
    dict_gex_profile = dict()
    with MongoClient(mongo_url) as mongodb_client:
        mongo_database = mongodb_client[MONGO_DB_NAME]
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_GEX_PROFILE']]
        # Define filter, sort, and limit
        filter = {}
        sort = [('_id', -1)]  # Sort by _id in descending order
        limit = 1

        # Query MongoDB with filter, sort, and limit
        cursor = mongo_collection.find(filter=filter).sort(sort).limit(limit)
        # old find
        # cursor = mongo_collection.find({})
        for document in cursor:
            doc_keys = list(document.keys())
            dict_gex_profile[doc_keys[-1]] = document[doc_keys[-1]]
    
    return dict_gex_profile

def get_zero_gamma_data() -> dict:
    dict_zero_gamma = dict()
    with MongoClient(mongo_url) as mongodb_client:
        mongo_database = mongodb_client[MONGO_DB_NAME]
        mongo_collection = mongo_database[CONFIG['MONGODB_COLECTION_GEX_ZERO_GAMMA']]
        # Define filter, sort, and limit
        filter = {}
        sort = [('_id', -1)]  # Sort by _id in descending order
        limit = 1
        # Query MongoDB with filter, sort, and limit
        cursor = mongo_collection.find(filter=filter).sort(sort).limit(limit)
        # cursor = mongo_collection.find({})
        for document in cursor:
            doc_keys = list(document.keys())
            dict_zero_gamma[doc_keys[-1]] = document[doc_keys[-1]]
    
    return dict_zero_gamma

def update_database():
    print('Fetch new option data from source')
    # Fetch new option data and store in S3
    response = store_raw_option_chains()
    
    print('Option data stored in S3')
    
    # Add new record to SQL with execution details
    id_sql = store_execution_details_sql(response_compressed=response)
    
    print('SQL record with execution details has been created')
    
    # Get quotes info from database
    quote_info = get_quote_info_from_mongo(mongodb_upload_id=str(response['_id']))
    spot_price = quote_info.iloc[:,0]['close']
    last_trade_date = pd.to_datetime(quote_info.iloc[:,0]['lastTradeTimestamp'])
    
    print(f'Last trade date obtained: {last_trade_date}')
    
    # Calculate gamma exposure and store in database
    upload_id_profile, upload_id_zero = store_gamma_profile(
        secure_url=response['secure_url'], 
        spot_price=spot_price, 
        last_trade_date=last_trade_date, 
        mongodb_upload_id=str(response['_id']))
    
    print('Gamma profile has been stored in db')
    
    upload_id_total_gamma = store_total_gamma(
        secure_url=response['secure_url'], 
        spot_price=spot_price, 
        mongodb_upload_id=str(response['_id']))
    
    print('Total gamma has been stored')
    
def init_db_from_scratch():
    """
    Drop and recreate all tables in PostgreSQL and drop all collections in MongoDB.
    
    Returns:
        dict: A dictionary with a 'status' key indicating success (1) or failure (0).
    """
    print("Initializing database from scratch...")
    
    try:
        # PostgreSQL: Drop and recreate tables
        print("Dropping and recreating PostgreSQL tables...")
        Base.metadata.drop_all(bind=engine)  # Drop all existing tables
        Base.metadata.create_all(bind=engine)  # Recreate tables
        print("PostgreSQL tables initialized successfully.")
        
        # MongoDB: Drop all collections
        print("Dropping all MongoDB collections...")
        with MongoClient(mongo_url) as mongodb_client:
            mongo_database = mongodb_client[MONGO_DB_NAME]
            for collection_name in mongo_database.list_collection_names():
                mongo_database.drop_collection(collection_name)
                print(f"Dropped MongoDB collection: {collection_name}")
        print("MongoDB collections dropped successfully.")
        
        # Return success status
        return {"status": 1}
    
    except Exception as e:
        # Log the error and return failure status
        print(f"Error during database initialization: {e}")
        return {"status": 0}

if __name__ == '__main__':
    update_database()