from fastapi import FastAPI, status, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import Response
from dotenv import load_dotenv
import os


# Load endpoint functions
from database import (
    get_execution_id,
    get_ohlc_data,
    get_gex_levels_data,
    get_gex_profile_data,
    get_zero_gamma_data,
    get_quote_info_from_mongo,
    store_raw_option_chains,
    store_execution_details_sql,
    store_gamma_profile,
    store_total_gamma,
    update_database
)
from memoryduck import (
    update_database_duckdb,
    update_raschke_from_s3,
    load_raschke_db,
    load_option_db,
    read_last_record_from_raschke,
    calculate_gex_levels_df
)

# Get env variables
load_dotenv()
# load_rasche db in memrory from deltatable
load_raschke_db()
#  Load opyton db in memrory from deltatable
load_option_db()

app = FastAPI(
    title="Gamma Exposure app",
    summary="Retrieves SPX option data and calculates the gamma exposure of the options",
)
# Add GZip middleware
app.add_middleware(
    GZipMiddleware,
    minimum_size=1024,
    compresslevel=8
)
# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:4173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# API endpoints
# ==========================
@app.api_route(
    "/",
    methods=["GET", "HEAD"],
    response_description="Welcome message",
    status_code=status.HTTP_200_OK,
)
async def hello_world():
    return {"message": "hello World"}

@app.api_route(
    "/execution_info",
    methods=["GET", "HEAD"],
    response_description="Get information about all executions",
    status_code=status.HTTP_200_OK,
)
async def execution_info():
    df_id = get_execution_id()
    df_id['execution_timestamp'] = df_id['execution_timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    df_id['delayed_timestamp'] = df_id['delayed_timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    return df_id.to_dict('list')

@app.api_route(
    "/zero_gamma",
    methods=["GET", "HEAD"],
    response_description="Get gex profile data",
    status_code=status.HTTP_200_OK,
)
async def zero_gamma_data():
    dict_zero_gamma = get_zero_gamma_data()   
    return dict_zero_gamma

@app.api_route( 
    "/gex_profile",
    methods=["GET", "HEAD"],
    response_description="Get gex profile data",
    status_code=status.HTTP_200_OK,
)
async def gex_profile_data():
    dict_gex_profile = get_gex_profile_data()
    return dict_gex_profile

@app.api_route( 
    "/gex_levels",
    methods=["GET", "HEAD"],
    response_description="Get gex levels data",
    status_code=status.HTTP_200_OK,
)
async def gex_levels_data():
    dict_gex_levels = get_gex_levels_data()        
    return dict_gex_levels
@app.api_route( 
    "/gex_levels_duck",
    methods=["GET", "HEAD"],
    response_description="Get gex levels data from DuckDB with optional DTE filtering",
    status_code=status.HTTP_200_OK,
)
async def gex_levels_data_duck(    
    dte_min: int = Query(None, description="Filter records with DTE greater than or equal to this value"),
    dte_max: int = Query(None, description="Filter records with DTE less than or equal to this value")
    ):
    """
    Fetch GEX levels data from DuckDB using the global connection.
    """
    # Call the function to fetch GEX levels data
    panda_gex_levels = calculate_gex_levels_df()
    
    # Apply DTE filters if both min and max are provided
    if dte_min is not None and dte_max is not None:
        panda_gex_levels = panda_gex_levels[
            (panda_gex_levels['dte'] >= dte_min) & 
            (panda_gex_levels['dte'] <= dte_max)
        ]
    
    # Convert the DataFrame to a dictionary and return it
    return panda_gex_levels.to_dict(orient="list")

@app.api_route( 
    "/raschke",
    methods=["GET", "HEAD"],
    response_description="Get Raschke Table for Futurues",
    status_code=status.HTTP_200_OK,
)
async def get_raschke_last():
    """
    Fetch Raschke last table for FuturesG.
    """
    panda_raschke_table = read_last_record_from_raschke()
    # Convert the DataFrame to a dictionary and return it
    return panda_raschke_table.to_dict()
@app.post(
    "/raschke/update",
    response_description="Update Raschke Table for Futures",
    status_code=status.HTTP_202_ACCEPTED,
)
async def update_raschke_db(background_tasks: BackgroundTasks):
    background_tasks.add_task(update_raschke_from_s3)
    return Response(status_code=status.HTTP_202_ACCEPTED)

@app.api_route(
    "/ohlc_data",
    methods=["GET", "HEAD"],
    response_description="Get ohlc data",
    status_code=status.HTTP_200_OK,
)
async def ohlc_data():
    ohlc = get_ohlc_data()
    ohlc = ohlc.reset_index().drop(columns={'Dividends', 'Stock Splits'})   
    return ohlc.to_dict('list')

@app.post(
    "/update_db",
    response_description="Fetch new raw data, transform and update values in database",
    status_code=status.HTTP_202_ACCEPTED,
)
async def update_db(background_tasks: BackgroundTasks):
    background_tasks.add_task(update_database)
    return Response(status_code=status.HTTP_202_ACCEPTED)

@app.post(
    "/update_db_duck",
    response_description="Fetch new raw data, transform and update values in database",
    status_code=status.HTTP_202_ACCEPTED,
)
async def update_db_duck(background_tasks: BackgroundTasks):
    background_tasks.add_task(update_database_duckdb)
    return Response(status_code=status.HTTP_202_ACCEPTED)

@app.post(
    "/initialize",
    response_description="Drop SQL tables and MongoDB collections and re-create them from scratch",
    status_code=status.HTTP_201_CREATED,
)
async def initialize(pwd: str):
    """
    Drop all tables and collections and re-create them from scratch
    """
    # Check if pwd is equal to .env value
    if pwd == os.environ.get('INIT_CRED'):
        # init_result = init_db_from_scratch()
        init_result = 1
        
        if init_result.status==1:
            return Response(status_code=status.HTTP_204_NO_CONTENT)
        
        raise HTTPException(status_code=404, detail=f"Unable to perform initialization")
    else:
        raise HTTPException(status_code=404, detail=f"Credentials not valid")
    
