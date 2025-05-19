from fastapi import APIRouter, BackgroundTasks, Response, status

from app.db_routines.database import (
   get_execution_id,
   get_zero_gamma_data,
   get_gex_profile_data,
   get_gex_levels_data,
   get_ohlc_data,
   update_database
)

router = APIRouter(
    prefix="/old",
    tags=["old"],
    responses={404: {"Error": "Not found"}},
)

@router.get("/execution_info", tags=["old"])
async def execution_info():
    df_id = get_execution_id()
    df_id['execution_timestamp'] = df_id['execution_timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    df_id['delayed_timestamp'] = df_id['delayed_timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    return df_id.to_dict('list')

@router.get("/zero_gamma", tags=["old"])
async def zero_gamma_data():
    dict_zero_gamma = get_zero_gamma_data()   
    return dict_zero_gamma

@router.get("/gex_profile", tags=["old"])
async def gex_profile_data():
    dict_gex_profile = get_gex_profile_data()
    return dict_gex_profile

@router.get("/gex_levels", tags=["old"])
async def gex_levels_data():
    dict_gex_levels = get_gex_levels_data()        
    return dict_gex_levels
@router.get("/ohlc_data", tags=["old"])
async def ohlc_data():
    ohlc = get_ohlc_data()
    ohlc = ohlc.reset_index().drop(columns={'Dividends', 'Stock Splits'})   
    return ohlc.to_dict('list')

@router.post("/update_db", tags=["old"])
async def update_db(background_tasks: BackgroundTasks):
    background_tasks.add_task(update_database)
    return Response(status_code=status.HTTP_202_ACCEPTED)
