from fastapi import APIRouter, BackgroundTasks, Query, Response, status

from app.db_routines.duckdb_routines import (
    calculate_gex_levels_df,
    get_options_chain_data,
    update_database_duckdb,
)

router = APIRouter(
    prefix="/gex",
    tags=["gex"],
    responses={404: {"Error": "Not found"}},
)

@router.get("/gex_levels", tags=["gex"])
async def gex_levels(    
    dte_min: int = Query(None, description="Filter records with DTE greater than or equal to this value"),
    dte_max: int = Query(None, description="Filter records with DTE less than or equal to this value"),
    ticker: str = Query("_SPX", description="Ticker symbol to filter data, default is '_SPX'")
    ):
    """
    Fetch GEX levels data from DuckDB using the global connection.
    """
     # Call the function to fetch GEX levels data with the specified ticker
    panda_gex_levels = calculate_gex_levels_df(ticker=ticker)
    
    # Apply DTE filters if both min and max are provided
    if dte_min is not None and dte_max is not None:
        panda_gex_levels = panda_gex_levels[
            (panda_gex_levels['dte'] >= dte_min) & 
            (panda_gex_levels['dte'] <= dte_max)
        ]
    
    # Convert the DataFrame to a dictionary and return it
    return panda_gex_levels.to_dict(orient="list")

@router.get("/options_chain", tags=["gex"])
async def options_chain(
    ticker: str = Query(None, description="Filter by ticker symbol"),
    days: int = Query(1, description="Number of days to look back (default: 1)")
):
    """
    Retrieve filtered options chain data by ticker and time range.
    """
    df = get_options_chain_data(ticker=ticker, days=days)
    return df.to_dict(orient="records")

@router.post("/update", tags=["gex"])
async def update_db_duck(background_tasks: BackgroundTasks, ticker: str = "_SPX"):
    background_tasks.add_task(update_database_duckdb, ticker=ticker)
    return Response(status_code=status.HTTP_202_ACCEPTED)