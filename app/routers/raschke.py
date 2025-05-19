from fastapi import APIRouter, BackgroundTasks, Response, status

from app.db_routines.duckdb_routines import (
    read_last_record_from_raschke,
    update_raschke_from_s3,
)

router = APIRouter( prefix="/raschke",
    tags=["raschke"],
    responses={404: {"Error": "Not found"}},
    )

@router.get("/", tags=["raschke"])
async def get_raschke_last():
    """
    Fetch Raschke last table for FuturesG.
    """
    panda_raschke_table = read_last_record_from_raschke()
    # Convert the DataFrame to a dictionary and return it
    return panda_raschke_table.to_dict()
@router.post("/update", tags=["raschke"])
async def update_raschke_db(background_tasks: BackgroundTasks):
    background_tasks.add_task(update_raschke_from_s3)
    return Response(status_code=status.HTTP_202_ACCEPTED)