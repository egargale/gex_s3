from fastapi import  FastAPI
from .routers import raschke, duck, old
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from app.db_routines.duckdb_routines import main as duckdb_load
from app.db_routines.config import get_duckdb_connection
import time

#wait for db to be ready
start_time = time.time()
timeout = 30  # seconds

while True:
    try:
        conn = get_duckdb_connection()
        conn.execute("PRAGMA show_tables").fetchdf()
        print("DuckDB ready")
        break
    except Exception as e:
        print(f"Error connecting to DuckDB: {e}")
        if time.time() - start_time > timeout:
            print("Timeout reached. Exiting program.")
            exit(1)
        time.sleep(5)
# popolate db with tables
duckdb_load()

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
    
app.include_router(duck.router)
app.include_router(raschke.router)
app.include_router(old.router)

@app.get("/")
async def root():
    return {"message": "Hello GEX Application!"}

@app.get("/health")
async def health_check():
    conn = get_duckdb_connection()
    try:
        count = conn.execute("PRAGMA show_tables").fetchdf()
        return {"status": "healthy", "tables": count.values.tolist()}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
