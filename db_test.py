# db_test.py
"""
This script initializes the database by creating all tables defined in the SQLAlchemy models.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from data_models import Base  # Import Base from data_models.py

# Load environment variables (if needed)
import os
from dotenv import load_dotenv

load_dotenv()

# Database connection settings
PG_USER_NAME = os.environ.get('PG_USER_NAME', 'Unable to retrieve PG_USER_NAME')
PG_USER_PWD = os.environ.get('PG_USER_PWD', 'Unable to retrieve PG_USER_PWD')
PG_REGION = os.environ.get('PG_REGION', 'Unable to retrieve PG_REGION')
PG_URL_PORT = os.environ.get('PG_URL_PORT', 'Unable to retrieve PG_URL_PORT')

# Create the database engine
DATABASE_URL = f"postgresql://{PG_USER_NAME}:{PG_USER_PWD}@{PG_REGION}:{PG_URL_PORT}/postgres"
engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=15)

# Create a session to interact with the database
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Initialize the database
def initialize_database():
    """
    Create all tables defined in the SQLAlchemy models.
    """
    print("Initializing database...")
    try:
        # Create all tables
        Base.metadata.create_all(bind=engine)
        print("Database tables created successfully.")
    except Exception as e:
        print(f"Error initializing database: {e}")
        raise

if __name__ == "__main__":
    # Call the initialization function
    initialize_database()
    