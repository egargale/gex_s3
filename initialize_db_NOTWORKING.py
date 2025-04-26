import sys
from database import init_db_from_scratch

def main():
    print("Starting database initialization...")
    try:
        # Call the initialization function
        result = init_db_from_scratch()
        
        if result.get("status") == 1:
            print("Database initialized successfully.")
            sys.exit(0)  # Success
        else:
            print("Database initialization failed.")
            sys.exit(1)  # Failure
    except Exception as e:
        print(f"Error during database initialization: {e}")
        sys.exit(1)  # Failure

if __name__ == "__main__":
    main()