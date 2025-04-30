#!/usr/bin/env python3
"""
Simple database connection test - no dependencies except for config.py
"""
import os
import sys
import traceback

print("=== Simple Database Connection Test ===")
print(f"Python version: {sys.version}")
print(f"Current working directory: {os.getcwd()}")
print()

try:
    print("1. Trying to import config...")
    import config
    print("   Success! Found config.py")
    print(f"   DATABASE_URI: {config.DATABASE_URI}")

    # Check if psycopg2 is installed
    print("\n2. Checking for psycopg2...")
    try:
        import psycopg2
        print("   Success! psycopg2 is installed")
    except ImportError:
        print("   ERROR: psycopg2 is not installed. Run: pip install psycopg2-binary")
        sys.exit(1)

    # Check if SQLAlchemy is installed
    print("\n3. Checking for SQLAlchemy...")
    try:
        import sqlalchemy
        print(f"   Success! SQLAlchemy version {sqlalchemy.__version__} is installed")
    except ImportError:
        print("   ERROR: SQLAlchemy is not installed. Run: pip install sqlalchemy")
        sys.exit(1)

    # Try to connect to the database
    print("\n4. Attempting database connection...")
    from sqlalchemy import create_engine
    
    # Explicitly print connection parameters (password masked)
    db_config = config.DATABASE_CONFIG
    masked_password = '*' * len(db_config['password']) if db_config['password'] else ''
    print(f"   Host: {db_config['host']}")
    print(f"   Port: {db_config['port']}")
    print(f"   Database: {db_config['database']}")
    print(f"   User: {db_config['user']}")
    print(f"   Password: {masked_password}")
    
    engine = create_engine(config.DATABASE_URI)
    connection = engine.connect()
    print("   SUCCESS! Connected to the database.")
    
    # List tables
    from sqlalchemy import inspect
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    print(f"\n5. Found {len(tables)} tables in database:")
    if tables:
        for table in tables:
            print(f"   - {table}")
    else:
        print("   No tables found. Database is empty.")
    
    connection.close()
    print("\nDatabase connection test completed successfully!")

except ImportError as e:
    print(f"ERROR: Import error: {str(e)}")
    print("Make sure you're running this script from the project root directory")
    traceback.print_exc()
    sys.exit(1)
    
except Exception as e:
    print(f"ERROR: {str(e)}")
    traceback.print_exc()
    sys.exit(1)