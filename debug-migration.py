#!/usr/bin/env python3
"""
Debug CSV migration script - simplified with explicit print statements
"""
import os
import sys
import traceback

print("=== Debug CSV Migration Script ===")
print(f"Python version: {sys.version}")
print(f"Current directory: {os.getcwd()}")
print("")

# Check if CSV file was provided
if len(sys.argv) < 2:
    print("Usage: python debug-migration.py <csv_file>")
    sys.exit(1)

csv_path = sys.argv[1]
print(f"CSV file: {csv_path}")

# Check if the file exists
if not os.path.exists(csv_path):
    print(f"ERROR: CSV file not found: {csv_path}")
    sys.exit(1)

print(f"CSV file exists: {csv_path}")

# Try to import pandas
try:
    print("\n1. Importing pandas...")
    import pandas as pd
    print("   Success!")
except ImportError:
    print("   ERROR: pandas is not installed. Run: pip install pandas")
    sys.exit(1)

# Try to read the CSV file
try:
    print("\n2. Reading CSV file...")
    df = pd.read_csv(csv_path)
    print(f"   Success! Found {len(df)} rows and {len(df.columns)} columns:")
    print(f"   Columns: {df.columns.tolist()}")
    
    # Check for required columns
    required_columns = ['home_team', 'away_team', 'league', 'country', 'date']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"   WARNING: Missing required columns: {missing_columns}")
    else:
        print("   All required columns are present.")
    
    # Check the date column
    if 'date' in df.columns:
        print(f"\n3. Checking date column...")
        print(f"   Date column type: {df['date'].dtype}")
        print(f"   First few dates: {df['date'].head().tolist()}")
        
        # Try to convert to datetime
        try:
            dates = pd.to_datetime(df['date'])
            print(f"   Date range: {dates.min()} to {dates.max()}")
        except Exception as e:
            print(f"   ERROR: Could not convert dates: {str(e)}")
    
    # Show first row as example
    print("\n4. Sample row (first row):")
    sample_row = df.iloc[0].to_dict()
    for key, value in sample_row.items():
        print(f"   {key}: {value}")
except Exception as e:
    print(f"   ERROR reading CSV: {str(e)}")
    traceback.print_exc()
    sys.exit(1)

# Try to import database modules
try:
    print("\n5. Importing database modules...")
    import config
    print("   Imported config successfully")
    print(f"   Database URI: {config.DATABASE_URI}")
    
    from sqlalchemy import create_engine, inspect
    print("   Imported SQLAlchemy successfully")
    
    # Try to connect to database
    print("\n6. Connecting to database...")
    engine = create_engine(config.DATABASE_URI)
    connection = engine.connect()
    print("   Connected successfully!")
    
    # Check if tables exist
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print(f"\n7. Found {len(tables)} tables in database:")
    if tables:
        for table in tables:
            print(f"   - {table}")
    else:
        print("   No tables found. Database is empty.")
    
    connection.close()

except ImportError as e:
    print(f"   ERROR importing database modules: {str(e)}")
    traceback.print_exc()
    sys.exit(1)
except Exception as e:
    print(f"   ERROR with database: {str(e)}")
    traceback.print_exc()
    sys.exit(1)

print("\nDebug completed successfully!")