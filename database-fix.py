#!/usr/bin/env python3
"""
Database connection tester for Football Intelligence platform.
Run this script to verify your database connection and provide diagnostic information.
"""
import os
import sys
import traceback
import pandas as pd
from datetime import datetime

# Add the current directory to the path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

try:
    # Import database configuration
    from config import DATABASE_URI, DATABASE_CONFIG
    
    # Try to import SQLAlchemy
    import sqlalchemy
    from sqlalchemy import create_engine, text
    
    print(f"=== Football Intelligence Database Tester ===")
    print(f"Python version: {sys.version}")
    print(f"SQLAlchemy version: {sqlalchemy.__version__}")
    print(f"Current directory: {current_dir}")
    print(f"")
    
    # Print masked configuration
    masked_password = '*' * len(DATABASE_CONFIG['password']) if DATABASE_CONFIG['password'] else ''
    print(f"Database configuration:")
    print(f"  Host: {DATABASE_CONFIG['host']}")
    print(f"  Port: {DATABASE_CONFIG['port']}")
    print(f"  Database: {DATABASE_CONFIG['database']}")
    print(f"  User: {DATABASE_CONFIG['user']}")
    print(f"  Password: {masked_password}")
    print(f"")
    
    # Test database connection
    print(f"Attempting to connect to database...")
    try:
        engine = create_engine(DATABASE_URI)
        connection = engine.connect()
        print(f"✅ Connection successful!")
        
        # Check if tables exist
        print(f"\nChecking database tables...")
        query = text("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name;
        """)
        
        result = connection.execute(query)
        tables = result.fetchall()
        
        if tables:
            print(f"Found {len(tables)} tables:")
            for table in tables:
                print(f"  - {table[0]}")
                
            # Check table counts
            print(f"\nChecking table counts...")
            for table in tables:
                count_query = text(f"SELECT COUNT(*) FROM {table[0]}")
                count_result = connection.execute(count_query)
                count = count_result.scalar()
                print(f"  - {table[0]}: {count} rows")
        else:
            print(f"No tables found in the database. Tables need to be created.")
            
        connection.close()
        
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        traceback.print_exc()
        
except ImportError as e:
    print(f"❌ Import error: {str(e)}")
    print("Please check that you have the required dependencies installed:")
    print("  - SQLAlchemy")
    print("  - psycopg2-binary")
    print("  - pandas")
    
except Exception as e:
    print(f"❌ Unexpected error: {str(e)}")
    traceback.print_exc()