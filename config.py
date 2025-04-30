# config.py
import os

# Use environment variables for production, defaults for development
DATABASE_CONFIG = {
    'user': os.environ.get('DB_USER', 'osegonte'),
    'password': os.environ.get('DB_PASSWORD', '1759'),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': os.environ.get('DB_PORT', '5432'),
    'database': os.environ.get('DB_NAME', 'football_intelligence')
}

# Connection string for SQLAlchemy
DATABASE_URI = f"postgresql://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"