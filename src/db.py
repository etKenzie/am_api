from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
import os
import time
from dotenv import load_dotenv

# Load .env file from the parent directory (root of the project)
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# Get environment variables with defaults
DB_HOST = os.getenv("DB_HOST", "valdo-database.cryywjm0cqqp.ap-southeast-3.rds.amazonaws.com")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "db_am")
DB_USER = os.getenv("DB_USER", "4kmj_4n4lys1s")
DB_PASSWORD = os.getenv("DB_PASSWORD", "5sB!m2Zx#T8qWj1L")

# Create database URL (same server, same database)
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Initialize variables
engine = None
SessionLocal = None
Base = declarative_base()

def get_engine():
    """Get database engine with retry mechanism for MySQL connection"""
    global engine
    if engine is None:
        max_retries = 0
        retry_delay = 3  # seconds
        
        for attempt in range(max_retries):
            try:
                print(f"🔄 Attempting to connect to MySQL database (attempt {attempt + 1}/{max_retries})...")
                engine = create_engine(DATABASE_URL)
                
                # Test the connection
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT 1"))
                    print(f"✅ MySQL connection successful: {result.fetchone()}")
                    
                    # Test if the table exists
                    table_check = conn.execute(text("SHOW TABLES LIKE 'td_karyawan'"))
                    table_exists = table_check.fetchone()
                    if table_exists:
                        # Count records in the table
                        count_result = conn.execute(text("SELECT COUNT(*) FROM td_karyawan"))
                        count = count_result.fetchone()[0]
                        print(f"📊 Found {count} records in td_karyawan table")
                        
                        # Get table structure
                        structure_result = conn.execute(text("DESCRIBE td_karyawan"))
                        columns = structure_result.fetchall()
                        print(f"📋 td_karyawan table has {len(columns)} columns")
                    else:
                        print(f"⚠️  Table 'td_karyawan' not found in database '{DB_NAME}'")
                
                # If we get here, connection is successful
                return engine
                
            except Exception as e:
                print(f"❌ MySQL connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    print(f"⏳ Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print(f"💥 Failed to connect to MySQL after {max_retries} attempts")
                    raise Exception(f"Database connection failed after {max_retries} attempts: {e}")
    
    return engine

def get_session_local():
    """Get session local with lazy initialization"""
    global SessionLocal
    if SessionLocal is None:
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
    return SessionLocal

def get_db():
    """Get database session with lazy initialization"""
    db = get_session_local()()
    try:
        yield db
    finally:
        db.close()