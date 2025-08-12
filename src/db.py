from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from dotenv import load_dotenv

# Load .env file from the parent directory (root of the project)
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# Get environment variables with defaults
DB_HOST = os.getenv("DB_HOST", "193.194.1.55")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "akumaju_db")
DB_USER = os.getenv("DB_USER", "4kmj_4n4lys1s")
DB_PASSWORD = os.getenv("DB_PASSWORD", "5sB!m2Zx#T8qWj1L")

# Create database URL
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Initialize variables
engine = None
SessionLocal = None
Base = declarative_base()

def get_engine():
    """Get database engine with fallback to SQLite"""
    global engine
    if engine is None:
        
        try:
            engine = create_engine(DATABASE_URL)
            # Test the connection
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                print(f"✅ MySQL connection test successful: {result.fetchone()}")
                
                # Test if the table exists
                table_check = conn.execute(text("SHOW TABLES LIKE 'td_karyawan'"))
                table_exists = table_check.fetchone()
                if table_exists:
                    
                    # Count records in the table
                    count_result = conn.execute(text("SELECT COUNT(*) FROM td_karyawan"))
                    count = count_result.fetchone()[0]
                    
                    # Get table structure
                    structure_result = conn.execute(text("DESCRIBE td_karyawan"))
                    columns = structure_result.fetchall()
                    
                   
                else:
                    print(f"⚠️  Table 'td_karyawan' not found in database '{DB_NAME}'")
                    
        
        except Exception as e:
            print(f"❌ Warning: MySQL connection failed: {e}")
            print("🔄 Using in-memory SQLite database for testing")
            engine = create_engine("sqlite:///./test.db")
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