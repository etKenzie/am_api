from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from dotenv import load_dotenv

# Load .env file from the parent directory (root of the project)
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# Get environment variables with defaults
DB_HOST = os.getenv("DB_HOST", "valdo-database.cryywjm0cqqp.ap-southeast-3.rds.amazonaws.com")
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
        print(f"🔍 Attempting to connect to database...")
        print(f"   Host: {DB_HOST}")
        print(f"   Port: {DB_PORT}")
        print(f"   Database: {DB_NAME}")
        print(f"   User: {DB_USER}")
        print(f"   URL: mysql+pymysql://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        
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
                    print(f"✅ Table 'td_karyawan' found in database '{DB_NAME}'")
                    
                    # Count records in the table
                    count_result = conn.execute(text("SELECT COUNT(*) FROM td_karyawan"))
                    count = count_result.fetchone()[0]
                    print(f"📊 Found {count} records in td_karyawan table")
                    
                    # Get table structure
                    print(f"🔍 Getting table structure...")
                    structure_result = conn.execute(text("DESCRIBE td_karyawan"))
                    columns = structure_result.fetchall()
                    print(f"📋 Table structure:")
                    for col in columns:
                        print(f"   {col[0]} - {col[1]} - {col[2]} - {col[3]} - {col[4]} - {col[5]}")
                    
                    # Show first few records
                    if count > 0:
                        sample_result = conn.execute(text("SELECT * FROM td_karyawan LIMIT 3"))
                        sample_records = sample_result.fetchall()
                        print(f"📋 Sample records:")
                        for record in sample_records:
                            print(f"   {record}")
                else:
                    print(f"⚠️  Table 'td_karyawan' not found in database '{DB_NAME}'")
                    print(f"   Available tables:")
                    tables_result = conn.execute(text("SHOW TABLES"))
                    tables = tables_result.fetchall()
                    for table in tables:
                        print(f"   - {table[0]}")
                        
            print("✅ Connected to MySQL database successfully")
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