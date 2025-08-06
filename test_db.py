#!/usr/bin/env python3
"""
Test script to verify database connection and td_karyawan table access
"""

import os
from dotenv import load_dotenv
from src.db import engine, SessionLocal
from src.models import TdKaryawan

# Load environment variables
load_dotenv()

def test_database_connection():
    """Test database connection"""
    try:
        # Test connection
        with engine.connect() as connection:
            print("✅ Database connection successful!")
            
            # Test if td_karyawan table exists
            result = connection.execute("SHOW TABLES LIKE 'td_karyawan'")
            if result.fetchone():
                print("✅ td_karyawan table found!")
                
                # Get table structure
                result = connection.execute("DESCRIBE td_karyawan")
                columns = result.fetchall()
                print(f"📋 Table structure ({len(columns)} columns):")
                for col in columns:
                    print(f"   - {col[0]} ({col[1]})")
                
                # Count rows
                result = connection.execute("SELECT COUNT(*) FROM td_karyawan")
                count = result.fetchone()[0]
                print(f"📊 Total rows in td_karyawan: {count}")
                
                # Get sample data
                result = connection.execute("SELECT id_karyawan, status, loan_kasbon_eligible FROM td_karyawan LIMIT 5")
                samples = result.fetchall()
                print(f"📝 Sample data (first 5 rows):")
                for row in samples:
                    print(f"   - ID: {row[0]}, Status: {row[1]}, Eligible: {row[2]}")
                    
            else:
                print("❌ td_karyawan table not found!")
                print("Available tables:")
                result = connection.execute("SHOW TABLES")
                tables = result.fetchall()
                for table in tables:
                    print(f"   - {table[0]}")
                    
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("\nEnvironment variables:")
        print(f"DB_HOST: {os.getenv('DB_HOST', 'Not set')}")
        print(f"DB_PORT: {os.getenv('DB_PORT', 'Not set')}")
        print(f"DB_NAME: {os.getenv('DB_NAME', 'Not set')}")
        print(f"DB_USER: {os.getenv('DB_USER', 'Not set')}")
        print(f"DB_PASSWORD: {'Set' if os.getenv('DB_PASSWORD') else 'Not set'}")

def test_sqlalchemy_models():
    """Test SQLAlchemy model operations"""
    try:
        db = SessionLocal()
        
        # Test querying td_karyawan
        karyawan_list = db.query(TdKaryawan).limit(5).all()
        print(f"\n✅ SQLAlchemy query successful! Found {len(karyawan_list)} records")
        
        for karyawan in karyawan_list:
            print(f"   - ID: {karyawan.id_karyawan}, Status: {karyawan.status}, Eligible: {karyawan.loan_kasbon_eligible}")
            
        db.close()
        
    except Exception as e:
        print(f"❌ SQLAlchemy test failed: {e}")

if __name__ == "__main__":
    print("🔍 Testing database connection...")
    test_database_connection()
    
    print("\n🔍 Testing SQLAlchemy models...")
    test_sqlalchemy_models()
    
    print("\n✨ Test completed!") 