from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List

# Flexible imports that work both locally and in Docker
try:
    # Try relative imports first (for Docker)
    from . import models
except ImportError:
    # Fall back to absolute imports (for local development)
    import models


def get_all_karyawan(db: Session, limit: int = 1000000, klient_filter: str = None) -> List[models.TdKaryawan]:
    """Get all karyawan with a limit of 1000 and optional klient filter"""

    
    try:
        # Try to get the count first
        if klient_filter:
            count_result = db.execute(text("SELECT COUNT(*) FROM td_karyawan WHERE klient = :klient"), {"klient": klient_filter})
        else:
            count_result = db.execute(text("SELECT COUNT(*) FROM td_karyawan"))
        count = count_result.fetchone()[0]


        # Test raw SQL query for the fields we need
        if klient_filter:
            raw_result = db.execute(text("SELECT id_karyawan, status, loan_kasbon_eligible, klient FROM td_karyawan WHERE klient = :klient LIMIT 3"), {"klient": klient_filter})
        else:
            raw_result = db.execute(text("SELECT id_karyawan, status, loan_kasbon_eligible, klient FROM td_karyawan LIMIT 3"))
        raw_records = raw_result.fetchall()
        print(f"📋 Raw SQL test results:")
        for record in raw_records:
            print(f"   ID: {record[0]}, Status: {record[1]}, Loan: {record[2]}, Klient: {record[3]}")
        
        # Now get the actual data using SQLAlchemy ORM
        print(f"🔍 Getting data via ORM...")
        query = db.query(models.TdKaryawan)
        if klient_filter:
            query = query.filter(models.TdKaryawan.klient == klient_filter)
        karyawan_list = query.limit(limit).all()
        
        print(f"✅ Retrieved {len(karyawan_list)} karyawan records via ORM")
        
        # Show some details about the first few records
        for i, karyawan in enumerate(karyawan_list[:3]):
            print(f"   Record {i+1}: ID={karyawan.id_karyawan}, Status={karyawan.status}, Loan={karyawan.loan_kasbon_eligible}, Klient={karyawan.klient}")
        
        return karyawan_list
        
    except Exception as e:
        print(f"❌ Error querying karyawan table: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return [] 