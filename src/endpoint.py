from typing import List
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

# Flexible imports that work both locally and in Docker
try:
    # Try relative imports first (for Docker)
    from . import crud, schemas
    from .db import get_db
except ImportError:
    # Fall back to absolute imports (for local development)
    import crud, schemas
    from db import get_db


router = APIRouter()


@router.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    return {"status": "TEST", "service": "akumaju-api"}


@router.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "service": "Aku Maju API",
        "version": "1.0.0",
        "endpoints": {
            "karyawan": "/karyawan (includes: id_karyawan, status, loan_kasbon_eligible, klient)",
            "karyawan_filtered": "/karyawan?klient=CLIENT_NAME (filter by specific klient)",
            "health": "/health"
        },
        "usage": {
            "get_all_karyawan": "GET /karyawan",
            "filter_by_klient": "GET /karyawan?klient=PT_ABC"
        }
    }


@router.get("/karyawan", response_model=schemas.KaryawanListResponse)
async def get_all_karyawan(klient: str = None, db: Session = Depends(get_db)):
    """Get all karyawan with optional klient filter"""
    print(f"🌐 API endpoint /karyawan called")
    if klient:
        print(f"🔍 Filtering by klient: {klient}")
    print(f"   Database session: {type(db).__name__}")
    
    try:
        print(f"🔍 About to call crud.get_all_karyawan...")
        karyawan_list = crud.get_all_karyawan(db, klient_filter=klient)
        print(f"📤 Returning {len(karyawan_list)} karyawan records to client")
        
        # Return structured response with status and results
        return {
            "status": "success",
            "count": len(karyawan_list),
            "results": karyawan_list
        }
    except Exception as e:
        print(f"❌ Error in endpoint: {e}")
        print(f"   Error type: {type(e).__name__}")
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "count": 0,
            "results": []
        }


@router.get("/karyawan-raw")
async def get_all_karyawan_raw(db: Session = Depends(get_db)):
    """Get all karyawan using raw SQL for testing"""
    print(f"🌐 API endpoint /karyawan-raw called")
    
    try:
        from sqlalchemy import text
        
        # Use raw SQL to get data
        result = db.execute(text("SELECT id_karyawan, nama, nik, email, status, dept, posisi FROM td_karyawan LIMIT 10"))
        records = result.fetchall()
        
        print(f"📊 Raw SQL returned {len(records)} records")
        
        # Convert to list of dictionaries
        karyawan_list = []
        for record in records:
            karyawan_list.append({
                "id_karyawan": record[0],
                "nama": record[1],
                "nik": record[2],
                "email": record[3],
                "status": record[4],
                "dept": record[5],
                "posisi": record[6]
            })
        
        print(f"📤 Returning {len(karyawan_list)} karyawan records to client")
        return {"status": "success", "count": len(karyawan_list), "data": karyawan_list}
        
    except Exception as e:
        print(f"❌ Error in raw endpoint: {e}")
        print(f"   Error type: {type(e).__name__}")
        return {"status": "error", "message": str(e)}
