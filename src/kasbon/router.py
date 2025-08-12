from typing import List
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

# Flexible imports that work both locally and in Docker
try:
    # Try relative imports first (for Docker)
    from . import crud, schemas
    from ..db import get_db
except ImportError:
    # Fall back to absolute imports (for local development)
    from kasbon import crud, schemas
    from db import get_db


router = APIRouter(prefix="/kasbon", tags=["kasbon"])


@router.get("/karyawan", response_model=schemas.KaryawanEnhancedListResponse)
async def get_karyawan(
    id_karyawan: int = None,
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None, 
    db: Session = Depends(get_db)
):
    """Get enhanced karyawan data with join to tbl_gmc table and multiple filters"""
    print(f"🌐 API endpoint /kasbon/karyawan called")
    print(f"🔍 Filters: id_karyawan={id_karyawan}, employer={employer}, sourced_to={sourced_to}, project={project}")
    
    try:
        print(f"🔍 About to call crud.get_enhanced_karyawan...")
        karyawan_list = crud.get_enhanced_karyawan(
            db, 
            id_karyawan_filter=id_karyawan,
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project
        )
        
        print(f"📤 Returning {len(karyawan_list)} enhanced karyawan records to client")
        
        # Return structured response with status and results
        return {
            "status": "success",
            "count": len(karyawan_list),
            "results": karyawan_list
        }
    except Exception as e:
        print(f"❌ Error in karyawan endpoint: {e}")
        print(f"   Error type: {type(e).__name__}")
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "count": 0,
            "results": []
        }


@router.get("/eligible-count", response_model=schemas.EligibleCountResponse)
async def get_eligible_count(
    id_karyawan: int = None,
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None, 
    db: Session = Depends(get_db)
):
    """Get count of eligible employees (status = '1' and loan_kasbon_eligible = '1')"""
    print(f"🌐 API endpoint /kasbon/eligible-count called")
    print(f"🔍 Filters: id_karyawan={id_karyawan}, employer={employer}, sourced_to={sourced_to}, project={project}")
    
    try:
        print(f"🔍 About to call crud.get_eligible_count...")
        total_eligible = crud.get_eligible_count(
            db, 
            id_karyawan_filter=id_karyawan,
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project
        )
        
        print(f"📊 Total eligible employees: {total_eligible}")
        
        # Return structured response
        return {
            "status": "success",
            "total_eligible_employees": total_eligible
        }
    except Exception as e:
        print(f"❌ Error in eligible count endpoint: {e}")
        print(f"   Error type: {type(e).__name__}")
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "total_eligible_employees": 0
        }


@router.get("/loans", response_model=schemas.LoanListResponse)
async def get_loans(
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None,
    loan_status: int = None,
    id_karyawan: int = None,
    db: Session = Depends(get_db)
):
    """Get loans data with enhanced karyawan information and filters"""
    print(f"🌐 API endpoint /kasbon/loans called")
    print(f"🔍 Filters: employer={employer}, sourced_to={sourced_to}, project={project}, loan_status={loan_status}, id_karyawan={id_karyawan}")
    
    try:
        print(f"🔍 About to call crud.get_loans_with_karyawan...")
        loans_list = crud.get_loans_with_karyawan(
            db, 
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project,
            loan_status_filter=loan_status,
            id_karyawan_filter=id_karyawan
        )
        
        print(f"📤 Returning {len(loans_list)} loan records to client")
        
        # Return structured response with status and results
        return {
            "status": "success",
            "count": len(loans_list),
            "results": loans_list
        }
    except Exception as e:
        print(f"❌ Error in loans endpoint: {e}")
        print(f"   Error type: {type(e).__name__}")
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "count": 0,
            "results": []
        }


@router.get("/filters")
async def get_available_filters(db: Session = Depends(get_db)):
    """Get available filter values for enhanced karyawan queries"""
    print(f"🌐 API endpoint /kasbon/filters called")
    
    try:
        print(f"🔍 About to call crud.get_available_filter_values...")
        filter_values = crud.get_available_filter_values(db)
        print(f"📤 Returning filter values to client")
        
        return {
            "status": "success",
            "filters": filter_values
        }
    except Exception as e:
        print(f"❌ Error getting filter values: {e}")
        return {
            "status": "error",
            "message": str(e),
            "filters": {}
        } 