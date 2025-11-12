from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
import re

# Flexible imports that work both locally and in Docker
try:
    # Try relative imports first (for Docker)
    from . import crud, schemas
    from ..db import get_db
except ImportError:
    # Fall back to absolute imports (for local development)
    from internal_payroll import crud, schemas
    from db import get_db


router = APIRouter(prefix="/internal_payroll", tags=["internal_payroll"])


@router.get("/total_payroll_disbursed", response_model=schemas.TotalPayrollDisbursedResponse)
async def get_total_payroll_disbursed(
    month: int = None,
    year: int = None,
    dept_id: int = None,
    status_kontrak: int = None,
    valdo_inc: int = None,
    db: Session = Depends(get_db)
):
    """Get total payroll disbursed (sum of take_home_pay) for a given month and year, optionally filtered by dept_id, status_kontrak (1=PKWTT, 2=PKWT, 3=Mitra), and valdo_inc"""
    try:
        total_payroll_disbursed = crud.get_total_payroll_disbursed(
            db,
            month=month,
            year=year,
            dept_id=dept_id,
            status_kontrak=status_kontrak,
            valdo_inc=valdo_inc
        )
        
        return {
            "status": "success",
            "total_payroll_disbursed": total_payroll_disbursed,
            "month": month,
            "year": year,
            "dept_id": dept_id
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "total_payroll_disbursed": 0,
            "month": month,
            "year": year,
            "dept_id": dept_id
        }


@router.get("/total_payroll_headcount", response_model=schemas.TotalPayrollHeadcountResponse)
async def get_total_payroll_headcount(
    month: int = None,
    year: int = None,
    dept_id: int = None,
    status_kontrak: int = None,
    valdo_inc: int = None,
    db: Session = Depends(get_db)
):
    """Get total payroll headcount with breakdown by status_kontrak (count of unique id_karyawan) for a given month and year, optionally filtered by dept_id, status_kontrak (1=PKWTT, 2=PKWT, 3=Mitra), and valdo_inc"""
    try:
        headcount_data = crud.get_total_payroll_headcount(
            db,
            month=month,
            year=year,
            dept_id=dept_id,
            status_kontrak=status_kontrak,
            valdo_inc=valdo_inc
        )
        
        return {
            "status": "success",
            "total_headcount": headcount_data["total_headcount"],
            "pkwtt_headcount": headcount_data["pkwtt_headcount"],
            "pkwt_headcount": headcount_data["pkwt_headcount"],
            "mitra_headcount": headcount_data["mitra_headcount"],
            "month": month,
            "year": year,
            "dept_id": dept_id
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "total_headcount": 0,
            "pkwtt_headcount": 0,
            "pkwt_headcount": 0,
            "mitra_headcount": 0,
            "month": month,
            "year": year,
            "dept_id": dept_id
        }


@router.get("/total_department_count", response_model=schemas.TotalDepartmentCountResponse)
async def get_total_department_count(
    month: int = None,
    year: int = None,
    valdo_inc: int = None,
    db: Session = Depends(get_db)
):
    """Get total number of unique departments (dept_id) from payroll_header for a given month and year, optionally filtered by valdo_inc"""
    try:
        total_department_count = crud.get_total_department_count(
            db,
            month=month,
            year=year,
            valdo_inc=valdo_inc
        )
        
        return {
            "status": "success",
            "total_department_count": total_department_count,
            "month": month,
            "year": year
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "total_department_count": 0,
            "month": month,
            "year": year
        }


@router.get("/total_bpsjtk", response_model=schemas.TotalBpsjtkResponse)
async def get_total_bpsjtk(
    month: int = None,
    year: int = None,
    dept_id: int = None,
    status_kontrak: int = None,
    valdo_inc: int = None,
    db: Session = Depends(get_db)
):
    """Get total BPJS TK (sum of all_bpjs_tk_comp) for a given month and year, optionally filtered by dept_id, status_kontrak (1=PKWTT, 2=PKWT, 3=Mitra), and valdo_inc"""
    try:
        total_bpsjtk = crud.get_total_bpsjtk(
            db,
            month=month,
            year=year,
            dept_id=dept_id,
            status_kontrak=status_kontrak,
            valdo_inc=valdo_inc
        )
        
        return {
            "status": "success",
            "total_bpsjtk": total_bpsjtk,
            "month": month,
            "year": year,
            "dept_id": dept_id
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "total_bpsjtk": 0,
            "month": month,
            "year": year,
            "dept_id": dept_id
        }


@router.get("/total_kesehatan", response_model=schemas.TotalKesehatanResponse)
async def get_total_kesehatan(
    month: int = None,
    year: int = None,
    dept_id: int = None,
    status_kontrak: int = None,
    valdo_inc: int = None,
    db: Session = Depends(get_db)
):
    """Get total BPJS Kesehatan (sum of all_bpjs_kesehatan_comp) for a given month and year, optionally filtered by dept_id, status_kontrak (1=PKWTT, 2=PKWT, 3=Mitra), and valdo_inc"""
    try:
        total_kesehatan = crud.get_total_kesehatan(
            db,
            month=month,
            year=year,
            dept_id=dept_id,
            status_kontrak=status_kontrak,
            valdo_inc=valdo_inc
        )
        
        return {
            "status": "success",
            "total_kesehatan": total_kesehatan,
            "month": month,
            "year": year,
            "dept_id": dept_id
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "total_kesehatan": 0,
            "month": month,
            "year": year,
            "dept_id": dept_id
        }


@router.get("/total_pensiun", response_model=schemas.TotalPensiunResponse)
async def get_total_pensiun(
    month: int = None,
    year: int = None,
    dept_id: int = None,
    status_kontrak: int = None,
    valdo_inc: int = None,
    db: Session = Depends(get_db)
):
    """Get total BPJS Pensiun (sum of all_bpjs_pensiun_comp) for a given month and year, optionally filtered by dept_id, status_kontrak (1=PKWTT, 2=PKWT, 3=Mitra), and valdo_inc"""
    try:
        total_pensiun = crud.get_total_pensiun(
            db,
            month=month,
            year=year,
            dept_id=dept_id,
            status_kontrak=status_kontrak,
            valdo_inc=valdo_inc
        )
        
        return {
            "status": "success",
            "total_pensiun": total_pensiun,
            "month": month,
            "year": year,
            "dept_id": dept_id
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "total_pensiun": 0,
            "month": month,
            "year": year,
            "dept_id": dept_id
        }


@router.get("/filters", response_model=schemas.DepartmentFiltersResponse)
async def get_department_filters(
    month: int = None,
    year: int = None,
    valdo_inc: int = None,
    db: Session = Depends(get_db)
):
    """Get list of departments (dept_id and department_name) from payroll_header joined with payroll_cost_owner for a given month and year, optionally filtered by valdo_inc"""
    try:
        departments = crud.get_department_filters(
            db,
            month=month,
            year=year,
            valdo_inc=valdo_inc
        )
        
        return {
            "status": "success",
            "departments": departments,
            "month": month,
            "year": year,
            "count": len(departments)
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "departments": [],
            "month": month,
            "year": year,
            "count": 0
        }


@router.get("/monthly", response_model=schemas.MonthlyPayrollSummaryResponse)
async def get_monthly_payroll_summary(
    start_month: str,
    end_month: str,
    dept_id: int = None,
    status_kontrak: int = None,
    valdo_inc: int = None,
    db: Session = Depends(get_db)
):
    """Get monthly payroll summaries combining total_disbursed and headcount for each month in the range.
    
    Args:
        start_month: Start month in MM-YYYY format (e.g., "01-2025")
        end_month: End month in MM-YYYY format (e.g., "08-2025")
        dept_id: Optional department ID filter
        status_kontrak: Optional status kontrak filter (1=PKWTT, 2=PKWT, 3=Mitra)
        valdo_inc: Optional valdo_inc filter
    """
    try:
        # Validate format
        date_pattern = re.compile(r'^\d{2}-\d{4}$')
        
        if not date_pattern.match(start_month):
            return {
                "status": "error",
                "message": "start_month must be in MM-YYYY format (e.g., '01-2025')",
                "summaries": {},
                "start_month": start_month,
                "end_month": end_month,
                "dept_id": dept_id
            }
        
        if not date_pattern.match(end_month):
            return {
                "status": "error",
                "message": "end_month must be in MM-YYYY format (e.g., '08-2025')",
                "summaries": {},
                "start_month": start_month,
                "end_month": end_month,
                "dept_id": dept_id
            }
        
        # Parse to validate months are valid
        try:
            start_parts = start_month.split("-")
            end_parts = end_month.split("-")
            start_month_int = int(start_parts[0])
            end_month_int = int(end_parts[0])
            
            if start_month_int < 1 or start_month_int > 12:
                return {
                    "status": "error",
                    "message": "start_month month must be between 01 and 12",
                    "summaries": {},
                    "start_month": start_month,
                    "end_month": end_month,
                    "dept_id": dept_id
                }
            
            if end_month_int < 1 or end_month_int > 12:
                return {
                    "status": "error",
                    "message": "end_month month must be between 01 and 12",
                    "summaries": {},
                    "start_month": start_month,
                    "end_month": end_month,
                    "dept_id": dept_id
                }
        except ValueError:
            return {
                "status": "error",
                "message": "Invalid month format. Must be MM-YYYY",
                "summaries": {},
                "start_month": start_month,
                "end_month": end_month,
                "dept_id": dept_id
            }
        
        monthly_summaries = crud.get_monthly_payroll_summary(
            db,
            start_month_str=start_month,
            end_month_str=end_month,
            dept_id=dept_id,
            status_kontrak=status_kontrak,
            valdo_inc=valdo_inc
        )
        
        return {
            "status": "success",
            "summaries": monthly_summaries,
            "start_month": start_month,
            "end_month": end_month,
            "dept_id": dept_id
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "summaries": {},
            "start_month": start_month,
            "end_month": end_month,
            "dept_id": dept_id
        }


@router.get("/department_summary", response_model=schemas.DepartmentSummaryResponse)
async def get_department_summary(
    month: int = None,
    year: int = None,
    status_kontrak: int = None,
    valdo_inc: int = None,
    db: Session = Depends(get_db)
):
    """Get department summary with headcount breakdown, distribution ratio, and total disbursed. Only includes departments that exist in payroll_cost_owner. Optionally filtered by status_kontrak (1=PKWTT, 2=PKWT, 3=Mitra) and valdo_inc."""
    try:
        departments = crud.get_department_summary(
            db,
            month=month,
            year=year,
            status_kontrak=status_kontrak,
            valdo_inc=valdo_inc
        )
        
        return {
            "status": "success",
            "departments": departments,
            "month": month,
            "year": year,
            "count": len(departments)
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "departments": [],
            "month": month,
            "year": year,
            "count": 0
        }


@router.get("/cost_owner_summary", response_model=schemas.CostOwnerSummaryResponse)
async def get_cost_owner_summary(
    month: int = None,
    year: int = None,
    status_kontrak: int = None,
    valdo_inc: int = None,
    db: Session = Depends(get_db)
):
    """Get cost owner summary with headcount breakdown, distribution ratio, and total disbursed. Only includes departments that exist in payroll_cost_owner. Optionally filtered by status_kontrak (1=PKWTT, 2=PKWT, 3=Mitra) and valdo_inc."""
    try:
        cost_owners = crud.get_cost_owner_summary(
            db,
            month=month,
            year=year,
            status_kontrak=status_kontrak,
            valdo_inc=valdo_inc
        )
        
        return {
            "status": "success",
            "cost_owners": cost_owners,
            "month": month,
            "year": year,
            "count": len(cost_owners)
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "cost_owners": [],
            "month": month,
            "year": year,
            "count": 0
        }

