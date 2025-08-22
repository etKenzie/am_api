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
    try:

        karyawan_list = crud.get_enhanced_karyawan(
            db, 
            id_karyawan_filter=id_karyawan,
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project
        )
        

        
        # Return structured response with status and results
        return {
            "status": "success",
            "count": len(karyawan_list),
            "results": karyawan_list
        }
    except Exception as e:

        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "count": 0,
            "results": []
        }


@router.get("/summary", response_model=schemas.SummaryResponse)
async def get_summary(
    month: int,
    year: int,
    id_karyawan: int = None,
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None, 
    db: Session = Depends(get_db)
):
    """Get kasbon summary with eligible count and kasbon request metrics"""
    try:
        coverage_summary = crud.get_user_coverage_summary(
            db, 
            id_karyawan_filter=id_karyawan,
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project,
            month_filter=month,
            year_filter=year
        )
        
        # Return structured response
        return {
            "status": "success",
            "total_eligible_employees": coverage_summary["total_eligible_employees"],
            "total_processed_kasbon_requests": coverage_summary["total_processed_kasbon_requests"],
            "total_pending_kasbon_requests": coverage_summary["total_pending_kasbon_requests"],
            "total_first_borrow": coverage_summary["total_first_borrow"],
            "total_approved_requests": coverage_summary["total_approved_requests"],
            "total_rejected_requests": coverage_summary["total_rejected_requests"],
            "total_disbursed_amount": coverage_summary["total_disbursed_amount"],
            "total_loans": coverage_summary["total_loans"],
            "average_disbursed_amount": coverage_summary["average_disbursed_amount"],
            "approval_rate": coverage_summary["approval_rate"],
            "average_approval_time": coverage_summary["average_approval_time"],
            "penetration_rate": coverage_summary["penetration_rate"]
        }
    except Exception as e:

        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "total_eligible_employees": 0,
            "total_processed_kasbon_requests": 0,
            "total_pending_kasbon_requests": 0,
            "total_first_borrow": 0,
            "total_approved_requests": 0,
            "total_rejected_requests": 0,
            "total_disbursed_amount": 0,
            "total_loans": 0,
            "average_disbursed_amount": 0,
            "approval_rate": 0,
            "average_approval_time": 0,
            "penetration_rate": 0
        }


@router.get("/user-coverage", response_model=schemas.UserCoverageResponse)
async def get_user_coverage(
    month: int = None,
    year: int = None,
    id_karyawan: int = None,
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None, 
    db: Session = Depends(get_db)
):
    """Get user coverage metrics: total_eligible_employees, total_kasbon_requests, penetration_rate, total_first_borrow"""
    try:
        coverage_data = crud.get_user_coverage_endpoint(
            db, 
            id_karyawan_filter=id_karyawan,
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project,
            month_filter=month,
            year_filter=year
        )
        
        # Return structured response
        return {
            "status": "success",
            "total_eligible_employees": coverage_data["total_eligible_employees"],
            "total_kasbon_requests": coverage_data["total_kasbon_requests"],
            "penetration_rate": coverage_data["penetration_rate"],
            "total_first_borrow": coverage_data["total_first_borrow"]
        }
    except Exception as e:
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "total_eligible_employees": 0,
            "total_kasbon_requests": 0,
            "penetration_rate": 0,
            "total_first_borrow": 0
        }


@router.get("/requests", response_model=schemas.RequestsResponse)
async def get_requests(
    month: int = None,
    year: int = None,
    id_karyawan: int = None,
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None, 
    db: Session = Depends(get_db)
):
    """Get requests metrics: total_approved_requests, total_rejected_requests, approval_rate, average_approval_time"""
    try:
        requests_data = crud.get_requests_endpoint(
            db, 
            id_karyawan_filter=id_karyawan,
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project,
            month_filter=month,
            year_filter=year
        )
        
        # Return structured response
        return {
            "status": "success",
            "total_approved_requests": requests_data["total_approved_requests"],
            "total_rejected_requests": requests_data["total_rejected_requests"],
            "approval_rate": requests_data["approval_rate"],
            "average_approval_time": requests_data["average_approval_time"]
        }
    except Exception as e:

        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "total_approved_requests": 0,
            "total_rejected_requests": 0,
            "approval_rate": 0,
            "average_approval_time": 0
        }


@router.get("/disbursement", response_model=schemas.DisbursementResponse)
async def get_disbursement(
    month: int = None,
    year: int = None,
    id_karyawan: int = None,
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None, 
    db: Session = Depends(get_db)
):
    """Get disbursement metrics: total_disbursed_amount, average_disbursed_amount"""
    try:
        disbursement_data = crud.get_disbursement_endpoint(
            db, 
            id_karyawan_filter=id_karyawan,
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project,
            month_filter=month,
            year_filter=year
        )
        
        # Return structured response
        return {
            "status": "success",
            "total_disbursed_amount": disbursement_data["total_disbursed_amount"],
            "average_disbursed_amount": disbursement_data["average_disbursed_amount"]
        }
    except Exception as e:

        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "total_disbursed_amount": 0,
            "average_disbursed_amount": 0
        }


@router.get("/user-coverage-monthly", response_model=schemas.UserCoverageMonthlyResponse)
async def get_user_coverage_monthly(
    start_date: str,
    end_date: str,
    id_karyawan: int = None,
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None, 
    db: Session = Depends(get_db)
):
    """Get user coverage monthly data: eligible employees and kasbon applicants by month"""
    try:
        monthly_coverage = crud.get_user_coverage_monthly_endpoint(
            db, 
            start_date=start_date,
            end_date=end_date,
            id_karyawan_filter=id_karyawan,
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project
        )
        

        
        # Return structured response
        return {
            "status": "success",
            "monthly_data": monthly_coverage
        }
    except Exception as e:
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "monthly_data": {}
        }


@router.get("/disbursement-monthly", response_model=schemas.DisbursementMonthlyResponse)
async def get_disbursement_monthly(
    start_date: str,
    end_date: str,
    id_karyawan: int = None,
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None, 
    db: Session = Depends(get_db)
):
    """Get disbursement monthly data: total disbursed amount and average disbursed amount by month"""
    
    try:
        monthly_disbursement = crud.get_disbursement_monthly_endpoint(
            db, 
            start_date=start_date,
            end_date=end_date,
            id_karyawan_filter=id_karyawan,
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project
        )
        
        
        # Return structured response
        return {
            "status": "success",
            "monthly_data": monthly_disbursement
        }
    except Exception as e:
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "monthly_data": {}
        }


@router.get("/summary-monthly", response_model=schemas.SummaryMonthlyResponse)
async def get_summary_monthly(
    start_date: str,
    end_date: str,
    id_karyawan: int = None,
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None, 
    db: Session = Depends(get_db)
):
    """Get kasbon summary monthly data with eligible count and kasbon request metrics"""
    
    try:
        monthly_coverage = crud.get_user_coverage_monthly_summary(
            db, 
            start_date=start_date,
            end_date=end_date,
            id_karyawan_filter=id_karyawan,
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project
        )
        
        
        # Return structured response
        return {
            "status": "success",
            "monthly_data": monthly_coverage
        }
    except Exception as e:
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "monthly_data": {}
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
    try:
        loans_list = crud.get_loans_with_karyawan(
            db, 
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project,
            loan_status_filter=loan_status,
            id_karyawan_filter=id_karyawan
        )
        
        
        # Return structured response with status and results
        return {
            "status": "success",
            "count": len(loans_list),
            "results": loans_list
        }
    except Exception as e:
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "count": 0,
            "results": []
        }


@router.get("/loan-purpose", response_model=schemas.LoanPurposeSummaryListResponse)
async def get_loan_purpose_summary(
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None,
    loan_status: int = None,
    id_karyawan: int = None,
    month: int = None,
    year: int = None,
    db: Session = Depends(get_db)
):
    """Get loan summary grouped by purpose with total count and sum of total_loan"""
    
    try:
        purpose_summary = crud.get_loan_purpose_summary(
            db, 
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project,
            loan_status_filter=loan_status,
            id_karyawan_filter=id_karyawan,
            month_filter=month,
            year_filter=year
        )
        
        
        # Return structured response
        return {
            "status": "success",
            "count": len(purpose_summary),
            "results": purpose_summary
        }
    except Exception as e:
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "count": 0,
            "results": []
        }


@router.get("/filters")
async def get_available_filters(
    employer: str = None, 
    placement: str = None, 
    db: Session = Depends(get_db)
):
    """Get available filter values for enhanced karyawan queries with cascading filters"""
    
    try:
        filter_values = crud.get_available_filter_values(
            db, 
            employer_filter=employer,
            placement_filter=placement
        )
        
        return {
            "status": "success",
            "filters": filter_values
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "filters": {}
        }


@router.get("/loan-fees", response_model=schemas.LoanFeesResponse)
async def get_loan_fees(
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None,
    loan_status: int = None,
    id_karyawan: int = None,
    month: int = None,
    year: int = None,
    db: Session = Depends(get_db)
):
    """Get loan fees summary (total expected and collected admin fees)"""
    
    try:
        fees_summary = crud.get_loan_fees_summary(
            db, 
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project,
            loan_status_filter=loan_status,
            id_karyawan_filter=id_karyawan,
            month_filter=month,
            year_filter=year
        )
        
        
        # Return structured response
        return {
            "status": "success",
            "total_expected_admin_fee": fees_summary["total_expected_admin_fee"],
            "expected_loans_count": fees_summary["expected_loans_count"],
            "total_collected_admin_fee": fees_summary["total_collected_admin_fee"],
            "collected_loans_count": fees_summary["collected_loans_count"],
            "total_failed_payment": fees_summary["total_failed_payment"],
            "admin_fee_profit": fees_summary["admin_fee_profit"],
        }
    except Exception as e:
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "total_expected_admin_fee": 0,
            "expected_loans_count": 0,
            "total_collected_admin_fee": 0,
            "collected_loans_count": 0,
            "total_failed_payment": 0,
            "admin_fee_profit": 0
        }


@router.get("/loan-fees-monthly", response_model=schemas.LoanFeesMonthlyResponse)
async def get_loan_fees_monthly(
    start_date: str,
    end_date: str,
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None,
    loan_status: int = None,
    id_karyawan: int = None,
    db: Session = Depends(get_db)
):
    """Get loan fees summary separated by months within a date range
    
    Required parameters:
    - start_date: Start date in YYYY-MM-DD format (e.g., "2024-01-01")
    - end_date: End date in YYYY-MM-DD format (e.g., "2024-12-31")
    """
    
    try:
        monthly_fees_summary = crud.get_loan_fees_monthly_summary(
            db, 
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project,
            loan_status_filter=loan_status,
            id_karyawan_filter=id_karyawan,
            start_date=start_date,
            end_date=end_date
        )
        
        
        # Return structured response
        return {
            "status": "success",
            "monthly_data": monthly_fees_summary
        }
    except Exception as e:
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "monthly_data": {}
        }


@router.get("/loan-risk", response_model=schemas.LoanRiskResponse)
async def get_loan_risk(
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None,
    loan_status: int = None,
    id_karyawan: int = None,
    month: int = None,
    year: int = None,
    db: Session = Depends(get_db)
):
    """Get loan risk summary with various risk metrics"""
    
    try:
        risk_summary = crud.get_loan_risk_summary(
            db, 
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project,
            loan_status_filter=loan_status,
            id_karyawan_filter=id_karyawan,
            month_filter=month,
            year_filter=year
        )
        
        
        # Return structured response
        return {
            "status": "success",
            "total_unrecovered_kasbon": risk_summary["total_unrecovered_kasbon"],
            "unrecovered_kasbon_count": risk_summary["unrecovered_kasbon_count"],
            "total_expected_repayment": risk_summary["total_expected_repayment"],
            "kasbon_principal_recovery_rate": risk_summary["kasbon_principal_recovery_rate"]
        }
    except Exception as e:
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "total_unrecovered_kasbon": 0,
            "unrecovered_kasbon_count": 0,
            "total_expected_repayment": 0,
            "kasbon_principal_recovery_rate": 0
        }


@router.get("/loan-risk-monthly", response_model=schemas.LoanRiskMonthlyResponse)
async def get_loan_risk_monthly(
    start_date: str,
    end_date: str,
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None,
    loan_status: int = None,
    id_karyawan: int = None,
    db: Session = Depends(get_db)
):
    """Get loan risk summary separated by months within a date range
    
    Required parameters:
    - start_date: Start date in YYYY-MM-DD format (e.g., "2024-01-01")
    - end_date: End date in YYYY-MM-DD format (e.g., "2024-12-31")
    """
    
    try:
        monthly_risk_summary = crud.get_loan_risk_monthly_summary(
            db, 
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project,
            loan_status_filter=loan_status,
            id_karyawan_filter=id_karyawan,
            start_date=start_date,
            end_date=end_date
        )
        
        
        # Return structured response
        return {
            "status": "success",
            "monthly_data": monthly_risk_summary
        }
    except Exception as e:
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "monthly_data": {}
        }


@router.get("/karyawan-overdue", response_model=schemas.KaryawanOverdueListResponse)
async def get_karyawan_overdue(
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None,
    loan_status: int = None,
    id_karyawan: int = None,
    month: int = None,
    year: int = None,
    db: Session = Depends(get_db)
):
    """Get karyawan data for those with overdue loans (status 4)"""
    
    try:
        overdue_list = crud.get_karyawan_overdue_summary(
            db, 
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project,
            loan_status_filter=loan_status,
            id_karyawan_filter=id_karyawan,
            month_filter=month,
            year_filter=year
        )
        
        
        # Return structured response
        return {
            "status": "success",
            "count": len(overdue_list),
            "results": overdue_list
        }
    except Exception as e:
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "count": 0,
            "results": []
        }