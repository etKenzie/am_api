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
        print(f"🔍 About to call crud.get_user_coverage_summary...")
        coverage_summary = crud.get_user_coverage_summary(
            db, 
            id_karyawan_filter=id_karyawan,
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project,
            month_filter=month,
            year_filter=year
        )
        
        print(f"📊 User coverage summary completed")
        
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
            "total_unique_requests": coverage_summary["total_unique_requests"],
            "average_disbursed_amount": coverage_summary["average_disbursed_amount"],
            "approval_rate": coverage_summary["approval_rate"],
            "average_approval_time": coverage_summary["average_approval_time"],
            "penetration_rate": coverage_summary["penetration_rate"]
        }
    except Exception as e:
        print(f"❌ Error in user coverage endpoint: {e}")
        print(f"   Error type: {type(e).__name__}")
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
            "total_unique_requests": 0,
            "average_disbursed_amount": 0,
            "approval_rate": 0,
            "average_approval_time": 0,
            "penetration_rate": 0
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
    print(f"🌐 API endpoint /kasbon/summary-monthly called")
    print(f"🔍 Filters: start_date={start_date}, end_date={end_date}, id_karyawan={id_karyawan}, employer={employer}, sourced_to={sourced_to}, project={project}")
    
    try:
        print(f"🔍 About to call crud.get_user_coverage_monthly_summary...")
        monthly_coverage = crud.get_user_coverage_monthly_summary(
            db, 
            start_date=start_date,
            end_date=end_date,
            id_karyawan_filter=id_karyawan,
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project
        )
        
        print(f"📊 Monthly user coverage summary completed")
        
        # Return structured response
        return {
            "status": "success",
            "monthly_data": monthly_coverage
        }
    except Exception as e:
        print(f"❌ Error in monthly user coverage endpoint: {e}")
        print(f"   Error type: {type(e).__name__}")
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
    print(f"🌐 API endpoint /kasbon/loan-purpose-summary called")
    print(f"🔍 Filters: employer={employer}, sourced_to={sourced_to}, project={project}, loan_status={loan_status}, id_karyawan={id_karyawan}, month={month}, year={year}")
    
    try:
        print(f"🔍 About to call crud.get_loan_purpose_summary...")
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
        
        print(f"📊 Returning loan purpose summary to client")
        
        # Return structured response
        return {
            "status": "success",
            "count": len(purpose_summary),
            "results": purpose_summary
        }
    except Exception as e:
        print(f"❌ Error in loan purpose summary endpoint: {e}")
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
    print(f"🌐 API endpoint /kasbon/loan-fees called")
    print(f"🔍 Filters: employer={employer}, sourced_to={sourced_to}, project={project}, loan_status={loan_status}, id_karyawan={id_karyawan}, month={month}, year={year}")
    
    try:
        print(f"🔍 About to call crud.get_loan_fees_summary...")
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
        
        print(f"💰 Returning loan fees summary to client")
        
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
        print(f"❌ Error in loan fees endpoint: {e}")
        print(f"   Error type: {type(e).__name__}")
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
    print(f"🌐 API endpoint /kasbon/loan-fees-monthly called")
    print(f"🔍 Filters: employer={employer}, sourced_to={sourced_to}, project={project}, loan_status={loan_status}, id_karyawan={id_karyawan}, start_date={start_date}, end_date={end_date}")
    
    try:
        print(f"🔍 About to call crud.get_loan_fees_monthly_summary...")
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
        
        print(f"💰 Returning monthly loan fees summary to client")
        
        # Return structured response
        return {
            "status": "success",
            "monthly_data": monthly_fees_summary
        }
    except Exception as e:
        print(f"❌ Error in monthly loan fees endpoint: {e}")
        print(f"   Error type: {type(e).__name__}")
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
    print(f"🌐 API endpoint /kasbon/loan-risk called")
    print(f"🔍 Filters: employer={employer}, sourced_to={sourced_to}, project={project}, loan_status={loan_status}, id_karyawan={id_karyawan}, month={month}, year={year}")
    
    try:
        print(f"🔍 About to call crud.get_loan_risk_summary...")
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
        
        print(f"📊 Returning loan risk summary to client")
        
        # Return structured response
        return {
            "status": "success",
            "total_unrecovered_kasbon": risk_summary["total_unrecovered_kasbon"],
            "unrecovered_kasbon_count": risk_summary["unrecovered_kasbon_count"],
            "total_expected_repayment": risk_summary["total_expected_repayment"],
            "kasbon_principal_recovery_rate": risk_summary["kasbon_principal_recovery_rate"]
        }
    except Exception as e:
        print(f"❌ Error in loan risk endpoint: {e}")
        print(f"   Error type: {type(e).__name__}")
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
    print(f"🌐 API endpoint /kasbon/loan-risk-monthly called")
    print(f"🔍 Filters: employer={employer}, sourced_to={sourced_to}, project={project}, loan_status={loan_status}, id_karyawan={id_karyawan}, start_date={start_date}, end_date={end_date}")
    
    try:
        print(f"🔍 About to call crud.get_loan_risk_monthly_summary...")
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
        
        print(f"📊 Returning monthly loan risk summary to client")
        
        # Return structured response
        return {
            "status": "success",
            "monthly_data": monthly_risk_summary
        }
    except Exception as e:
        print(f"❌ Error in monthly loan risk endpoint: {e}")
        print(f"   Error type: {type(e).__name__}")
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
    print(f"🌐 API endpoint /kasbon/karyawan-overdue called")
    print(f"🔍 Filters: employer={employer}, sourced_to={sourced_to}, project={project}, loan_status={loan_status}, id_karyawan={id_karyawan}, month={month}, year={year}")
    
    try:
        print(f"🔍 About to call crud.get_karyawan_overdue_summary...")
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
        
        print(f"📊 Returning karyawan overdue summary to client")
        
        # Return structured response
        return {
            "status": "success",
            "count": len(overdue_list),
            "results": overdue_list
        }
    except Exception as e:
        print(f"❌ Error in karyawan overdue endpoint: {e}")
        print(f"   Error type: {type(e).__name__}")
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "count": 0,
            "results": []
        }