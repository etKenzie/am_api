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
    from loan import crud, schemas
    from db import get_db


router = APIRouter(prefix="/loan", tags=["loan"])


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





@router.get("/client-summary")
async def get_client_summary(
    month: int = None,
    year: int = None,
    loan_type: str = "loan",
    db: Session = Depends(get_db)
):
    """Get comprehensive client summary with disbursement and other metrics"""
    try:
        client_summaries = crud.get_client_summary(
            db, 
            month_filter=month,
            year_filter=year,
            loan_type=loan_type
        )
        
        return {
            "status": "success",
            "count": len(client_summaries),
            "results": client_summaries
        }
    except Exception as e:
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
    """Get loan summary with eligible count and loan request metrics"""
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
            "total_processed_loan_requests": coverage_summary["total_processed_loan_requests"],
            "total_pending_loan_requests": coverage_summary["total_pending_loan_requests"],
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
            "total_processed_loan_requests": 0,
            "total_pending_loan_requests": 0,
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
    """Get loan summary monthly data with eligible count and loan request metrics"""
    
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
    loan_type: str = "loan",
    db: Session = Depends(get_db)
):
    """Get available filter values for enhanced karyawan queries with cascading filters"""
    
    try:
        filter_values = crud.get_available_filter_values(
            db, 
            employer_filter=employer,
            placement_filter=placement,
            loan_type=loan_type
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
            "total_unrecovered_loan": risk_summary["total_unrecovered_loan"],
            "unrecovered_loan_count": risk_summary["unrecovered_loan_count"],
            "total_expected_repayment": risk_summary["total_expected_repayment"],
            "loan_principal_recovery_rate": risk_summary["loan_principal_recovery_rate"]
        }
    except Exception as e:
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "total_unrecovered_loan": 0,
            "unrecovered_loan_count": 0,
            "total_expected_repayment": 0,
            "loan_principal_recovery_rate": 0
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
    loan_type: str = "loan",
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
            year_filter=year,
            loan_type=loan_type
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


@router.get("/repayment-risk", response_model=schemas.RepaymentRiskResponse)
async def get_repayment_risk(
    month: int = None,
    year: int = None,
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None,
    loan_status: int = None,
    id_karyawan: int = None,
    loan_type: str = "loan",
    db: Session = Depends(get_db)
):
    """Get repayment risk summary with various repayment and risk metrics"""
    
    try:
        repayment_risk_summary = crud.get_repayment_risk_summary(
            db, 
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project,
            loan_status_filter=loan_status,
            id_karyawan_filter=id_karyawan,
            month_filter=month,
            year_filter=year,
            loan_type=loan_type
        )
        
        
        # Return structured response
        return {
            "status": "success",
            "total_expected_repayment": repayment_risk_summary["total_expected_repayment"],
            "total_loan_principal_collected": repayment_risk_summary["total_loan_principal_collected"],
            "total_admin_fee_collected": repayment_risk_summary["total_admin_fee_collected"],
            "total_unrecovered_repayment": repayment_risk_summary["total_unrecovered_repayment"],
            "total_unrecovered_loan_principal": repayment_risk_summary["total_unrecovered_loan_principal"],
            "total_unrecovered_admin_fee": repayment_risk_summary["total_unrecovered_admin_fee"],
            "repayment_recovery_rate": repayment_risk_summary["repayment_recovery_rate"],
            "delinquencies_rate": repayment_risk_summary["delinquencies_rate"],
            "admin_fee_profit": repayment_risk_summary["admin_fee_profit"]
        }
    except Exception as e:
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "total_expected_repayment": 0,
            "total_loan_principal_collected": 0,
            "total_admin_fee_collected": 0,
            "total_unrecovered_repayment": 0,
            "total_unrecovered_loan_principal": 0,
            "total_unrecovered_admin_fee": 0,
            "repayment_recovery_rate": 0,
            "delinquencies_rate": 0,
            "admin_fee_profit": 0
        }


@router.get("/repayment-risk-monthly", response_model=schemas.RepaymentRiskMonthlyResponse)
async def get_repayment_risk_monthly(
    start_date: str,
    end_date: str,
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None,
    loan_status: int = None,
    id_karyawan: int = None,
    loan_type: str = "loan",
    db: Session = Depends(get_db)
):
    """Get repayment risk summary separated by months within a date range
    
    Required parameters:
    - start_date: Start date in YYYY-MM-DD format (e.g., "2024-01-01")
    - end_date: End date in YYYY-MM-DD format (e.g., "2024-12-31")
    """
    
    try:
        monthly_repayment_risk_summary = crud.get_repayment_risk_monthly_summary(
            db, 
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project,
            loan_status_filter=loan_status,
            id_karyawan_filter=id_karyawan,
            start_date=start_date,
            end_date=end_date,
            loan_type=loan_type
        )
        
        
        # Return structured response
        return {
            "status": "success",
            "monthly_data": monthly_repayment_risk_summary
        }
    except Exception as e:
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "monthly_data": {}
        }


@router.get("/coverage-utilization", response_model=schemas.CoverageUtilizationResponse)
async def get_coverage_utilization(
    month: int = None,
    year: int = None,
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None,
    loan_status: int = None,
    id_karyawan: int = None,
    loan_type: str = "loan",
    db: Session = Depends(get_db)
):
    """Get comprehensive coverage and utilization summary combining multiple metrics"""
    
    try:
        coverage_utilization_summary = crud.get_coverage_utilization_summary(
            db, 
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project,
            loan_status_filter=loan_status,
            id_karyawan_filter=id_karyawan,
            month_filter=month,
            year_filter=year,
            loan_type=loan_type
        )
        
        
        # Return structured response
        return {
            "status": "success",
            "total_eligible_employees": coverage_utilization_summary["total_eligible_employees"],
            "total_active_employees": coverage_utilization_summary["total_active_employees"],
            "total_loan_requests": coverage_utilization_summary["total_loan_requests"],
            "penetration_rate": coverage_utilization_summary["penetration_rate"],
            "eligible_rate": coverage_utilization_summary["eligible_rate"],
            "total_approved_requests": coverage_utilization_summary["total_approved_requests"],
            "total_rejected_requests": coverage_utilization_summary["total_rejected_requests"],
            "approval_rate": coverage_utilization_summary["approval_rate"],
            "total_new_borrowers": coverage_utilization_summary["total_new_borrowers"],
            "average_approval_time": coverage_utilization_summary["average_approval_time"],
            "total_disbursed_amount": coverage_utilization_summary["total_disbursed_amount"],
            "average_disbursed_amount": coverage_utilization_summary["average_disbursed_amount"]
        }
    except Exception as e:
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "total_eligible_employees": 0,
            "total_active_employees": 0,
            "total_loan_requests": 0,
            "penetration_rate": 0,
            "eligible_rate": 0.0,
            "total_approved_requests": 0,
            "total_rejected_requests": 0,
            "approval_rate": 0,
            "total_new_borrowers": 0,
            "average_approval_time": 0,
            "total_disbursed_amount": 0,
            "average_disbursed_amount": 0
        }


@router.get("/coverage-utilization-monthly", response_model=schemas.CoverageUtilizationMonthlyResponse)
async def get_coverage_utilization_monthly(
    start_date: str,
    end_date: str,
    employer: str = None, 
    sourced_to: str = None, 
    project: str = None,
    loan_status: int = None,
    id_karyawan: int = None,
    loan_type: str = "loan",
    db: Session = Depends(get_db)
):
    """Get coverage utilization summary separated by months within a date range
    
    Required parameters:
    - start_date: Start date in YYYY-MM-DD format (e.g., "2024-01-01")
    - end_date: End date in YYYY-MM-DD format (e.g., "2024-12-31")
    """
    
    try:
        monthly_coverage_utilization_summary = crud.get_coverage_utilization_monthly_summary(
            db, 
            employer_filter=employer,
            sourced_to_filter=sourced_to,
            project_filter=project,
            loan_status_filter=loan_status,
            id_karyawan_filter=id_karyawan,
            start_date=start_date,
            end_date=end_date,
            loan_type=loan_type
        )
        
        
        # Return structured response
        return {
            "status": "success",
            "monthly_data": monthly_coverage_utilization_summary
        }
    except Exception as e:
        # Return error response with status
        return {
            "status": "error",
            "message": str(e),
            "monthly_data": {}
        }