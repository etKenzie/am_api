from pydantic import BaseModel
from typing import List, Optional


class TdKaryawanEnhancedResponse(BaseModel):
    """Enhanced response with joined data from tbl_gmc"""
    id_karyawan: int
    status: Optional[str] = None
    loan_kasbon_eligible: Optional[int] = None
    klient: Optional[str] = None
    employer_name: Optional[str] = None
    sourced_to_name: Optional[str] = None
    project_name: Optional[str] = None

    class Config:
        from_attributes = True


class KaryawanEnhancedListResponse(BaseModel):
    status: str
    count: int
    results: List[TdKaryawanEnhancedResponse]
    message: Optional[str] = None

    class Config:
        from_attributes = True


class SummaryResponse(BaseModel):
    status: str
    total_eligible_employees: int
    total_processed_kasbon_requests: int
    total_pending_kasbon_requests: int
    total_first_borrow: int
    total_approved_requests: int
    total_rejected_requests: int
    total_disbursed_amount: int
    total_loans: int
    average_disbursed_amount: float
    approval_rate: float
    average_approval_time: float
    penetration_rate: float
    message: Optional[str] = None

    class Config:
        from_attributes = True


class UserCoverageResponse(BaseModel):
    status: str
    total_eligible_employees: int
    total_kasbon_requests: int
    penetration_rate: float
    total_first_borrow: int
    message: Optional[str] = None

    class Config:
        from_attributes = True


class RequestsResponse(BaseModel):
    status: str
    total_approved_requests: int
    total_rejected_requests: int
    approval_rate: float
    average_approval_time: float
    message: Optional[str] = None

    class Config:
        from_attributes = True


class DisbursementResponse(BaseModel):
    status: str
    total_disbursed_amount: int
    average_disbursed_amount: float
    message: Optional[str] = None

    class Config:
        from_attributes = True


class MonthlyUserCoverageData(BaseModel):
    total_eligible_employees: int
    total_kasbon_requests: int
    total_first_borrow: int
    total_approved_requests: int
    total_disbursed_amount: int
    penetration_rate: float

    class Config:
        from_attributes = True


class UserCoverageMonthlyResponse(BaseModel):
    status: str
    monthly_data: dict[str, MonthlyUserCoverageData]
    message: Optional[str] = None

    class Config:
        from_attributes = True


class MonthlyDisbursementData(BaseModel):
    total_disbursed_amount: int
    total_loans: int
    average_disbursed_amount: float

    class Config:
        from_attributes = True


class DisbursementMonthlyResponse(BaseModel):
    status: str
    monthly_data: dict[str, MonthlyDisbursementData]
    message: Optional[str] = None

    class Config:
        from_attributes = True


class MonthlySummaryData(BaseModel):
    total_eligible_employees: int
    total_processed_kasbon_requests: int
    total_disbursed_amount: int
    penetration_rate: float

    class Config:
        from_attributes = True


class SummaryMonthlyResponse(BaseModel):
    status: str
    monthly_data: dict[str, MonthlySummaryData]
    message: Optional[str] = None

    class Config:
        from_attributes = True


class LoanResponse(BaseModel):
    """Response model for loan data with enhanced karyawan information"""
    id: int
    id_karyawan: int
    loan_id: int
    purpose: int
    duration: int
    total_loan: int
    admin_fee: int
    total_payment: int
    repayment_date: Optional[str] = None
    received_date: Optional[str] = None
    send_date: Optional[str] = None
    loan_status: int
    user_process: Optional[int] = None
    process_date: Optional[str] = None
    payment_date: Optional[str] = None
    disbursement: Optional[int] = None
    ref_number_transaction: Optional[str] = None
    is_non_approved: int
    # Enhanced karyawan data
    employer_name: Optional[str] = None
    sourced_to_name: Optional[str] = None
    project_name: Optional[str] = None

    class Config:
        from_attributes = True


class LoanListResponse(BaseModel):
    status: str
    count: int
    results: List[LoanResponse]
    message: Optional[str] = None

    class Config:
        from_attributes = True


class LoanFeesResponse(BaseModel):
    status: str
    total_expected_admin_fee: int
    expected_loans_count: int
    total_collected_admin_fee: int
    collected_loans_count: int
    total_failed_payment: int
    admin_fee_profit: int
    message: Optional[str] = None

    class Config:
        from_attributes = True


class MonthlyLoanFeesData(BaseModel):
    total_expected_admin_fee: int
    expected_loans_count: int
    total_collected_admin_fee: int
    collected_loans_count: int
    total_failed_payment: int
    admin_fee_profit: int

    class Config:
        from_attributes = True


class LoanFeesMonthlyResponse(BaseModel):
    status: str
    monthly_data: dict[str, MonthlyLoanFeesData]
    message: Optional[str] = None

    class Config:
        from_attributes = True


class LoanRiskResponse(BaseModel):
    status: str
    total_unrecovered_kasbon: int
    unrecovered_kasbon_count: int
    total_expected_repayment: int
    kasbon_principal_recovery_rate: float
    message: Optional[str] = None

    class Config:
        from_attributes = True


class MonthlyLoanRiskData(BaseModel):
    total_unrecovered_kasbon: int
    unrecovered_kasbon_count: int
    total_expected_repayment: int
    kasbon_principal_recovery_rate: float

    class Config:
        from_attributes = True


class LoanRiskMonthlyResponse(BaseModel):
    status: str
    monthly_data: dict[str, MonthlyLoanRiskData]
    message: Optional[str] = None

    class Config:
        from_attributes = True


class KaryawanOverdueResponse(BaseModel):
    """Response model for karyawan with overdue loans"""
    id_karyawan: Optional[int] = None
    ktp: Optional[str] = None
    name: Optional[str] = None
    company: Optional[str] = None
    sourced_to: Optional[str] = None
    project: Optional[str] = None
    total_amount_owed: int
    repayment_date: Optional[str] = None
    days_overdue: int
    admin_fee: int
    total_payment: int

    class Config:
        from_attributes = True


class KaryawanOverdueListResponse(BaseModel):
    status: str
    count: int
    results: List[KaryawanOverdueResponse]
    message: Optional[str] = None

    class Config:
        from_attributes = True


class LoanPurposeSummaryResponse(BaseModel):
    """Response model for loan purpose summary data"""
    purpose_id: Optional[int] = None
    purpose_name: str
    total_count: int
    total_amount: int

    class Config:
        from_attributes = True


class LoanPurposeSummaryListResponse(BaseModel):
    status: str
    count: int
    results: List[LoanPurposeSummaryResponse]
    message: Optional[str] = None

    class Config:
        from_attributes = True 


class RepaymentRiskResponse(BaseModel):
    """Response model for repayment risk summary data"""
    status: str
    total_expected_repayment: int
    total_kasbon_principal_collected: int
    total_admin_fee_collected: int
    total_unrecovered_repayment: int
    total_unrecovered_kasbon_principal: int
    total_unrecovered_admin_fee: int
    repayment_recovery_rate: float
    delinquencies_rate: float
    admin_fee_profit: int
    message: Optional[str] = None

    class Config:
        from_attributes = True 


class MonthlyRepaymentRiskData(BaseModel):
    """Monthly data for repayment risk summary"""
    repayment_recovery_rate: float
    total_expected_repayment: int
    total_kasbon_principal_collected: int
    total_unrecovered_repayment: int
    admin_fee_profit: int

    class Config:
        from_attributes = True


class RepaymentRiskMonthlyResponse(BaseModel):
    """Response model for monthly repayment risk summary data"""
    status: str
    monthly_data: dict[str, MonthlyRepaymentRiskData]
    message: Optional[str] = None

    class Config:
        from_attributes = True 


class CoverageUtilizationResponse(BaseModel):
    """Response model for coverage utilization summary data"""
    status: str
    total_eligible_employees: int
    total_active_employees: int
    total_loan_requests: int
    penetration_rate: float
    eligible_rate: float
    total_approved_requests: int
    total_rejected_requests: int
    approval_rate: float
    total_new_borrowers: int
    average_approval_time: float
    total_disbursed_amount: int
    average_disbursed_amount: float
    message: Optional[str] = None

    class Config:
        from_attributes = True 


class MonthlyCoverageUtilizationData(BaseModel):
    """Monthly data for coverage utilization summary"""
    total_first_borrow: int
    total_loan_requests: int
    total_approved_requests: int
    total_rejected_requests: int
    penetration_rate: float
    total_disbursed_amount: int

    class Config:
        from_attributes = True


class CoverageUtilizationMonthlyResponse(BaseModel):
    """Response model for monthly coverage utilization summary data"""
    status: str
    monthly_data: dict[str, MonthlyCoverageUtilizationData]
    message: Optional[str] = None

    class Config:
        from_attributes = True 