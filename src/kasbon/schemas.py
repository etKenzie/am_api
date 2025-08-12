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


class EligibleCountResponse(BaseModel):
    status: str
    total_eligible_employees: int
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