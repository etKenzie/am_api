from pydantic import BaseModel
from typing import Optional, List, Dict


class TotalPayrollDisbursedResponse(BaseModel):
    """Response model for total payroll disbursed"""
    status: str
    total_payroll_disbursed: float
    month: Optional[int] = None
    year: Optional[int] = None
    dept_id: Optional[int] = None
    message: Optional[str] = None

    class Config:
        from_attributes = True


class TotalPayrollHeadcountResponse(BaseModel):
    """Response model for total payroll headcount with breakdown by status_kontrak"""
    status: str
    total_headcount: int
    pkwtt_headcount: int
    pkwt_headcount: int
    mitra_headcount: int
    month: Optional[int] = None
    year: Optional[int] = None
    dept_id: Optional[int] = None
    message: Optional[str] = None

    class Config:
        from_attributes = True


class TotalDepartmentCountResponse(BaseModel):
    """Response model for total department count"""
    status: str
    total_department_count: int
    month: Optional[int] = None
    year: Optional[int] = None
    message: Optional[str] = None

    class Config:
        from_attributes = True


class TotalBpsjtkResponse(BaseModel):
    """Response model for total BPJS TK"""
    status: str
    total_bpsjtk: float
    month: Optional[int] = None
    year: Optional[int] = None
    dept_id: Optional[int] = None
    message: Optional[str] = None

    class Config:
        from_attributes = True


class TotalKesehatanResponse(BaseModel):
    """Response model for total BPJS Kesehatan"""
    status: str
    total_kesehatan: float
    month: Optional[int] = None
    year: Optional[int] = None
    dept_id: Optional[int] = None
    message: Optional[str] = None

    class Config:
        from_attributes = True


class TotalPensiunResponse(BaseModel):
    """Response model for total BPJS Pensiun"""
    status: str
    total_pensiun: float
    month: Optional[int] = None
    year: Optional[int] = None
    dept_id: Optional[int] = None
    message: Optional[str] = None

    class Config:
        from_attributes = True


class DepartmentFilterItem(BaseModel):
    """Department filter item with dept_id and department_name"""
    dept_id: Optional[int] = None
    department_name: Optional[str] = None

    class Config:
        from_attributes = True


class DepartmentFiltersResponse(BaseModel):
    """Response model for department filters"""
    status: str
    departments: List[DepartmentFilterItem]
    month: Optional[int] = None
    year: Optional[int] = None
    count: int
    message: Optional[str] = None

    class Config:
        from_attributes = True


class MonthlySummaryItem(BaseModel):
    """Monthly summary item with total_disbursed and headcount breakdown"""
    total_disbursed: float
    total_headcount: int
    pkwtt_headcount: int
    pkwt_headcount: int
    mitra_headcount: int

    class Config:
        from_attributes = True


class MonthlyPayrollSummaryResponse(BaseModel):
    """Response model for monthly payroll summary"""
    status: str
    summaries: Dict[str, MonthlySummaryItem]
    start_month: str
    end_month: str
    dept_id: Optional[int] = None
    message: Optional[str] = None

    class Config:
        from_attributes = True


class DepartmentSummaryItem(BaseModel):
    """Department summary item with all metrics"""
    dept_id: Optional[int] = None
    department_name: Optional[str] = None
    cost_owner: Optional[str] = None
    total_headcount: int
    pkwtt_headcount: int
    pkwt_headcount: int
    mitra_headcount: int
    distribution_ratio: float
    total_disbursed: float

    class Config:
        from_attributes = True


class DepartmentSummaryResponse(BaseModel):
    """Response model for department summary"""
    status: str
    departments: List[DepartmentSummaryItem]
    month: Optional[int] = None
    year: Optional[int] = None
    count: int
    message: Optional[str] = None

    class Config:
        from_attributes = True


class CostOwnerSummaryItem(BaseModel):
    """Cost owner summary item with all metrics"""
    cost_owner: Optional[str] = None
    total_headcount: int
    pkwtt_headcount: int
    pkwt_headcount: int
    mitra_headcount: int
    distribution_ratio: float
    total_disbursed: float

    class Config:
        from_attributes = True


class CostOwnerSummaryResponse(BaseModel):
    """Response model for cost owner summary"""
    status: str
    cost_owners: List[CostOwnerSummaryItem]
    month: Optional[int] = None
    year: Optional[int] = None
    count: int
    message: Optional[str] = None

    class Config:
        from_attributes = True

