from pydantic import BaseModel
from typing import List, Optional


class TdKaryawanResponse(BaseModel):
    id_karyawan: int
    status: Optional[str] = None
    loan_kasbon_eligible: Optional[int] = None
    klient: Optional[str] = None

    class Config:
        from_attributes = True


class KaryawanListResponse(BaseModel):
    status: str
    count: int
    results: List[TdKaryawanResponse]
    message: Optional[str] = None

    class Config:
        from_attributes = True


