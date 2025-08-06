from pydantic import BaseModel
from typing import List, Optional


class TdKaryawanResponse(BaseModel):
    id_karyawan: int
    status: Optional[str] = None
    loan_kasbon_eligible: Optional[int] = None

    class Config:
        from_attributes = True


