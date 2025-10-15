from typing import List
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

# Flexible imports that work both locally and in Docker
try:
    # Try relative imports first (for Docker)
    from .db import get_db
except ImportError:
    # Fall back to absolute imports (for local development)
    from db import get_db


router = APIRouter()


@router.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    return {"status": "TEST", "service": "akumaju-api"}


@router.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "service": "Aku Maju API",
        "version": "1.0.0",
        "endpoints": {
            "loan_karyawan": "/loan/karyawan (includes: employer_name, sourced_to_name, project_name)",
            "loan_karyawan_filtered": "/loan/karyawan?employer=EMPLOYER&sourced_to=PLACEMENT&project=PROJECT&id_karyawan=123",
            "loan_eligible_count": "/loan/eligible-count (count of employees with status='1' and loan_eligible='1')",
            "loan_eligible_count_filtered": "/loan/eligible-count?employer=EMPLOYER&project=PROJECT&id_karyawan=123",
            "loan_loans": "/loan/loans (loan data with enhanced karyawan information)",
            "loan_loans_filtered": "/loan/loans?employer=EMPLOYER&project=PROJECT&loan_status=1&id_karyawan=123",
            "loan_loan_fees": "/loan/loan-fees (total expected and collected admin fees)",
            "loan_loan_fees_filtered": "/loan/loan-fees?employer=EMPLOYER&project=PROJECT&loan_status=1&id_karyawan=123",
            "loan_filters": "/loan/filters (get available filter values)",
            "health": "/health"
        },
        "usage": {
            "get_loan_karyawan": "GET /loan/karyawan",
            "filter_loan_karyawan": "GET /loan/karyawan?employer=Employer A&sourced_to=Placement X&project=Project 1&id_karyawan=123",
            "get_loan_eligible_count": "GET /loan/eligible-count",
            "filter_loan_eligible_count": "GET /loan/eligible-count?employer=Employer A&project=Project 1&id_karyawan=123",
            "get_loan_loans": "GET /loan/loans",
            "filter_loan_loans": "GET /loan/loans?employer=Employer A&project=Project 1&loan_status=1&id_karyawan=123",
            "get_loan_loan_fees": "GET /loan/loan-fees",
            "filter_loan_loan_fees": "GET /loan/loan-fees?employer=Employer A&project=Project 1&loan_status=1&id_karyawan=123"
        }
    }
