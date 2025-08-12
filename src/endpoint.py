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
            "kasbon_karyawan": "/kasbon/karyawan (includes: employer_name, sourced_to_name, project_name)",
            "kasbon_karyawan_filtered": "/kasbon/karyawan?employer=EMPLOYER&sourced_to=PLACEMENT&project=PROJECT&id_karyawan=123",
            "kasbon_eligible_count": "/kasbon/eligible-count (count of employees with status='1' and loan_kasbon_eligible='1')",
            "kasbon_eligible_count_filtered": "/kasbon/eligible-count?employer=EMPLOYER&project=PROJECT&id_karyawan=123",
            "kasbon_loans": "/kasbon/loans (loan data with enhanced karyawan information)",
            "kasbon_loans_filtered": "/kasbon/loans?employer=EMPLOYER&project=PROJECT&loan_status=1&id_karyawan=123",
            "kasbon_loan_fees": "/kasbon/loan-fees (total expected and collected admin fees)",
            "kasbon_loan_fees_filtered": "/kasbon/loan-fees?employer=EMPLOYER&project=PROJECT&loan_status=1&id_karyawan=123",
            "kasbon_filters": "/kasbon/filters (get available filter values)",
            "health": "/health"
        },
        "usage": {
            "get_kasbon_karyawan": "GET /kasbon/karyawan",
            "filter_kasbon_karyawan": "GET /kasbon/karyawan?employer=Employer A&sourced_to=Placement X&project=Project 1&id_karyawan=123",
            "get_kasbon_eligible_count": "GET /kasbon/eligible-count",
            "filter_kasbon_eligible_count": "GET /kasbon/eligible-count?employer=Employer A&project=Project 1&id_karyawan=123",
            "get_kasbon_loans": "GET /kasbon/loans",
            "filter_kasbon_loans": "GET /kasbon/loans?employer=Employer A&project=Project 1&loan_status=1&id_karyawan=123",
            "get_kasbon_loan_fees": "GET /kasbon/loan-fees",
            "filter_kasbon_loan_fees": "GET /kasbon/loan-fees?employer=Employer A&project=Project 1&loan_status=1&id_karyawan=123"
        }
    }
