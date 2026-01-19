from fastapi import APIRouter

# Flexible imports that work both locally and in Docker
try:
    # Try relative imports first (for Docker)
    from . import endpoint
    from .loan.router import router as loan_router
    from .ai.router import router as ai_router
    from .external_payroll.router import router as external_payroll_router
    from .internal_payroll.router import router as internal_payroll_router
except ImportError:
    # Fall back to absolute imports (for local development)
    import endpoint
    from loan.router import router as loan_router
    from ai.router import router as ai_router
    from external_payroll.router import router as external_payroll_router
    from internal_payroll.router import router as internal_payroll_router


router = APIRouter()

# Include the main endpoint router
router.include_router(endpoint.router)

# Include the loan router
router.include_router(loan_router)

# Include the AI router
router.include_router(ai_router)

# Include the external payroll router
router.include_router(external_payroll_router)

# Include the internal payroll router
router.include_router(internal_payroll_router)
