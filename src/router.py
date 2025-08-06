from fastapi import APIRouter

# Flexible imports that work both locally and in Docker
try:
    # Try relative imports first (for Docker)
    from . import endpoint
except ImportError:
    # Fall back to absolute imports (for local development)
    import endpoint


router = APIRouter()

router.include_router(endpoint.router)
