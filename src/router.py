from fastapi import APIRouter

# Flexible imports that work both locally and in Docker
try:
    # Try relative imports first (for Docker)
    from . import endpoint
    from .kasbon.router import router as kasbon_router
    from .ai.router import router as ai_router
except ImportError:
    # Fall back to absolute imports (for local development)
    import endpoint
    from kasbon.router import router as kasbon_router
    from ai.router import router as ai_router


router = APIRouter()

# Include the main endpoint router
router.include_router(endpoint.router)

# Include the kasbon router
router.include_router(kasbon_router)

# Include the AI router
router.include_router(ai_router)
