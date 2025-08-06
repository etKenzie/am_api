from fastapi import FastAPI

# Flexible imports that work both locally and in Docker
try:
    # Try relative imports first (for Docker)
    from .router import router as process_router
    from .db import get_engine
    from . import models
except ImportError:
    # Fall back to absolute imports (for local development)
    from router import router as process_router
    from db import get_engine
    import models

# Create database tables with error handling
try:
    engine = get_engine()
    models.Base.metadata.create_all(bind=engine)
    print("Database tables created successfully")
except Exception as e:
    print(f"Warning: Could not create database tables: {e}")

app = FastAPI(
    title="Aku Maju API",
    description="API for karyawan management",
    version="1.0.0"
)

app.include_router(process_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


# 
