from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Flexible imports that work both locally and in Docker
try:
    # Try relative imports first (for Docker)
    from .router import router as process_router
    from .db import get_engine
except ImportError:
    # Fall back to absolute imports (for local development)
    from router import router as process_router
    from db import get_engine

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

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",      # React default port
        "http://localhost:3001",      # Alternative React port
        "http://localhost:8080",      # Vue default port
        "http://localhost:4200",      # Angular default port
        "http://127.0.0.1:3000",     # Alternative localhost
        "http://127.0.0.1:3001",
        "http://127.0.0.1:8080",
        "http://127.0.0.1:4200",
        "*"                           # Allow all origins (for development - remove in production)
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

app.include_router(process_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


# 
