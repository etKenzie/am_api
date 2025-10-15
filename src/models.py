# Database models for SQLAlchemy
# This file is only used for database table creation
# AI routes don't need database models

from sqlalchemy.ext.declarative import declarative_base

# Create the base class for models
Base = declarative_base()

# Note: We don't define actual models here since we're using raw SQL queries
# in the loan module. This file exists only to satisfy the import in main.py
