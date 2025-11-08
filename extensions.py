# extensions.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import os

DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)

# CRITICAL: Import psycopg dialect directly, bypass psycopg2
from sqlalchemy.dialects.postgresql import psycopg

# Create engine with explicit dialect
engine = create_engine(
    DATABASE_URL,
    connect_args={'sslmode': 'require'},
    dialect=psycopg  # <-- THIS IS THE KEY
)

SessionLocal = scoped_session(sessionmaker(bind=engine))

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
