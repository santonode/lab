# db.py — MODERN & RECOMMENDED
import os
import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/wurdle_db')

def get_conn():
    """Return a connection with dict-like rows"""
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

def init_db():
    """Create tables if they don't exist"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Users table
            cur.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(12) UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')

            # Memes table
            cur.execute('''
                CREATE TABLE IF NOT EXISTS memes (
                    meme_id SERIAL PRIMARY KEY,
                    type VARCHAR(20),
                    meme_description TEXT,
                    meme_download_counts INTEGER DEFAULT 0,
                    owner INTEGER REFERENCES users(id) ON DELETE SET NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')

            # E-Rate table (from your erate.py)
            cur.execute('''
                CREATE TABLE IF NOT EXISTS erate (
                    app_number VARCHAR(20) PRIMARY KEY,
                    form_nickname VARCHAR(255),
                    form_pdf TEXT,
                    funding_year VARCHAR(10),
                    fcc_status VARCHAR(50),
                    allowable_contract_date DATE,
                    created_datetime TIMESTAMP,
                    created_by VARCHAR(100),
                    certified_datetime TIMESTAMP,
                    certified_by VARCHAR(100),
                    last_modified_datetime TIMESTAMP,
                    last_modified_by VARCHAR(100),
                    ben VARCHAR(20),
                    entity_name TEXT,
                    org_status VARCHAR(50),
                    org_type VARCHAR(50),
                    applicant_type VARCHAR(50),
                    website TEXT,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    fcc_reg_num VARCHAR(20),
                    address1 TEXT,
                    address2 TEXT,
                    city VARCHAR(100),
                    state VARCHAR(2),
                    zip_code VARCHAR(10),
                    zip_ext VARCHAR(10),
                    email VARCHAR(255),
                    phone VARCHAR(20),
                    phone_ext VARCHAR(10),
                    num_eligible INTEGER,
                    contact_name VARCHAR(100),
                    contact_address1 TEXT,
                    contact_address2 TEXT,
                    contact_city VARCHAR(100),
                    contact_state VARCHAR(2),
                    contact_zip VARCHAR(10),
                    contact_zip_ext VARCHAR(10),
                    contact_phone VARCHAR(20),
                    contact_phone_ext VARCHAR(10),
                    contact_email VARCHAR(255),
                    tech_name VARCHAR(100),
                    tech_title VARCHAR(100),
                    tech_phone VARCHAR(20),
                    tech_phone_ext VARCHAR(10),
                    tech_email VARCHAR(255),
                    auth_name VARCHAR(100),
                    auth_address TEXT,
                    auth_city VARCHAR(100),
                    auth_state VARCHAR(2),
                    auth_zip VARCHAR(10),
                    auth_zip_ext VARCHAR(10),
                    auth_phone VARCHAR(20),
                    auth_phone_ext VARCHAR(10),
                    auth_email VARCHAR(255),
                    auth_title VARCHAR(100),
                    auth_employer VARCHAR(255),
                    cat1_desc TEXT,
                    cat2_desc TEXT,
                    installment_type VARCHAR(50),
                    installment_min INTEGER,
                    installment_max INTEGER,
                    rfp_id VARCHAR(100),
                    state_restrictions VARCHAR(10),
                    restriction_desc TEXT,
                    statewide VARCHAR(10),
                    all_public VARCHAR(10),
                    all_nonpublic VARCHAR(10),
                    all_libraries VARCHAR(10),
                    form_version VARCHAR(50)
                )
            ''')

        conn.commit()
        print("DB initialized: users, memes, erate")
