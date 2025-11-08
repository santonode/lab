# db.py
from psycopg import connect
import os

DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

def get_conn():
    return connect(DATABASE_URL, sslmode='require')

def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS erate (
                    app_number TEXT PRIMARY KEY,
                    form_nickname TEXT,
                    form_pdf TEXT,
                    funding_year TEXT,
                    fcc_status TEXT,
                    allowable_contract_date TIMESTAMP,
                    created_datetime TIMESTAMP,
                    created_by TEXT,
                    certified_datetime TIMESTAMP,
                    certified_by TEXT,
                    last_modified_datetime TIMESTAMP,
                    last_modified_by TEXT,
                    ben TEXT,
                    entity_name TEXT,
                    org_status TEXT,
                    org_type TEXT,
                    applicant_type TEXT,
                    website TEXT,
                    latitude FLOAT,
                    longitude FLOAT,
                    fcc_reg_num TEXT,
                    address1 TEXT,
                    address2 TEXT,
                    city TEXT,
                    state TEXT,
                    zip_code TEXT,
                    zip_ext TEXT,
                    email TEXT,
                    phone TEXT,
                    phone_ext TEXT,
                    num_eligible INTEGER,
                    contact_name TEXT,
                    contact_address1 TEXT,
                    contact_address2 TEXT,
                    contact_city TEXT,
                    contact_state TEXT,
                    contact_zip TEXT,
                    contact_zip_ext TEXT,
                    contact_phone TEXT,
                    contact_phone_ext TEXT,
                    contact_email TEXT,
                    tech_name TEXT,
                    tech_title TEXT,
                    tech_phone TEXT,
                    tech_phone_ext TEXT,
                    tech_email TEXT,
                    auth_name TEXT,
                    auth_address TEXT,
                    auth_city TEXT,
                    auth_state TEXT,
                    auth_zip TEXT,
                    auth_zip_ext TEXT,
                    auth_phone TEXT,
                    auth_phone_ext TEXT,
                    auth_email TEXT,
                    auth_title TEXT,
                    auth_employer TEXT,
                    cat1_desc TEXT,
                    cat2_desc TEXT,
                    installment_type TEXT,
                    installment_min INTEGER,
                    installment_max INTEGER,
                    rfp_id TEXT,
                    state_restrictions TEXT,
                    restriction_desc TEXT,
                    statewide TEXT,
                    all_public TEXT,
                    all_nonpublic TEXT,
                    all_libraries TEXT,
                    form_version TEXT
                )
            ''')
            conn.commit()
