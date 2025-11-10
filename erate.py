# erate.py
from flask import (
    Blueprint, render_template, request, session, redirect, url_for,
    send_file, flash, current_app
)
import csv
import os
import logging
import requests
import threading
import time
import psycopg
from datetime import datetime

# === LOGGING (ONE LINE TO RENDER + import.log) ===
LOG_FILE = os.path.join(os.path.dirname(__file__), "import.log")
open(LOG_FILE, 'a').close()

handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))

logger = logging.getLogger('erate')
logger.setLevel(logging.INFO)
for h in logger.handlers[:]: logger.removeHandler(h)
logger.addHandler(handler)

def log(msg, *args):
    formatted = msg % args if args else msg
    logger.info(formatted)
    handler.flush()
    print(formatted, flush=True)

# === BLUEPRINT ===
erate_bp = Blueprint('erate', __name__, url_prefix='/erate', template_folder='templates')
CSV_FILE = os.path.join(os.path.dirname(__file__), "470schema.csv")

# === GET DATABASE_URL FROM ENV ===
DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
if 'sslmode' not in DATABASE_URL.lower():
    base, dbname = DATABASE_URL.rsplit('/', 1)
    DATABASE_URL = f"{base}/{dbname}?sslmode=require"

# === SQL INSERT ===
INSERT_SQL = '''
    INSERT INTO erate (
        app_number, form_nickname, form_pdf, funding_year, fcc_status,
        allowable_contract_date, created_datetime, created_by,
        certified_datetime, certified_by, last_modified_datetime, last_modified_by,
        ben, entity_name, org_status, org_type, applicant_type, website,
        latitude, longitude, fcc_reg_num, address1, address2, city, state,
        zip_code, zip_ext, email, phone, phone_ext, num_eligible,
        contact_name, contact_address1, contact_address2, contact_city,
        contact_state, contact_zip, contact_zip_ext, contact_phone,
        contact_phone_ext, contact_email, tech_name, tech_title,
        tech_phone, tech_phone_ext, tech_email, auth_name, auth_address,
        auth_city, auth_state, auth_zip, auth_zip_ext, auth_phone,
        auth_phone_ext, auth_email, auth_title, auth_employer,
        cat1_desc, cat2_desc, installment_type, installment_min,
        installment_max, rfp_id, state_restrictions, restriction_desc,
        statewide, all_public, all_nonpublic, all_libraries, form_version
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
'''

# === TIME PARSING ===
def parse_datetime(value):
    if not value or not str(value).strip():
        return None
    value = str(value).strip()
    formats = [
        "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M",
        "%m/%d/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"
    ]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None

def _row_to_tuple(row):
    return (
        row.get('Application Number', '').strip(),
        row.get('Form Nickname', ''),
        row.get('Form PDF', ''),
        row.get('Funding Year', ''),
        row.get('FCC Form 470 Status', ''),
        parse_datetime(row.get('Allowable Contract Date')),
        parse_datetime(row.get('Created Date/Time')),
        row.get('Created By', ''),
        parse_datetime(row.get('Certified Date/Time')),
        row.get('Certified By', ''),
        parse_datetime(row.get('Last Modified Date/Time')),
        row.get('Last Modified By', ''),
        row.get('Billed Entity Number', ''),
        row.get('Billed Entity Name', ''),
        row.get('Organization Status', ''),
        row.get('Organization Type', ''),
        row(the 'Applicant Type', ''),
        row.get('Website URL', ''),
        float(row.get('Latitude') or 0),
        float(row.get('Longitude') or 0),
        row.get('Billed Entity FCC Registration Number', ''),
        row.get('Billed Entity Address 1', ''),
        row.get('Billed Entity Address 2', ''),
        row.get('Billed Entity City', ''),
        row.get('Billed Entity State', ''),
        row.get('Billed Entity Zip Code', ''),
        row.get('Billed Entity Zip Code Ext', ''),
        row.get('Billed Entity Email', ''),
        row.get('Billed Entity Phone', ''),
        row.get('Billed Entity Phone Ext', ''),
        int(row.get('Number of Eligible Entities') or 0),
        row.get('Contact Name', ''),
        row.get('Contact Address 1', ''),
        row.get('Contact Address 2', ''),
        row.get('Contact City', ''),
        row.get('Contact State', ''),
        row.get('Contact Zip', ''),
        row.get('Contact Zip Ext', ''),
        row.get('Contact Phone', ''),
        row.get('Contact Phone Ext', ''),
        row.get('Contact Email', ''),
        row.get('Technical Contact Name', ''),
        row.get('Technical Contact Title', ''),
        row.get('Technical Contact Phone', ''),
        row.get('Technical Contact Phone Ext', ''),
        row.get('Technical Contact Email', ''),
        row.get('Authorized Person Name', ''),
        row.get('Authorized Person Address', ''),
        row.get('Authorized Person City', ''),
        row.get('Authorized Person State', ''),
        row.get('Authorized Person Zip', ''),
        row.get('Authorized Person Zip Ext', ''),
        row.get('Authorized Person Phone Number', ''),
        row.get('Authorized Person Phone Number Ext', ''),
        row.get('Authorized Person Email', ''),
        row.get('Authorized Person Title', ''),
        row.get('Authorized Person Employer', ''),
        row.get('Category One Description', ''),
        row.get('Category Two Description', ''),
        row.get('Installment Type', ''),
        int(row.get('Installment Min Range Years') or 0),
        int(row.get('Installment Max Range Years') or 0),
        row.get('Request for Proposal Identifier', ''),
        row.get('State or Local Restrictions', ''),
        row.get('State or Local Restrictions Description', ''),
        row.get('Statewide State', ''),
        row.get('All Public Schools Districts', ''),
        row.get('All Non-Public schools', ''),
        row.get('All Libraries', ''),
        row.get('Form Version', '')
    )

# === BULK IMPORT — USE psycopg.connect() DIRECTLY ===
def _import_all_background(app, progress):
    time.sleep(1)
    batch_size = 1000
    try:
        log("Bulk import started from record %s", progress['index'])
        with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [n.strip().lstrip('\ufeff') for n in reader.fieldnames]
            for _ in range(progress['index'] - 1): next(reader)

            batch = []
            imported = 0

            for row in reader:
                app_number = row.get('Application Number', '').strip()
                if not app_number: continue

                # CONNECT DIRECTLY — NO g
                conn = psycopg.connect(DATABASE_URL)
                try:
                    with conn.cursor() as cur:
                        cur.execute('SELECT 1 FROM erate WHERE app_number = %s', (app_number,))
                        if cur.fetchone():
                            conn.close()
                            continue
                except Exception as e:
                    log("DB check failed: %s", e)
                    conn.close()
                    continue

                batch.append(row)
                imported += 1
                conn.close()

                if len(batch) >= batch_size:
                    conn = psycopg.connect(DATABASE_URL)
                    try:
                        with conn.cursor() as cur:
                            for r in batch:
                                cur.execute(INSERT_SQL, _row_to_tuple(r))
                        conn.commit()
                    except Exception as e:
                        log("Batch commit failed: %s", e)
                        conn.rollback()
                    finally:
                        conn.close()
                    progress['index'] += batch_size
                    progress['success'] += batch_size
                    log("Imported: %s", progress['index'] - 1)
                    batch = []

            if batch:
                conn = psycopg.connect(DATABASE_URL)
                try:
                    with conn.cursor() as cur:
                        for r in batch:
                            cur.execute(INSERT_SQL, _row_to_tuple(r))
                    conn.commit()
                except Exception as e:
                    log("Final batch failed: %s", e)
                finally:
                    conn.close()
                progress['index'] = progress['total'] + 1
                progress['success'] += len(batch)

            log("Bulk import complete: %s imported", progress['success'])

    except Exception as e:
        log("Import failed: %s", e)
    finally:
        with app.app_context():
            app.config.update({
                'BULK_IMPORT_IN_PROGRESS': False,
                'IMPORT_THREAD': None
            })
