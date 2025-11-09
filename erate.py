# erate.py
from flask import Blueprint, render_template, request, session, redirect, url_for
import csv
import os
import logging
from db import get_conn
from datetime import datetime

# === SETUP LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler("import.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')
CSV_FILE = os.path.join(os.path.dirname(__file__), "470schema.csv")

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

# === DASHBOARD WITH STATE + DATE FILTER + PAGINATION ===
@erate_bp.route('/')
def dashboard():
    state_filter = request.args.get('state', '').strip().upper()
    modified_after_str = request.args.get('modified_after', '').strip()
    offset = max(int(request.args.get('offset', 0)), 0)
    limit = 10
    filters = {
        'state': state_filter,
        'modified_after': modified_after_str
    }

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Count total
                count_sql = 'SELECT COUNT(*) AS total FROM erate'
                count_params = []
                where_clauses = []
                if state_filter:
                    where_clauses.append('state = %s')
                    count_params.append(state_filter)
                if modified_after_str:
                    where_clauses.append('last_modified_datetime >= %s')
                    count_params.append(modified_after_str)
                if where_clauses:
                    count_sql += ' WHERE ' + ' AND '.join(where_clauses)
                cur.execute(count_sql, count_params)
                total_count = cur.fetchone()['total']  # ← dict key

                # Fetch page
                sql = '''
                    SELECT
                        app_number, entity_name, state, funding_year,
                        fcc_status, last_modified_datetime
                    FROM erate
                '''
                params = []
                if where_clauses:
                    sql += ' WHERE ' + ' AND '.join(where_clauses)
                    params.extend(count_params)
                sql += ' ORDER BY app_number LIMIT %s OFFSET %s'
                params.extend([limit + 1, offset])
                cur.execute(sql, params)
                rows = cur.fetchall()

        has_more = len(rows) > limit
        table_data = rows[:limit]
        next_offset = offset + limit

        return render_template(
            'erate.html',
            table_data=table_data,
            filters=filters,
            total_count=total_count,
            total_filtered=offset + len(table_data),
            has_more=has_more,
            next_offset=next_offset
        )
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        return f"<pre>ERROR: {e}</pre>"

# === IMPORT INTERACTIVE ===
@erate_bp.route('/import-interactive', methods=['GET', 'POST'])
def import_interactive():
    if not os.path.exists(CSV_FILE):
        logger.error(f"CSV file not found: {CSV_FILE}")
        return "<h2>CSV not found: 470schema.csv</h2>", 404

    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        total = sum(1 for _ in f) - 1 or 1

    if 'import_progress' not in session:
        session['import_progress'] = {
            'index': 1,
            'total': total,
            'success': 0,
            'error': 0
        }

    progress = session['import_progress']

    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'reset':
            logger.info("Import reset by user")
            session.pop('import_progress', None)
            return redirect(url_for('erate.import_interactive'))
        if action == 'import_one':
            return _import_one_record()
        if action == 'import_all':
            return _import_all_records()

    if progress['index'] > progress['total']:
        logger.info(f"Import completed: {progress['success']} success, {progress['error']} errors")
        return render_template('erate_import.html', progress=progress)

    try:
        with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [n.strip().lstrip('\ufeff') for n in reader.fieldnames]
            for _ in range(progress['index'] - 1):
                next(reader)
            row = next(reader)
    except StopIteration:
        progress['index'] = progress['total'] + 1
        session['import_progress'] = progress
        return render_template('erate_import.html', progress=progress)

    return render_template('erate_import.html', row=row, progress=progress)

# === IMPORT ONE RECORD ===
def _import_one_record():
    progress = session['import_progress']
    try:
        with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [n.strip().lstrip('\ufeff') for n in reader.fieldnames]
            for _ in range(progress['index'] - 1):
                next(reader)
            row = next(reader)

        app_number = row.get('Application Number', '').strip()
        if not app_number:
            error_msg = "Missing Application Number"
            logger.warning(f"Record {progress['index']}: {error_msg} | Row: {row}")
            progress['error'] += 1
            progress['index'] += 1
            session['import_progress'] = progress
            return render_template('erate_import.html', row=row, progress=progress, error=error_msg)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT 1 FROM erate WHERE app_number = %s', (app_number,))
                if cur.fetchone():
                    error_msg = "Already exists"
                    logger.info(f"Record {progress['index']}: Skipped (duplicate) | App: {app_number}")
                    progress['error'] += 1
                    progress['index'] += 1
                    session['import_progress'] = progress
                    return render_template('erate_import.html', row=row, progress=progress, error=error_msg)

                # 70 VALUES = 70 PLACEHOLDERS
                cur.execute('''
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
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    app_number,
                    row.get('Form Nickname',''),
                    row.get('Form PDF',''),
                    row.get('Funding Year',''),
                    row.get('FCC Form 470 Status',''),
                    parse_datetime(row.get('Allowable Contract Date')),
                    parse_datetime(row.get('Created Date/Time')),
                    row.get('Created By',''),
                    parse_datetime(row.get('Certified Date/Time')),
                    row.get('Certified By',''),
                    parse_datetime(row.get('Last Modified Date/Time')),
                    row.get('Last Modified By',''),
                    row.get('Billed Entity Number',''),
                    row.get('Billed Entity Name',''),
                    row.get('Organization Status',''),
                    row.get('Organization Type',''),
                    row.get('Applicant Type',''),  # 70TH COLUMN
                    row.get('Website URL',''),
                    float(row.get('Latitude') or 0),
                    float(row.get('Longitude') or 0),
                    row.get('Billed Entity FCC Registration Number',''),
                    row.get('Billed Entity Address 1',''),
                    row.get('Billed Entity Address 2',''),
                    row.get('Billed Entity City',''),
                    row.get('Billed Entity State',''),
                    row.get('Billed Entity Zip Code',''),
                    row.get('Billed Entity Zip Code Ext',''),
                    row.get('Billed Entity Email',''),
                    row.get('Billed Entity Phone',''),
                    row.get('Billed Entity Phone Ext',''),
                    int(row.get('Number of Eligible Entities') or 0),
                    row.get('Contact Name',''),
                    row.get('Contact Address 1',''),
                    row.get('Contact Address 2',''),
                    row.get('Contact City',''),
                    row.get('Contact State',''),
                    row.get('Contact Zip',''),
                    row.get('Contact Zip Ext',''),
                    row.get('Contact Phone',''),
                    row.get('Contact Phone Ext',''),
                    row.get('Contact Email',''),
                    row.get('Technical Contact Name',''),
                    row.get('Technical Contact Title',''),
                    row.get('Technical Contact Phone',''),
                    row.get('Technical Contact Phone Ext',''),
                    row.get('Technical Contact Email',''),
                    row.get('Authorized Person Name',''),
                    row.get('Authorized Person Address',''),
                    row.get('Authorized Person City',''),
                    row.get('Authorized Person State',''),
                    row.get('Authorized Person Zip',''),
                    row.get('Authorized Person Zip Ext',''),
                    row.get('Authorized Person Phone Number',''),
                    row.get('Authorized Person Phone Number Ext',''),
                    row.get('Authorized Person Email',''),
                    row.get('Authorized Person Title',''),
                    row.get('Authorized Person Employer',''),
                    row.get('Category One Description',''),
                    row.get('Category Two Description',''),
                    row.get('Installment Type',''),
                    int(row.get('Installment Min Range Years') or 0),
                    int(row.get('Installment Max Range Years') or 0),
                    row.get('Request for Proposal Identifier',''),
                    row.get('State or Local Restrictions',''),
                    row.get('State or Local Restrictions Description',''),
                    row.get('Statewide State',''),
                    row.get('All Public Schools Districts',''),
                    row.get('All Non-Public schools',''),
                    row.get('All Libraries',''),
                    row.get('Form Version','')
                ))
                conn.commit()

        logger.info(f"Record {progress['index']} imported | App: {app_number}")
        progress['success'] += 1
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Record {progress['index']} failed | App: {app_number} | Error: {error_msg}")
        progress['error'] += 1
        session['import_progress'] = progress
        return render_template('erate_import.html', row=row, progress=progress, error=error_msg)

    progress['index'] += 1
    session['import_progress'] = progress
    return render_template('erate_import.html', row=row, progress=progress, success=True)

# === BULK IMPORT (70 = 70) ===
def _import_all_records():
    progress = session['import_progress']
    try:
        with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [n.strip().lstrip('\ufeff') for n in reader.fieldnames]
            for _ in range(progress['index'] - 1):
                next(reader)

            imported = skipped = 0
            with get_conn() as conn:
                with conn.cursor() as cur:
                    for row in reader:
                        app_number = row.get('Application Number', '').strip()
                        if not app_number:
                            logger.warning(f"Skipped row {progress['index'] + imported + skipped}: Missing Application Number")
                            skipped += 1
                            continue

                        cur.execute('SELECT 1 FROM erate WHERE app_number = %s', (app_number,))
                        if cur.fetchone():
                            logger.info(f"Skipped row {progress['index'] + imported + skipped}: Duplicate App {app_number}")
                            skipped += 1
                            continue

                        try:
                            cur.execute('''
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
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ''', (
                                app_number,
                                row.get('Form Nickname',''),
                                row.get('Form PDF',''),
                                row.get('Funding Year',''),
                                row.get('FCC Form 470 Status',''),
                                parse_datetime(row.get('Allowable Contract Date')),
                                parse_datetime(row.get('Created Date/Time')),
                                row.get('Created By',''),
                                parse_datetime(row.get('Certified Date/Time')),
                                row.get('Certified By',''),
                                parse_datetime(row.get('Last Modified Date/Time')),
                                row.get('Last Modified By',''),
                                row.get('Billed Entity Number',''),
                                row.get('Billed Entity Name',''),
                                row.get('Organization Status',''),
                                row.get('Organization Type',''),
                                row.get('Applicant Type',''),  # 70TH VALUE
                                row.get('Website URL',''),
                                float(row.get('Latitude') or 0),
                                float(row.get('Longitude') or 0),
                                row.get('Billed Entity FCC Registration Number',''),
                                row.get('Billed Entity Address 1',''),
                                row.get('Billed Entity Address 2',''),
                                row.get('Billed Entity City',''),
                                row.get('Billed Entity State',''),
                                row.get('Billed Entity Zip Code',''),
                                row.get('Billed Entity Zip Code Ext',''),
                                row.get('Billed Entity Email',''),
                                row.get('Billed Entity Phone',''),
                                row.get('Billed Entity Phone Ext',''),
                                int(row.get('Number of Eligible Entities') or 0),
                                row.get('Contact Name',''),
                                row.get('Contact Address 1',''),
                                row.get('Contact Address 2',''),
                                row.get('Contact City',''),
                                row.get('Contact State',''),
                                row.get('Contact Zip',''),
                                row.get('Contact Zip Ext',''),
                                row.get('Contact Phone',''),
                                row.get('Contact Phone Ext',''),
                                row.get('Contact Email',''),
                                row.get('Technical Contact Name',''),
                                row.get('Technical Contact Title',''),
                                row.get('Technical Contact Phone',''),
                                row.get('Technical Contact Phone Ext',''),
                                row.get('Technical Contact Email',''),
                                row.get('Authorized Person Name',''),
                                row.get('Authorized Person Address',''),
                                row.get('Authorized Person City',''),
                                row.get('Authorized Person State',''),
                                row.get('Authorized Person Zip',''),
                                row.get('Authorized Person Zip Ext',''),
                                row.get('Authorized Person Phone Number',''),
                                row.get('Authorized Person Phone Number Ext',''),
                                row.get('Authorized Person Email',''),
                                row.get('Authorized Person Title',''),
                                row.get('Authorized Person Employer',''),
                                row.get('Category One Description',''),
                                row.get('Category Two Description',''),
                                row.get('Installment Type',''),
                                int(row.get('Installment Min Range Years') or 0),
                                int(row.get('Installment Max Range Years') or 0),
                                row.get('Request for Proposal Identifier',''),
                                row.get('State or Local Restrictions',''),
                                row.get('State or Local Restrictions Description',''),
                                row.get('Statewide State',''),
                                row.get('All Public Schools Districts',''),
                                row.get('All Non-Public schools',''),
                                row.get('All Libraries',''),
                                row.get('Form Version','')
                            ))
                            imported += 1
                            if imported % 100 == 0:
                                conn.commit()
                                logger.info(f"Bulk import: {imported} records processed")
                        except Exception as e:
                            logger.error(f"Bulk import failed at row {progress['index'] + imported + skipped} | App: {app_number} | Error: {e}")
                            skipped += 1
                            conn.rollback()
                    conn.commit()

            progress['success'] += imported
            progress['error'] += skipped
            progress['index'] = progress['total'] + 1
            session['import_progress'] = progress
            logger.info(f"Bulk import completed: {imported} imported, {skipped} skipped")
            return redirect(url_for('erate.import_interactive'))
    except Exception as e:
        progress['error'] += 1
        session['import_progress'] = progress
        logger.critical(f"Bulk import crashed: {e}")
        return render_template('erate_import.html', progress=progress, error="Bulk import failed. Check logs.")
