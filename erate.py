# erate.py
from flask import (
    Blueprint, render_template, request, session, redirect, url_for,
    jsonify, send_file, flash, current_app
)
import csv
import os
import logging
import requests
import threading
import time
import sys
import traceback
from db import get_conn
from datetime import datetime

# === LOGGING SETUP (RENDER + FILE + FULL DEBUG) ===
LOG_FILE = os.path.join(os.path.dirname(__file__), "import.log")

# Ensure file exists
try:
    open(LOG_FILE, 'a').close()
except Exception as e:
    print(f"WARNING: Cannot access {LOG_FILE}: {e}")

# Log to stdout (Render) + file
handler_file = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
handler_stdout = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
handler_file.setFormatter(formatter)
handler_stdout.setFormatter(formatter)

logger = logging.getLogger('erate')
logger.setLevel(logging.INFO)
for h in logger.handlers[:]: logger.removeHandler(h)
logger.addHandler(handler_file)
logger.addHandler(handler_stdout)

# Force log to Render + file + flush + print
def log(msg, *args):
    formatted = msg % args if args else msg
    logger.info(formatted)
    for h in logger.handlers:
        h.flush()
    print(formatted, flush=True)  # Force Render

# === BLUEPRINT ===
erate_bp = Blueprint('erate', __name__, url_prefix='/erate', template_folder='templates')
CSV_FILE = os.path.join(os.path.dirname(__file__), "470schema.csv")

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
        row.get('Applicant Type', ''),
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

# === DASHBOARD ===
@erate_bp.route('/')
def dashboard():
    state_filter = request.args.get('state', '').strip().upper()
    modified_after_str = request.args.get('modified_after', '').strip()
    offset = max(int(request.args.get('offset', 0)), 0)
    limit = 10

    filters = {'state': state_filter, 'modified_after': modified_after_str}

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            count_sql = 'SELECT COUNT(*) FROM erate'
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
            total_count = cur.fetchone()[0]

            sql = '''
                SELECT app_number, entity_name, state, funding_year,
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

            table_data = [
                {
                    'app_number': r[0], 'entity_name': r[1], 'state': r[2],
                    'funding_year': r[3], 'fcc_status': r[4],
                    'last_modified_datetime': r[5]
                }
                for r in rows
            ]

        has_more = len(table_data) > limit
        table_data = table_data[:limit]
        next_offset = offset + limit

        return render_template(
            'erate.html',
            table_data=table_data, filters=filters, total_count=total_count,
            total_filtered=offset + len(table_data), has_more=has_more,
            next_offset=next_offset
        )

    except Exception as e:
        log("Dashboard error: %s", e)
        return f"<pre>ERROR: {e}</pre>", 500

# === EXTRACT CSV ===
@erate_bp.route('/extract-csv')
def extract_csv():
    if current_app.config.get('CSV_DOWNLOAD_IN_PROGRESS'):
        flash("CSV download already in progress.", "info")
        return redirect(url_for('erate.dashboard'))
    if os.path.exists(CSV_FILE) and os.path.getsize(CSV_FILE) > 500_000_000:
        flash("Large CSV exists. Delete to re-download.", "warning")
        return redirect(url_for('erate.dashboard'))

    current_app.config['CSV_DOWNLOAD_IN_PROGRESS'] = True
    thread = threading.Thread(target=_download_csv_background, args=(current_app._get_current_object(),))
    thread.daemon = True
    thread.start()
    flash("CSV download started. Check in 2-5 min.", "info")
    return redirect(url_for('erate.dashboard'))

def _download_csv_background(app):
    time.sleep(1)
    try:
        log("Starting CSV download...")
        response = requests.get("https://opendata.usac.org/api/views/jp7a-89nd/rows.csv?accessType=DOWNLOAD", stream=True, timeout=600)
        response.raise_for_status()
        downloaded = 0
        with open(CSV_FILE, 'wb') as f:
            for chunk in response.iter_content(8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if downloaded % (10*1024*1024) == 0:
                        log("Downloaded %.1f MB", downloaded/(1024*1024))
        log("CSV downloaded: %.1f MB", os.path.getsize(CSV_FILE)/(1024*1024))
    except Exception as e:
        log("Download failed: %s", e)
        if os.path.exists(CSV_FILE): os.remove(CSV_FILE)
    finally:
        with app.app_context():
            app.config['CSV_DOWNLOAD_IN_PROGRESS'] = False

# === PROGRESS & STATUS ===
@erate_bp.route('/progress')
def get_progress():
    return jsonify(current_app.config.get('IMPORT_PROGRESS', {'index':1,'total':1,'success':0,'error':0}))

@erate_bp.route('/status')
def get_status():
    thread = current_app.config.get('IMPORT_THREAD')
    return jsonify({
        'running': current_app.config.get('BULK_IMPORT_IN_PROGRESS', False),
        'thread_alive': thread.is_alive() if thread else False
    })

# === IMPORT INTERACTIVE ===
@erate_bp.route('/import-interactive', methods=['GET', 'POST'])
def import_interactive():
    if not os.path.exists(CSV_FILE):
        return "<h2>CSV not found: 470schema.csv</h2>", 404

    total = sum(1 for _ in open(CSV_FILE, 'r', encoding='utf-8-sig')) - 1
    progress = session.get('import_progress', {'index': 1, 'total': total, 'success': 0, 'error': 0})

    if request.method == 'POST' and request.form.get('action') == 'import_all':
        if current_app.config.get('BULK_IMPORT_IN_PROGRESS'):
            return jsonify({'error': 'Running'}), 429

        current_app.config.update({
            'BULK_IMPORT_IN_PROGRESS': True,
            'IMPORT_PROGRESS': progress.copy(),
            'IMPORT_THREAD': None
        })

        thread = threading.Thread(target=_import_all_background, args=(current_app._get_current_object(), progress.copy()))
        thread.daemon = True
        current_app.config['IMPORT_THREAD'] = thread
        thread.start()
        return jsonify({'status': 'started'})

    if progress['index'] > progress['total']:
        return render_template('erate_import_complete.html', progress=progress)

    try:
        with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [n.strip().lstrip('\ufeff') for n in reader.fieldnames]
            for _ in range(progress['index'] - 1): next(reader)
            row = next(reader)
    except StopIteration:
        progress['index'] = progress['total'] + 1
        session['import_progress'] = progress
        return render_template('erate_import_complete.html', progress=progress)

    return render_template('erate_import.html', row=row, progress=progress)

# === BULK IMPORT — FULL DEBUG ===
def _import_all_background(app, progress):
    time.sleep(1)
    batch_size = 1000
    try:
        log("Bulk import started from record %s", progress['index'])
        log("Opening CSV file: %s", CSV_FILE)
        with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
            log("CSV opened successfully")
            reader = csv.DictReader(f)
            log("CSV reader created, fieldnames: %s", reader.fieldnames[:5])

            # Skip to start index
            skipped_rows = 0
            for _ in range(progress['index'] - 1):
                next(reader)
                skipped_rows += 1
                if skipped_rows % 10000 == 0:
                    log("Skipped %s rows to reach start index", skipped_rows)

            log("Starting import loop at record %s", progress['index'])

            batch = []
            imported = 0
            row_count = 0

            for row in reader:
                row_count += 1
                if row_count % 10000 == 0:
                    log("Processing row %s", progress['index'] + row_count - 1)

                app_number = row.get('Application Number', '').strip()
                if not app_number:
                    log("Skipping row %s: missing app_number", row_count)
                    continue

                # DB CHECK WITH FULL DEBUG
                try:
                    with app.app_context():
                        with get_conn() as conn:
                            with conn.cursor() as cur:
                                cur.execute('SELECT 1 FROM erate WHERE app_number = %s', (app_number,))
                                if cur.fetchone():
                                    log("Duplicate: %s", app_number)
                                    continue
                except Exception as e:
                    log("DB ERROR on app_number %s: %s", app_number, e)
                    continue

                batch.append(row)
                imported += 1

                if len(batch) >= batch_size:
                    log("Committing batch of %s records (total imported: %s)", batch_size, imported)
                    with app.app_context():
                        _commit_batch(app, batch)
                    progress['index'] += batch_size
                    progress['success'] += batch_size
                    with app.app_context():
                        current_app.config['IMPORT_PROGRESS'] = progress.copy()
                    log("Imported: %s", progress['index'] - 1)
                    batch = []

            # Final batch
            if batch:
                log("Committing final batch of %s records", len(batch))
                with app.app_context():
                    _commit_batch(app, batch)
                progress['index'] = progress['total'] + 1
                progress['success'] += len(batch)
                with app.app_context():
                    current_app.config['IMPORT_PROGRESS'] = progress.copy()

            log("Bulk import complete: %s imported, %s processed", imported, row_count)

    except Exception as e:
        log("FATAL ERROR in import: %s", e)
        log("Traceback: %s", traceback.format_exc())
    finally:
        with app.app_context():
            app.config.update({
                'BULK_IMPORT_IN_PROGRESS': False,
                'IMPORT_THREAD': None
            })
        log("Import thread finished")

def _commit_batch(app, batch):
    with app.app_context():
        with get_conn() as conn:
            with conn.cursor() as cur:
                for row in batch:
                    try:
                        cur.execute(INSERT_SQL, _row_to_tuple(row))
                    except Exception as e:
                        log("Row failed: %s", e)
                conn.commit()

# === CANCEL / VIEW / RESET ===
@erate_bp.route('/cancel-import', methods=['POST'])
def cancel_import():
    thread = current_app.config.get('IMPORT_THREAD')
    if thread and thread.is_alive():
        current_app.config['BULK_IMPORT_IN_PROGRESS'] = False
        flash("Import cancelled.", "warning")
    return redirect(url_for('erate.import_interactive'))

@erate_bp.route('/view-log')
def view_log():
    if os.path.exists(LOG_FILE):
        return send_file(LOG_FILE, mimetype='text/plain')
    return "No log file.", 404

@erate_bp.route('/reset-import', methods=['POST'])
def reset_import():
    session.pop('import_progress', None)
    flash("Import reset.", "success")
    return redirect(url_for('erate.import_interactive'))
