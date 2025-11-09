# erate.py
from flask import Blueprint, render_template, request, session, redirect, url_for, jsonify, send_file, flash
import os
import csv
import logging
from db import get_conn
from datetime import datetime

erate_bp = Blueprint('erate', __name__, template_folder='templates', static_folder='static')

# === CONFIG ===
CSV_FILE = '470schema.csv'
LOG_FILE = 'import.log'

# Setup logger
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === HELPERS ===
def log(message, level="INFO"):
    if level == "ERROR":
        logger.error(message)
    elif level == "WARNING":
        logger.warning(message)
    else:
        logger.info(message)

def reset_import_session():
    session.pop('import_progress', None)
    log("Import session reset by user")

# === DASHBOARD ROUTE (FULLY WORKING) ===
@erate_bp.route('/')
def dashboard():
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Base query for total count
                query = "SELECT COUNT(*) FROM erate_forms"
                params = []

                # Apply filters
                filters = {}
                if request.args.get('state'):
                    filters['state'] = request.args['state'].strip().upper()
                    query += " WHERE state = %s"
                    params.append(filters['state'])
                if request.args.get('modified_after'):
                    filters['modified_after'] = request.args['modified_after']
                    if 'WHERE' not in query:
                        query += " WHERE"
                    else:
                        query += " AND"
                    query += " last_modified_datetime >= %s"
                    params.append(filters['modified_after'])

                cur.execute(query, params)
                total_count = cur.fetchone()[0]

                # Filtered count
                filtered_query = query
                cur.execute(filtered_query, params)
                total_filtered = cur.fetchone()[0]

                # Fetch paginated data
                offset = int(request.args.get('offset', 0))
                limit = 50
                data_query = """
                    SELECT app_number, entity_name, state, funding_year, fcc_status, last_modified_datetime
                    FROM erate_forms
                """
                if params:
                    data_query += " WHERE " + " AND ".join([
                        "state = %s" if 'state' in filters else "",
                        "last_modified_datetime >= %s" if 'modified_after' in filters else ""
                    ]).strip(" AND ")
                data_query += " ORDER BY last_modified_datetime DESC LIMIT %s OFFSET %s"
                data_params = params + [limit, offset]
                cur.execute(data_query, data_params)
                table_data = cur.fetchall()

                has_more = (offset + limit) < total_filtered
                next_offset = offset + limit

    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        total_count = total_filtered = 0
        table_data = []
        has_more = False
        next_offset = 0
        filters = {}

    return render_template(
        'erate.html',
        total_count=total_count,
        total_filtered=total_filtered,
        table_data=table_data,
        filters=filters,
        has_more=has_more,
        next_offset=next_offset
    )

# === IMPORT INTERACTIVE ===
@erate_bp.route('/import-interactive', methods=['GET', 'POST'])
def import_interactive():
    if not os.path.exists(CSV_FILE):
        log(f"CSV file not found: {CSV_FILE}", "ERROR")
        return "<h2>CSV not found: 470schema.csv</h2>", 404

    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        total = sum(1 for _ in f) - 1 or 1

    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'reset':
            reset_import_session()
            flash("Import session reset. Ready for new import.", "success")
            return redirect(url_for('erate.import_interactive'))
        if action == 'import_one':
            return _import_one_record()
        if action == 'import_all':
            return _import_all_records()

    if 'import_progress' in session:
        progress = session['import_progress']
        if progress.get('index', 0) > total:
            log("Stuck import detected - auto resetting")
            reset_import_session()

    if 'import_progress' not in session:
        session['import_progress'] = {
            'index': 1,
            'total': total,
            'success': 0,
            'error': 0
        }

    progress = session['import_progress']

    if progress['index'] > progress['total']:
        return render_template('erate_import_complete.html', progress=progress)

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
        return render_template('erate_import_complete.html', progress=progress)

    return render_template('erate_import.html', row=row, progress=progress)

# === IMPORT ONE RECORD ===
def _import_one_record():
    progress = session['import_progress']
    index = progress['index']

    try:
        with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [n.strip().lstrip('\ufeff') for n in reader.fieldnames]
            for _ in range(index - 1):
                next(reader)
            row = next(reader)
    except StopIteration:
        progress['index'] = progress['total'] + 1
        session['import_progress'] = progress
        return jsonify({'success': False, 'message': 'No more rows'})

    success = _save_to_db(row)
    if success:
        progress['success'] += 1
        log(f"Row {index}: Imported FRN {row.get('FRN', 'N/A')}")
    else:
        progress['error'] += 1
        log(f"Row {index}: FAILED - {row.get('FRN', 'N/A')}", "ERROR")

    progress['index'] += 1
    session['import_progress'] = progress

    return jsonify({
        'success': success,
        'index': progress['index'],
        'total': progress['total'],
        'success_count': progress['success'],
        'error_count': progress['error']
    })

# === IMPORT ALL RECORDS ===
def _import_all_records():
    progress = session['import_progress']
    start_index = progress['index']

    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        reader.fieldnames = [n.strip().lstrip('\ufeff') for n in reader.fieldnames]
        for _ in range(start_index - 1):
            next(reader)

        for row in reader:
            success = _save_to_db(row)
            if success:
                progress['success'] += 1
                log(f"Auto-import Row {progress['index']}: FRN {row.get('FRN', 'N/A')}")
            else:
                progress['error'] += 1
                log(f"Auto-import FAILED Row {progress['index']}: {row.get('FRN', 'N/A')}", "ERROR")
            progress['index'] += 1

    session['import_progress'] = progress
    return jsonify({'success': True, 'complete': True, 'progress': progress})

# === SAVE TO DB ===
def _save_to_db(row):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO erate_forms (
                        frn, applicant_name, ben, city, state, zip_code,
                        contact_name, contact_phone, contact_email,
                        category, discount_level, service_type
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (frn) DO NOTHING
                ''', (
                    row.get('FRN'),
                    row.get('Applicant Name'),
                    row.get('BEN'),
                    row.get('City'),
                    row.get('State'),
                    row.get('Zip Code'),
                    row.get('Contact Name'),
                    row.get('Contact Phone'),
                    row.get('Contact Email'),
                    row.get('Category'),
                    row.get('Discount Level'),
                    row.get('Service Type')
                ))
                conn.commit()
        return True
    except Exception as e:
        log(f"DB Error: {e}", "ERROR")
        return False

# === VIEW LOG ===
@erate_bp.route('/view-log')
def view_log():
    if os.path.exists(LOG_FILE):
        return send_file(LOG_FILE, mimetype='text/plain')
    return "No log file found.", 404

# === RESET IMPORT ===
@erate_bp.route('/reset-import', methods=['POST'])
def reset_import():
    reset_import_session()
    flash("Import session fully reset.", "success")
    return redirect(url_for('erate.import_interactive'))
