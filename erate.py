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

# === ROUTES ===
@erate_bp.route('/')
def dashboard():
    return render_template('erate.html')

@erate_bp.route('/import-interactive', methods=['GET', 'POST'])
def import_interactive():
    if not os.path.exists(CSV_FILE):
        log(f"CSV file not found: {CSV_FILE}", "ERROR")
        return "<h2>CSV not found: 470schema.csv</h2>", 404

    # Count total rows
    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        total = sum(1 for _ in f) - 1 or 1

    # Handle POST actions
    if request.method == 'POST':
        action = request.form.get('action')

        # FORCE RESET
        if action == 'reset':
            reset_import_session()
            flash("Import session reset. Ready for new import.", "success")
            return redirect(url_for('erate.import_interactive'))

        if action == 'import_one':
            return _import_one_record()
        if action == 'import_all':
            return _import_all_records()

    # Auto-reset if stuck
    if 'import_progress' in session:
        progress = session['import_progress']
        if progress.get('index', 0) > total:
            log("Stuck import detected - auto resetting")
            reset_import_session()

    # Initialize session
    if 'import_progress' not in session:
        session['import_progress'] = {
            'index': 1,
            'total': total,
            'success': 0,
            'error': 0
        }

    progress = session['import_progress']

    # Check if import is complete
    if progress['index'] > progress['total']:
        return render_template('erate_import_complete.html', progress=progress)

    # Load current row
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

@erate_bp.route('/view-log')
def view_log():
    if os.path.exists(LOG_FILE):
        return send_file(LOG_FILE, mimetype='text/plain')
    return "No log file found.", 404

@erate_bp.route('/reset-import', methods=['POST'])
def reset_import():
    reset_import_session()
    flash("Import session fully reset.", "success")
    return redirect(url_for('erate.import_interactive'))
