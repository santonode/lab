# erate.py
from flask import Blueprint, render_template, request, current_app, session
import requests
import csv
import io
from extensions import db
from models import Erate
from urllib.parse import quote_plus
from datetime import datetime

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

API_BASE_URL = "https://opendata.usac.org/api/views/jp7a-89nd"
ROWS_CSV_URL = f"{API_BASE_URL}/rows.csv"

# === HELPER FUNCTIONS ===
def safe_float(value, default=0.0):
    try:
        return float(value) if value and str(value).strip() else default
    except (ValueError, TypeError):
        return default

def safe_date(value):
    try:
        if value and str(value).strip():
            clean = value.strip().replace('Z', '+00:00')
            return datetime.fromisoformat(clean).date()
        return None
    except Exception:
        return None

def get_unique_id(row):
    """Generate a unique ID from available fields"""
    frn = row.get('FRN', '').strip()
    app_num = row.get('Application Number', '').strip()
    if frn and app_num:
        return f"{frn}_{app_num}"
    elif frn:
        return f"frn_{frn}"
    elif app_num:
        return f"app_{app_num}"
    else:
        return f"auto_{int(datetime.now().timestamp()*1000000)}"

# === INTERACTIVE IMPORT ===
@erate_bp.route('/import-interactive', methods=['GET', 'POST'])
def import_interactive():
    if 'import_progress' not in session:
        session['import_progress'] = {'index': 1, 'success': 0, 'error': 0, 'total': 2161188}
    progress = session['import_progress']

    if request.method == 'POST':
        confirm = request.form.get('confirm')
        if confirm == 'ok':
            try:
                offset = progress['index'] - 1
                url = f"{ROWS_CSV_URL}?$limit=1&$offset={offset}&accessType=DOWNLOAD"
                response = requests.get(url, stream=True, timeout=30)
                response.raise_for_status()
                response.raw.decode_content = True
                stream = io.TextIOWrapper(response.raw, encoding='utf-8')
                reader = csv.DictReader(stream)
                row = next(reader)

                # === SKIP IF NO ID AND NO FALLBACK ===
                raw_id = row.get('ID', '').strip()
                if not raw_id:
                    frn = row.get('FRN', '').strip()
                    app_num = row.get('Application Number', '').strip()
                    if not frn and not app_num:
                        progress['error'] += 1
                        progress['index'] += 1
                        session['import_progress'] = progress
                        return render_template('erate_import.html', row=row, progress=progress, success=False, error="Skipped: No ID, FRN, or App Number")

                # === USE ID OR GENERATE ONE ===
                record_id = raw_id if raw_id else get_unique_id(row)

                # === CHECK IF ALREADY EXISTS ===
                if db.session.get(Erate, record_id):
                    progress['error'] += 1
                    progress['index'] += 1
                    session['import_progress'] = progress
                    return render_template('erate_import.html', row=row, progress=progress, success=False, error="Skipped: Already in DB")

                erate = Erate(
                    id=record_id,
                    state=row.get('Billed Entity State', '')[:2],
                    funding_year=row.get('Funding Year', ''),
                    entity_name=row.get('Billed Entity Name', ''),
                    address=row.get('Billed Entity Address', ''),
                    zip_code=row.get('Billed Entity ZIP Code', ''),
                    frn=row.get('FRN', ''),
                    app_number=row.get('Application Number', ''),
                    status=row.get('FRN Status', ''),
                    amount=safe_float(row.get('Total Committed Amount')),
                    description=row.get('Service Type', ''),
                    last_modified=safe_date(row.get('Last Modified Date/Time'))
                )
                db.session.add(erate)
                db.session.commit()
                progress['success'] += 1
                progress['index'] += 1
                session['import_progress'] = progress
                return render_template('erate_import.html', row=row, progress=progress, success=True)
            except Exception as e:
                db.session.rollback()
                progress['error'] += 1
                progress['index'] += 1
                session['import_progress'] = progress
                return render_template('erate_import.html', row={}, progress=progress, error=str(e))

    if progress['index'] > progress['total']:
        return f"<h1>IMPORT COMPLETE!</h1><p>Success: {progress['success']} | Errors: {progress['error']}</p><a href='/erate'>Dashboard</a>"

    try:
        offset = progress['index'] - 1
        url = f"{ROWS_CSV_URL}?$limit=1&$offset={offset}&accessType=DOWNLOAD"
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        response.raw.decode_content = True
        stream = io.TextIOWrapper(response.raw, encoding='utf-8')
        reader = csv.DictReader(stream)
        row = next(reader)
    except Exception as e:
        return f"Error fetching row: {e}"

    return render_template('erate_import.html', row=row, progress=progress, success=False, error=None)
