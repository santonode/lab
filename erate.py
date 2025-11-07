# erate.py
from flask import Blueprint, render_template, request, current_app, redirect, Response, session
import requests
import csv
import os
import io
from extensions import db
from models import Erate
from urllib.parse import quote_plus
from datetime import datetime

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

# === CONFIG ===
API_BASE_URL = "https://opendata.usac.org/api/views/jp7a-89nd"
ROWS_CSV_URL = f"{API_BASE_URL}/rows.csv"
CSV_FILE = "erate_data.csv"

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

def download_csv():
    if os.path.exists(CSV_FILE):
        return f"{CSV_FILE} already exists. <a href='/erate/import-interactive'>Start Import</a>"

    current_app.logger.info("Downloading full E-Rate CSV...")
    url = ROWS_CSV_URL + "?accessType=DOWNLOAD"
    headers = {'Accept-Encoding': 'gzip, deflate'}

    try:
        response = requests.get(url, stream=True, timeout=120, headers=headers)
        response.raise_for_status()
        response.raw.decode_content = True

        with open(CSV_FILE, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        current_app.logger.info("CSV downloaded.")
        return f"CSV downloaded: {CSV_FILE}<br><a href='/erate/import-interactive'>Start Interactive Import</a>"
    except Exception as e:
        current_app.logger.error(f"Download failed: {e}")
        return f"Download failed: {str(e)}"

# === INTERACTIVE IMPORT (GET/POST with Session State) ===
@erate_bp.route('/import-interactive', methods=['GET', 'POST'])
def import_interactive():
    if not os.path.exists(CSV_FILE):
        return redirect('/erate/download')

    # Session state for progress
    if 'import_progress' not in session:
        session['import_progress'] = {'index': 1, 'success': 0, 'error': 0, 'total': 0}
    progress = session['import_progress']

    # Calculate total rows if not done
    if progress['total'] == 0:
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            progress['total'] = sum(1 for _ in f) - 1  # minus header
        session['import_progress'] = progress

    # Handle POST (import current record)
    if request.method == 'POST':
        confirm = request.form.get('confirm')
        if confirm == 'ok':
            with open(CSV_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                next(reader)  # skip header

                # Go to current index
                for _ in range(progress['index'] - 1):
                    next(reader)

                row = next(reader)
                try:
                    erate = Erate(
                        id=row.get('ID', ''),
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
                    return render_success(row, progress)
                except Exception as e:
                    db.session.rollback()
                    progress['error'] += 1
                    progress['index'] += 1
                    session['import_progress'] = progress
                    return render_error(row, progress, str(e))

    # GET: Show current record
    if progress['index'] > progress['total']:
        return render_complete(progress)

    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        next(reader)
        for _ in range(progress['index'] - 1):
            next(reader)
        row = next(reader)

    return render_record(row, progress)

def render_record(row, progress):
    return render_template('erate_import.html',
        row=row, progress=progress, message=None, success=False, error=None)

def render_success(row, progress):
    return render_template('erate_import.html',
        row=row, progress=progress, message="Imported successfully!", success=True, error=None)

def render_error(row, progress, error):
    return render_template('erate_import.html',
        row=row, progress=progress, message=None, success=False, error=error)

def render_complete(progress):
    return f"""
    <h1>IMPORT COMPLETE!</h1>
    <div class="progress">
        Success: {progress['success']} |
        Errors: {progress['error']} |
        Total: {progress['total']}
    </div>
    <a href="/erate" class="action-btn">Go to Dashboard</a>
    """

# === DASHBOARD ===
@erate_bp.route('/')
def erate_dashboard():
    try:
        state = request.args.get('state', 'KS')
        min_date = request.args.get('min_date', '2025-01-01')
        offset = int(request.args.get('offset', 0))
        limit = 10

        query = Erate.query.filter(
            Erate.state == state.upper(),
            Erate.last_modified >= min_date
        ).order_by(Erate.id)

        total_filtered = query.count()
        data = query.offset(offset).limit(limit + 1).all()
        has_more = len(data) > limit
        table_data = data[:limit]

        return render_template(
            'erate.html',
            table_data=table_data,
            filters={'state': state, 'min_date': min_date},
            total=offset + len(table_data),
            total_filtered=total_filtered,
            has_more=has_more,
            next_offset=offset + limit
        )
    except Exception as e:
        current_app.logger.error(f"E-Rate error: {e}")
        return render_template(
            'erate.html',
            error=str(e),
            table_data=[],
            total=0,
            total_filtered=0,
            filters={'state': 'KS', 'min_date': '2025-01-01'},
            has_more=False,
            next_offset=0
        )

@erate_bp.route('/download-csv')
def download_csv():
    state = request.args.get('state', 'KS')
    min_date = request.args.get('min_date', '2025-01-01')
    where = f"`Billed Entity State` = '{state.upper()}' AND `Last Modified Date/Time` >= '{min_date}T00:00:00.000'"
    encoded = quote_plus(where)
    url = f"{ROWS_CSV_URL}?$where={encoded}&$order=`ID` ASC"
    return redirect(url)
