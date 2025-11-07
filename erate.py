# erate.py
from flask import Blueprint, render_template, request, current_app, redirect, jsonify, Response
import requests
import csv
import io
import os
from extensions import db
from models import Erate
from urllib.parse import quote_plus
from datetime import datetime

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

# === CONFIG ===
API_BASE_URL = "https://opendata.usac.org/api/views/jp7a-89nd"
ROWS_CSV_URL = f"{API_BASE_URL}/rows.csv"
CSV_FILE = "erate_data.csv"  # Saved on Render server

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

# === STEP 1: DOWNLOAD CSV ONCE ===
def download_csv():
    if os.path.exists(CSV_FILE):
        return f"{CSV_FILE} already exists. Skipping download."
    
    current_app.logger.info("Starting CSV download...")
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
        
        current_app.logger.info(f"Downloaded {CSV_FILE}")
        return f"CSV downloaded successfully: {CSV_FILE}<br>Now go to <a href='/erate/import-interactive'>/erate/import-interactive</a>"
    except Exception as e:
        current_app.logger.error(f"Download failed: {e}")
        return f"Download failed: {str(e)}"

# === STEP 2: INTERACTIVE IMPORT (ONE BY ONE) ===
def interactive_import_generator():
    if not os.path.exists(CSV_FILE):
        yield "ERROR: CSV not found. Run /erate/download first.<br>"
        return

    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        total_rows = sum(1 for _ in open(CSV_FILE, 'r', encoding='utf-8')) - 1
        f.seek(0)
        next(reader)  # skip header

        success = 0
        error = 0

        yield f"<pre>Starting interactive import... Total rows: {total_rows}\n"
        yield "Press ENTER in browser console (F12) to import each record.\n\n</pre>"

        for i, row in enumerate(reader, 1):
            # Show record
            yield f"<pre>\n--- Record {i}/{total_rows} ---\n"
            yield f"ID: {row.get('ID', 'N/A')}\n"
            yield f"State: {row.get('Billed Entity State', 'N/A')}\n"
            yield f"Entity: {row.get('Billed Entity Name', 'N/A')}\n"
            yield f"Amount: {row.get('Total Committed Amount', 'N/A')}\n"
            yield f"Modified: {row.get('Last Modified Date/Time', 'N/A')}\n"
            yield "\n<i>Waiting for you to press ENTER...</i>\n</pre>"

            # Wait for user input via JS
            user_input = request.form.get('confirm')
            if not user_input:
                yield "<script>setTimeout(() => { document.getElementById('confirm').focus(); }, 100);</script>"
                yield """
                <form method="post">
                    <input type="text" id="confirm" name="confirm" placeholder="Type anything and press Enter" style="width:100%;padding:10px;font-size:16px;">
                </form>
                <script>document.getElementById('confirm').focus();</script>
                """
                return

            # Import record
            try:
                erate = Erate(
                    id=str(row.get('ID', '')).strip(),
                    state=str(row.get('Billed Entity State', '')).strip()[:2],
                    funding_year=str(row.get('Funding Year', '')).strip(),
                    entity_name=str(row.get('Billed Entity Name', '')).strip(),
                    address=str(row.get('Billed Entity Address', '')).strip(),
                    zip_code=str(row.get('Billed Entity ZIP Code', '')).strip(),
                    frn=str(row.get('FRN', '')).strip(),
                    app_number=str(row.get('Application Number', '')).strip(),
                    status=str(row.get('FRN Status', '')).strip(),
                    amount=safe_float(row.get('Total Committed Amount')),
                    description=str(row.get('Service Type', '')).strip(),
                    last_modified=safe_date(row.get('Last Modified Date/Time'))
                )
                db.session.add(erate)
                db.session.commit()
                success += 1
                yield f"<pre style='color:green;'>IMPORTED! Success: {success}</pre>"
            except Exception as e:
                db.session.rollback()
                error += 1
                current_app.logger.error(f"Row {i} failed: {e}")
                yield f"<pre style='color:red;'>ERROR: {e}\nSkipped. Errors: {error}</pre>"

            yield "<hr>"

        yield f"<pre><strong>IMPORT COMPLETE!</strong><br>Success: {success}<br>Errors: {error}</pre>"

# === ROUTES ===
@erate_bp.route('/download')
def download_data():
    return download_csv()

@erate_bp.route('/import-interactive', methods=['GET', 'POST'])
def import_interactive():
    return Response(interactive_import_generator(), mimetype='text/html')

@erate_bp.route('/')
def erate_dashboard():
    try:
        state = request.args.get('state', 'KS').strip().upper()
        min_date = request.args.get('min_date', '2025-01-01')
        offset = max(int(request.args.get('offset', 0)), 0)
        limit = 10

        query = Erate.query.filter(
            Erate.state == state,
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
        current_app.logger.error(f"Dashboard error: {e}")
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
    state = request.args.get('state', 'KS').strip().upper()
    min_date = request.args.get('min_date', '2025-01-01')
    where = f"`Billed Entity State` = '{state}' AND `Last Modified Date/Time` >= '{min_date}T00:00:00.000'"
    encoded = quote_plus(where)
    url = f"{ROWS_CSV_URL}?$where={encoded}&$order=`ID` ASC"
    return redirect(url)
