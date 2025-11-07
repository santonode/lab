# erate.py
from flask import Blueprint, render_template, request, current_app, redirect, Response
import requests
import csv
import os
from extensions import db
from models import Erate
from urllib.parse import quote_plus
from datetime import datetime

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

API_BASE_URL = "https://opendata.usac.org/api/views/jp7a-89nd"
ROWS_CSV_URL = f"{API_BASE_URL}/rows.csv"
CSV_FILE = "erate_data.csv"

# === STEP 1: DOWNLOAD FULL CSV ===
def download_csv():
    if os.path.exists(CSV_FILE):
        return f"{CSV_FILE} already exists. <a href='/erate/import-interactive'>Start import</a>"

    current_app.logger.info("Downloading full E-Rate CSV...")
    url = ROWS_CSV_URL + "?accessType=DOWNLOAD"
    headers = {'Accept-Encoding': 'gzip, deflate'}

    try:
        response = requests.get(url, stream=True, timeout=120, headers=headers)
        response.raise_for_status()
        response.raw.decode_content = True

        with open(CSV_FILE, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        current_app.logger.info("CSV downloaded.")
        return f"CSV downloaded: {CSV_FILE}<br><a href='/erate/import-interactive'>Start Interactive Import</a>"
    except Exception as e:
        return f"Download failed: {e}"

# === STEP 2: INTERACTIVE IMPORT (BROWSER) ===
def import_generator():
    if not os.path.exists(CSV_FILE):
        yield "Run /erate/download first.<br>"
        return

    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        total = sum(1 for _ in open(CSV_FILE)) - 1
        f.seek(0)
        next(reader)

        success = error = 0
        for i, row in enumerate(reader, 1):
            yield f"<pre>\n--- Record {i}/{total} ---\n"
            for k, v in row.items():
                if k in ['ID', 'Billed Entity State', 'Billed Entity Name', 'Total Committed Amount', 'Last Modified Date/Time']:
                    yield f"{k}: {v}\n"
            yield "\nPress ENTER to import...\n</pre>"

            confirm = request.form.get('confirm')
            if not confirm:
                yield """
                <form method="post">
                    <input name="confirm" placeholder="Type 'ok' and press Enter" style="width:100%;padding:10px;font-size:16px;">
                </form>
                <script>document.querySelector('input').focus();</script>
                """
                return

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
                    amount=float(row.get('Total Committed Amount', 0) or 0),
                    description=row.get('Service Type', ''),
                    last_modified=datetime.fromisoformat(row.get('Last Modified Date/Time', '').replace('Z', '+00:00')).date() if row.get('Last Modified Date/Time') else None
                )
                db.session.add(erate)
                db.session.commit()
                success += 1
                yield f"<pre style='color:green'>IMPORTED! ({success})</pre><hr>"
            except Exception as e:
                db.session.rollback()
                error += 1
                yield f"<pre style='color:red'>ERROR: {e}</pre><hr>"

        yield f"<h2>Done! Success: {success}, Errors: {error}</h2>"

# === ROUTES ===
@erate_bp.route('/download')
def download_route():
    return download_csv()

@erate_bp.route('/import-interactive', methods=['GET', 'POST'])
def import_interactive():
    return Response(import_generator(), mimetype='text/html')

@erate_bp.route('/')
def dashboard():
    # ... your dashboard code
    pass

@erate_bp.route('/download-csv')
def download_filtered():
    state = request.args.get('state', 'KS')
    min_date = request.args.get('min_date', '2025-01-01')
    where = f"`Billed Entity State` = '{state}' AND `Last Modified Date/Time` >= '{min_date}T00:00:00.000'"
    url = f"{ROWS_CSV_URL}?$where={quote_plus(where)}&$order=`ID` ASC"
    return redirect(url)
