# erate.py
from flask import Blueprint, render_template, request, current_app, redirect, Response, jsonify
import requests
import csv
import os
import threading
from extensions import db
from models import Erate
from urllib.parse import quote_plus
from datetime import datetime

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

API_BASE_URL = "https://opendata.usac.org/api/views/jp7a-89nd"
ROWS_CSV_URL = f"{API_BASE_URL}/rows.csv"
CSV_FILE = "erate_data.csv"
STATUS_FILE = "download_status.txt"

# === BACKGROUND DOWNLOAD ===
def download_csv_background():
    if os.path.exists(CSV_FILE):
        with open(STATUS_FILE, 'w') as f:
            f.write("already_exists")
        return

    with open(STATUS_FILE, 'w') as f:
        f.write("downloading")

    url = ROWS_CSV_URL + "?accessType=DOWNLOAD"
    headers = {'Accept-Encoding': 'gzip, deflate'}

    try:
        response = requests.get(url, stream=True, timeout=300, headers=headers)
        response.raise_for_status()
        response.raw.decode_content = True

        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0

        with open(CSV_FILE, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    progress = (downloaded / total_size * 100) if total_size else 0
                    with open(STATUS_FILE, 'w') as sf:
                        sf.write(f"downloading:{progress:.1f}")

        with open(STATUS_FILE, 'w') as f:
            f.write("complete")
    except Exception as e:
        with open(STATUS_FILE, 'w') as f:
            f.write(f"error:{str(e)}")

# === INTERACTIVE IMPORT GENERATOR ===
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
            yield f"ID: {row.get('ID', 'N/A')}\n"
            yield f"State: {row.get('Billed Entity State', 'N/A')}\n"
            yield f"Entity: {row.get('Billed Entity Name', 'N/A')}\n"
            yield f"Amount: {row.get('Total Committed Amount', 'N/A')}\n"
            yield f"Modified: {row.get('Last Modified Date/Time', 'N/A')}\n"
            yield "\nType 'ok' and press Enter...\n</pre>"

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

        yield f"<h2>DONE! Success: {success}, Errors: {error}</h2>"

# === ROUTES ===
@erate_bp.route('/download')
def start_download():
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE, 'r') as f:
            status = f.read()
        if status == "complete":
            return f"CSV ready! <a href='/erate/import-interactive'>Start Import</a>"
        elif status.startswith("downloading"):
            return "Download in progress... <a href='/erate/status'>Check Status</a>"
        elif status.startswith("error"):
            return f"Download failed: {status.split(':',1)[1]}"
        else:
            return "Unknown status. <a href='/erate/download'>Retry</a>"

    if os.path.exists(CSV_FILE):
        return f"CSV exists. <a href='/erate/import-interactive'>Start Import</a>"

    threading.Thread(target=download_csv_background, daemon=True).start()
    return "Download started in background... <a href='/erate/status'>Check Status</a>"

@erate_bp.route('/status')
def download_status():
    if not os.path.exists(STATUS_FILE):
        return "No download in progress."
    with open(STATUS_FILE, 'r') as f:
        status = f.read()
    if status == "complete":
        return "DOWNLOAD COMPLETE! <a href='/erate/import-interactive'>Start Import</a>"
    elif status.startswith("downloading"):
        progress = status.split(':',1)[1]
        return f"Downloading... {progress}%"
    elif status.startswith("error"):
        return f"Error: {status.split(':',1)[1]}"
    else:
        return "Starting..."

@erate_bp.route('/import-interactive', methods=['GET', 'POST'])
def import_interactive():
    return Response(import_generator(), mimetype='text/html')

@erate_bp.route('/')
def dashboard():
    # your dashboard
    pass

@erate_bp.route('/download-csv')
def download_filtered():
    state = request.args.get('state', 'KS')
    min_date = request.args.get('min_date', '2025-01-01')
    where = f"`Billed Entity State` = '{state}' AND `Last Modified Date/Time` >= '{min_date}T00:00:00.000'"
    url = f"{ROWS_CSV_URL}?$where={quote_plus(where)}&$order=`ID` ASC"
    return redirect(url)
