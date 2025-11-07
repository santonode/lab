# erate.py
from flask import Blueprint, render_template, request, current_app, redirect
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

def safe_float(value, default=0.0):
    try:
        return float(value) if value and value.strip() else default
    except (ValueError, TypeError):
        return default

def safe_date(value):
    try:
        if value and value.strip():
            return datetime.fromisoformat(value.replace('Z', '+00:00')).date()
        return None
    except:
        return None

def import_csv_to_postgres():
    url = ROWS_CSV_URL + "?accessType=DOWNLOAD"
    headers = {'Accept-Encoding': 'gzip, deflate'}
    response = requests.get(url, stream=True, timeout=60, headers=headers)
    response.raise_for_status()
    response.raw.decode_content = True

    stream = io.TextIOWrapper(response.raw, encoding='utf-8')
    reader = csv.DictReader(stream)  # ← USE DICT READER

    success_count = 0
    error_count = 0

    for row in reader:
        try:
            # Map by COLUMN NAME (not index)
            erate = Erate(
                id=row.get('ID', '').strip(),
                state=row.get('Billed Entity State', '').strip(),
                funding_year=row.get('Funding Year', '').strip(),
                entity_name=row.get('Billed Entity Name', '').strip(),
                address=row.get('Billed Entity Address', '').strip(),
                zip_code=row.get('Billed Entity ZIP Code', '').strip(),
                frn=row.get('FRN', '').strip(),
                app_number=row.get('Application Number', '').strip(),
                status=row.get('FRN Status', '').strip(),
                amount=safe_float(row.get('Total Committed Amount')),
                description=row.get('Service Type', '').strip(),
                last_modified=safe_date(row.get('Last Modified Date/Time'))
            )

            db.session.add(erate)
            db.session.commit()
            success_count += 1

        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f"Row import failed: {e} | ID: {row.get('ID', 'N/A')}")
            error_count += 1
            continue  # Skip bad row

    stream.close()
    current_app.logger.info(f"Import complete: {success_count} success, {error_count} errors")
    return success_count, error_count

@erate_bp.route('/import')
def import_data():
    try:
        success, errors = import_csv_to_postgres()
        return f"E-Rate import complete!<br>Success: {success}<br>Errors (skipped): {errors}"
    except Exception as e:
        current_app.logger.error(f"Import failed: {e}")
        return f"Import failed: {str(e)}"

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

@erate_bp.route('/download')
def download_csv():
    state = request.args.get('state', 'KS').strip().upper()
    min_date = request.args.get('min_date', '2025-01-01')
    where = f"`Billed Entity State` = '{state}' AND `Last Modified Date/Time` >= '{min_date}T00:00:00.000'"
    encoded = quote_plus(where)
    url = f"{ROWS_CSV_URL}?$where={encoded}&$order=`ID` ASC"
    return redirect(url)
