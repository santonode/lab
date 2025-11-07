# erate.py
from flask import Blueprint, render_template, request, current_app, send_file
import requests
import csv
from io import StringIO, BytesIO
from typing import Dict, Any, Optional

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

# === CONFIG ===
API_BASE_URL = "https://opendata.usac.org/api/views/jp7a-89nd"
ROWS_CSV_URL = f"{API_BASE_URL}/rows.csv"  # CSV endpoint
FULL_CSV_URL = f"{API_BASE_URL}/rows.csv?accessType=DOWNLOAD"

# === HELPERS ===
def fetch_erate_csv_stream(params: Dict[str, Any]) -> requests.Response:
    url = ROWS_CSV_URL
    params.setdefault('$limit', '10')  # ONLY 10 ROWS
    params.setdefault('$offset', '0')

    try:
        response = requests.get(url, params=params, stream=True, timeout=15)
        response.raise_for_status()
        return response
    except Exception as e:
        current_app.logger.error(f"E-Rate CSV error: {e}")
        raise

def parse_csv_rows(response: requests.Response, limit: int = 10) -> list:
    """Parse only first N rows from streaming CSV."""
    rows = []
    for i, line in enumerate(response.iter_lines(decode_unicode=True)):
        if i == 0:  # Skip header
            continue
        if i > limit:
            break
        row = line.split(',')
        if len(row) >= 11:
            rows.append({
                'id': row[0],
                'state': row[1],
                'funding_year': row[2],
                'entity_name': row[3],
                'address': row[4],
                'zip': row[5],
                'frn': row[6],
                'app_number': row[7],
                'status': row[8],
                'amount': row[9],
                'description': row[10]
            })
    return rows

def build_where_clause(state: Optional[str] = None, year: Optional[str] = None) -> str:
    conditions = []
    if state:
        conditions.append(f"state = '{state.upper()}'")
    if year:
        conditions.append(f"funding_year = {year}")
    return " AND ".join(conditions) if conditions else "1=1"

# === ROUTES ===
@erate_bp.route('/')
def erate_dashboard():
    try:
        state = request.args.get('state')
        year = request.args.get('year')
        offset = int(request.args.get('offset', 0))

        params = {
            '$where': build_where_clause(state, year),
            '$limit': '11',  # 10 + 1 to check "has more"
            '$offset': str(offset),
            '$order': 'id'
        }

        response = fetch_erate_csv_stream(params)
        all_rows = parse_csv_rows(response, limit=11)
        table_data = all_rows[:10]
        has_more = len(all_rows) > 10

        return render_template(
            'erate.html',
            table_data=table_data,
            filters={'state': state, 'year': year},
            total=offset + len(table_data),
            has_more=has_more,
            next_offset=offset + 10
        )
    except Exception as e:
        current_app.logger.error(f"E-Rate error: {e}")
        return render_template('erate.html', error=str(e), table_data=[], total=0)

@erate_bp.route('/download')
def download_csv():
    try:
        state = request.args.get('state')
        year = request.args.get('year')
        params = {'$where': build_where_clause(state, year)}

        response = requests.get(ROWS_CSV_URL, params=params, stream=True, timeout=300)
        response.raise_for_status()

        output = BytesIO()
        for chunk in response.iter_content(chunk_size=8192):
            output.write(chunk)
        output.seek(0)

        filename = f"erate_{state or 'all'}_{year or 'all'}.csv"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='text/csv'
        )
    except Exception as e:
        current_app.logger.error(f"Download error: {e}")
        return "Download failed.", 500
