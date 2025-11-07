# erate.py
from flask import Blueprint, render_template, request, current_app, redirect
import requests
from urllib.parse import quote_plus
from typing import Optional

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

# === CONFIG ===
API_BASE_URL = "https://opendata.usac.org/api/views/jp7a-89nd"
ROWS_CSV_URL = f"{API_BASE_URL}/rows.csv"
COUNT_URL = f"{API_BASE_URL}/rows.json?$select=count(*)"
FULL_CSV_URL = f"{API_BASE_URL}/rows.csv?accessType=DOWNLOAD"

# === HELPERS ===
def get_filtered_count(where: str) -> int:
    try:
        params = {'$where': where}
        response = requests.get(COUNT_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        return int(data[0]['count'])
    except Exception as e:
        current_app.logger.error(f"Count error: {e}")
        return 0

def fetch_erate_csv_stream(params):
    url = ROWS_CSV_URL
    try:
        response = requests.get(url, params=params, stream=True, timeout=15)
        response.raise_for_status()
        return response
    except Exception as e:
        current_app.logger.error(f"E-Rate CSV error: {e}")
        raise

def parse_csv_rows(response, limit=10):
    rows = []
    header = None
    for i, line_bytes in enumerate(response.iter_lines()):
        if i == 0:
            header = line_bytes.decode('utf-8').strip().split(',')
            continue
        line = line_bytes.decode('utf-8').strip()
        if not line: continue
        values = [v.strip('"') for v in line.split(',')]
        if len(values) < len(header): continue
        row = dict(zip(header, values))
        rows.append({
            'id': row.get('ID', ''),
            'state': row.get('Billed Entity State', ''),
            'funding_year': row.get('Funding Year', ''),
            'entity_name': row.get('Billed Entity Name', ''),
            'address': row.get('Billed Entity Address', ''),
            'zip': row.get('Billed Entity ZIP Code', ''),
            'frn': row.get('FRN', ''),
            'app_number': row.get('Application Number', ''),
            'status': row.get('FRN Status', ''),
            'amount': row.get('Total Committed Amount', ''),
            'description': row.get('Service Type', '')
        })
        if len(rows) >= limit:
            break
    return rows

def build_where_clause(state: Optional[str] = None, min_date: Optional[str] = None) -> str:
    conditions = []
    if state:
        conditions.append(f"`Billed Entity State` = '{state.upper()}'")
    if min_date:
        conditions.append(f"`Last Modified Date/Time` >= '{min_date}T00:00:00.000'")
    return " AND ".join(conditions) if conditions else "1=1"

# === ROUTES ===
@erate_bp.route('/')
def erate_dashboard():
    try:
        state = request.args.get('state', 'KS')
        min_date = request.args.get('min_date', '2025-01-01')
        offset = int(request.args.get('offset', 0))

        where = build_where_clause(state, min_date)
        total_filtered = get_filtered_count(where)

        params = {
            '$where': where,
            '$limit': '11',
            '$offset': str(offset),
            '$order': '`ID` ASC'
        }

        response = fetch_erate_csv_stream(params)
        all_rows = parse_csv_rows(response, limit=11)
        table_data = all_rows[:10]
        has_more = len(all_rows) > 10

        return render_template(
            'erate.html',
            table_data=table_data,
            filters={'state': state, 'min_date': min_date},
            total=offset + len(table_data),
            total_filtered=total_filtered,
            has_more=has_more,
            next_offset=offset + 10
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

@erate_bp.route('/download')
def download_csv():
    state = request.args.get('state', 'KS')
    min_date = request.args.get('min_date', '2025-01-01')
    where = build_where_clause(state, min_date)
    encoded_where = quote_plus(where)
    url = f"{ROWS_CSV_URL}?$where={encoded_where}&$order=`ID` ASC"
    return redirect(url)
