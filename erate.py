# erate.py
from flask import Blueprint, render_template, request, current_app, redirect
import requests
from typing import Optional

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

# === CONFIG ===
API_BASE_URL = "https://opendata.usac.org/api/views/jp7a-89nd"
ROWS_CSV_URL = f"{API_BASE_URL}/rows.csv"
FULL_CSV_URL = f"{API_BASE_URL}/rows.csv?accessType=DOWNLOAD"

# === HELPERS ===
def fetch_erate_csv_stream(params):
    url = ROWS_CSV_URL
    params.setdefault('$limit', '10')
    params.setdefault('$offset', '0')
    try:
        response = requests.get(url, params=params, stream=True, timeout=15)
        response.raise_for_status()
        return response
    except Exception as e:
        current_app.logger.error(f"E-Rate CSV error: {e}")
        raise

def parse_csv_rows(response, limit=10):
    rows = []
    for i, line in enumerate(response.iter_lines(decode_unicode=True)):
        if i == 0: continue
        if i > limit: break
        row = line.split(',')
        if len(row) >= 11:
            rows.append({
                'id': row[0], 'state': row[1], 'funding_year': row[2],
                'entity_name': row[3], 'address': row[4], 'zip': row[5],
                'frn': row[6], 'app_number': row[7], 'status': row[8],
                'amount': row[9], 'description': row[10]
            })
    return rows

def build_where_clause(state: Optional[str] = None, min_date: Optional[str] = None) -> str:
    conditions = []
    if state:
        conditions.append(f"billed_entity_state = '{state.upper()}'")
    if min_date:
        # Socrata ISO format: 2025-01-01T00:00:00
        conditions.append(f"last_modified_date_time >= '{min_date}T00:00:00'")
    return " AND ".join(conditions) if conditions else "1=1"

# === ROUTES ===
@erate_bp.route('/')
def erate_dashboard():
    try:
        state = request.args.get('state', 'KS')  # Default KS
        min_date = request.args.get('min_date', '2025-01-01')  # Default Jan 1, 2025

        offset = int(request.args.get('offset', 0))

        params = {
            '$where': build_where_clause(state, min_date),
            '$limit': '11',
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
            filters={'state': state, 'min_date': min_date},
            total=offset + len(table_data),
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
            filters={'state': 'KS', 'min_date': '2025-01-01'},
            has_more=False,
            next_offset=0
        )

@erate_bp.route('/download')
def download_csv():
    state = request.args.get('state', 'KS')
    min_date = request.args.get('min_date', '2025-01-01')

    params = {'$where': build_where_clause(state, min_date)}
    url = f"{ROWS_CSV_URL}?{requests.compat.urlencode(params)}"
    return redirect(url)
