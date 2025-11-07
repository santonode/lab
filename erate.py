# erate.py
from flask import Blueprint, render_template, request, current_app, redirect
import requests  # ← IMPORT ADDED
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
            filters={'state': state, 'year': year},  # ← ALWAYS PASS
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
            filters={},  # ← DEFAULT
            has_more=False,
            next_offset=0
        )

@erate_bp.route('/download')
def download_csv():
    state = request.args.get('state')
    year = request.args.get('year')

    if not state and not year:
        return redirect(FULL_CSV_URL)

    params = {'$where': build_where_clause(state, year)}
    url = f"{ROWS_CSV_URL}?{requests.compat.urlencode(params)}"
    return redirect(url)
