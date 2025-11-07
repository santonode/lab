# erate.py
from flask import Blueprint, render_template, request, current_app, send_file
import requests
import csv
from io import StringIO
from typing import Dict, Any, Optional

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

# === CONFIG ===
API_BASE_URL = "https://opendata.usac.org/api/views/jp7a-89nd"
ROWS_JSON_URL = f"{API_BASE_URL}/rows.json"
FULL_CSV_URL = f"{API_BASE_URL}/rows.csv?accessType=DOWNLOAD"

# === HELPERS ===
def fetch_erate_data(params: Dict[str, Any] = None) -> Dict[str, Any]:
    url = ROWS_JSON_URL
    if params is None:
        params = {}
    params.setdefault('$limit', '100')  # ← ONLY 100 ROWS
    params.setdefault('$offset', '0')

    try:
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        current_app.logger.error(f"E-Rate API error: {e}")
        raise

def format_table_data(data: Dict[str, Any]) -> list:
    rows = data.get('data', [])
    if not rows or len(rows) <= 1:
        return []
    headers = rows[0]
    return [
        {
            'id': r[0] if len(r) > 0 else '',
            'state': r[1] if len(r) > 1 else '',
            'funding_year': r[2] if len(r) > 2 else '',
            'entity_name': r[3] if len(r) > 3 else '',
            'address': r[4] if len(r) > 4 else '',
            'zip': r[5] if len(r) > 5 else '',
            'frn': r[6] if len(r) > 6 else '',
            'app_number': r[7] if len(r) > 7 else '',
            'status': r[8] if len(r) > 8 else '',
            'amount': r[9] if len(r) > 9 else '',
            'description': r[10] if len(r) > 10 else ''
        }
        for r in rows[1:]
    ]

def build_where_clause(state: Optional[str] = None, year: Optional[str] = None) -> str:
    conditions = []
    if state:
        conditions.append(f"state='{state.upper()}'")
    if year:
        conditions.append(f"funding_year={year}")
    return ' AND '.join(conditions) if conditions else '1=1'

# === ROUTES ===
@erate_bp.route('/')
def erate_dashboard():
    try:
        state = request.args.get('state')
        year = request.args.get('year')
        offset = int(request.args.get('offset', 0))
        load_more = request.args.get('load_more') == '1'

        params = {
            '$where': build_where_clause(state, year),
            '$limit': '100',
            '$offset': str(offset)
        }

        data = fetch_erate_data(params)
        table_data = format_table_data(data)
        has_more = len(table_data) == 100

        return render_template(
            'erate.html',
            table_data=table_data,
            filters={'state': state, 'year': year},
            total=len(table_data) + offset,
            has_more=has_more,
            next_offset=offset + 100
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

        # Stream full filtered CSV
        response = requests.get(ROWS_JSON_URL, params=params, stream=True, timeout=300)
        response.raise_for_status()

        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(['ID','State','Year','Entity','Address','ZIP','FRN','App #','Status','Amount','Service'])

        for chunk in response.iter_lines(decode_unicode=True):
            if chunk:
                row = chunk.split('\t')  # Socrata uses TSV in stream
                if len(row) > 10:
                    writer.writerow(row[:11])

        output.seek(0)
        return send_file(
            StringIO(output.getvalue()),
            as_attachment=True,
            download_name=f"erate_{state or 'all'}_{year or 'all'}.csv",
            mimetype='text/csv'
        )
    except Exception as e:
        current_app.logger.error(f"Download error: {e}")
        return "Download failed.", 500
