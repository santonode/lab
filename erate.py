# erate.py
from flask import Blueprint, render_template, request, current_app, send_file
import requests
import csv
from io import StringIO
from typing import Dict, Any, Optional

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

# === CONFIG ===
API_BASE_URL = "https://opendata.usac.org/api/views/jp7a-89nd"
ROWS_JSON_URL = f"{API_BASE_URL}/rows.json"  # Correct endpoint
FULL_CSV_URL = f"{API_BASE_URL}/rows.csv?accessType=DOWNLOAD"

# === HELPERS ===
def fetch_erate_data(params: Dict[str, Any] = None) -> Dict[str, Any]:
    """Fetch data using /rows.json (Socrata standard)."""
    url = ROWS_JSON_URL
    if params is None:
        params = {}
    params.setdefault('$limit', '1000')

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        current_app.logger.error(f"E-Rate API error: {e}")
        raise

def format_table_data(data: Dict[str, Any]) -> list:
    rows = data.get('data', [])
    if not rows or len(rows) <= 1:
        return []

    # First row is headers
    headers = rows[0]
    formatted = []
    for row in rows[1:]:
        if len(row) < len(headers):
            continue
        row_dict = dict(zip(headers, row))
        formatted.append({
            'id': row_dict.get('id', ''),
            'state': row_dict.get('state', ''),
            'funding_year': row_dict.get('funding_year', ''),
            'entity_name': row_dict.get('entity_name', ''),
            'address': row_dict.get('address', ''),
            'zip': row_dict.get('zip_code', ''),  # Field is 'zip_code'
            'frn': row_dict.get('frn', ''),
            'app_number': row_dict.get('application_number', ''),
            'status': row_dict.get('status', ''),
            'amount': row_dict.get('committed_amount', ''),
            'description': row_dict.get('service_type', '')
        })
    return formatted

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
        load_all = request.args.get('load_all') == '1'

        params = {'$where': build_where_clause(state, year)}
        if load_all and not state and not year:
            params.pop('$limit', None)
        else:
            params['$limit'] = '1000'

        data = fetch_erate_data(params)
        table_data = format_table_data(data)

        return render_template(
            'erate.html',
            table_data=table_data,
            filters={'state': state, 'year': year},
            total=len(table_data),
            load_all=load_all
        )
    except Exception as e:
        current_app.logger.error(f"E-Rate error: {e}")
        return render_template('erate.html', error=str(e), table_data=[], total=0, filters={}, load_all=False)

@erate_bp.route('/download')
def download_csv():
    try:
        state = request.args.get('state')
        year = request.args.get('year')
        full = request.args.get('full') == '1'

        if full and not state and not year:
            response = requests.get(FULL_CSV_URL, timeout=300)
            response.raise_for_status()
            return send_file(
                StringIO(response.text),
                as_attachment=True,
                download_name="erate_full.csv",
                mimetype='text/csv'
            )
        else:
            params = {'$where': build_where_clause(state, year)}
            data = fetch_erate_data(params)
            rows = data.get('data', [])
            if not rows:
                return "No data to download.", 400

            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(['ID', 'State', 'Year', 'Entity', 'Address', 'ZIP', 'FRN', 'App #', 'Status', 'Amount', 'Service'])
            for row in rows[1:]:
                writer.writerow(row[:11])  # Adjust as needed

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
