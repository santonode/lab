# erate.py
from flask import Blueprint, render_template, request, current_app
import requests
import os
from typing import Dict, Any, Optional

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

# === CONFIG ===
API_BASE_URL = "https://opendata.usac.org/api/v3/views/jp7a-89nd"  # ← ADDED /v3
QUERY_URL = f"{API_BASE_URL}/query.json"
FULL_DATA_URL = f"{API_BASE_URL}/rows.json?accessType=DOWNLOAD"

ERATE_USERNAME = os.environ.get('ERATE_USERNAME', '')
ERATE_PASSWORD = os.environ.get('ERATE_PASSWORD', '')

# === HELPERS ===
def fetch_erate_data(params: Dict[str, Any] = None) -> Dict[str, Any]:
    url = QUERY_URL
    if params is None:
        params = {}
    params['accessType'] = 'DOWNLOAD'
    params.setdefault('$limit', '1000')

    auth = (ERATE_USERNAME, ERATE_PASSWORD) if ERATE_USERNAME and ERATE_PASSWORD else None

    try:
        response = requests.get(url, params=params, auth=auth, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        current_app.logger.error(f"E-Rate API error: {e}")
        raise

def format_table_data(data: Dict[str, Any]) -> list:
    rows = data.get('data', [])
    if not rows:
        return []

    formatted = []
    for row in rows:
        formatted.append({
            'id': row[0] if len(row) > 0 else '',
            'state': row[1] if len(row) > 1 else '',
            'funding_year': row[2] if len(row) > 2 else '',
            'entity_name': row[3] if len(row) > 3 else '',
            'address': row[4] if len(row) > 4 else '',
            'zip': row[5] if len(row) > 5 else '',
            'frn': row[6] if len(row) > 6 else '',
            'app_number': row[7] if len(row) > 7 else '',
            'status': row[8] if len(row) > 8 else '',
            'amount': row[9] if len(row) > 9 else '',
            'description': row[10] if len(row) > 10 else ''
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

        params = {}
        where = build_where_clause(state, year)
        params['$where'] = where

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
