from flask import Blueprint, render_template, request, jsonify, send_file
import requests
import os
import csv
from io import StringIO
from typing import Dict, Any, Optional

# Blueprint
erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

# === CONFIG ===
API_BASE_URL = "https://opendata.usac.org/api/views/jp7a-89nd"
FULL_DATA_URL = f"{API_BASE_URL}/rows.json?accessType=DOWNLOAD"
QUERY_URL = f"{API_BASE_URL}/query.json"
ERATE_USERNAME = os.environ.get('ERATE_USERNAME', '')
ERATE_PASSWORD = os.environ.get('ERATE_PASSWORD', '')

# === HELPERS ===
def fetch_erate_data(params: Dict[str, Any] = None, full_dataset: bool = False) -> Dict[str, Any]:
    """
    Fetch E-Rate data from OpenData API.
    - params: Query params (e.g., {'state': 'KS', 'funding_year': '2025'}).
    - full_dataset: Use /rows.json for full download, /query.json for filtered.
    """
    url = FULL_DATA_URL if full_dataset else QUERY_URL
    if params:
        params['accessType'] = 'DOWNLOAD'

    auth = (ERATE_USERNAME, ERATE_PASSWORD) if ERATE_USERNAME and ERATE_PASSWORD else None
    
    try:
        response = requests.get(url, params=params, auth=auth, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        current_app.logger.error(f"E-Rate API error: {e}")
        raise

def format_table_data(data: Dict[str, Any]) -> list:
    """
    Format API response for HTML table.
    - data: JSON from API ('data' array of [id, ...fields]).
    - Returns: List of dicts with user-friendly keys.
    """
    rows = data.get('data', [])
    if not rows:
        return []

    # Headers from first row (adjust indices as per actual schema)
    headers = [
        'ID', 'State', 'Funding Year', 'Entity Name', 'Address', 'ZIP', 
        'FRN', 'Application Number', 'Status', 'Amount', 'Description'
    ]  # Update based on actual field names from curl | jq '.[0][0]'

    formatted_rows = []
    for row in rows:
        formatted_rows.append({
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
    return formatted_rows

def build_where_clause(state: Optional[str] = None, year: Optional[str] = None) -> str:
    """Build Socrata $where clause."""
    conditions = []
    if state:
        conditions.append(f"state='{state}'")
    if year:
        conditions.append(f"funding_year={year}")
    return ' AND '.join(conditions) if conditions else ''

# === ROUTES ===
@erate_bp.route('/')
def erate_dashboard():
    """Main dashboard with filters and table."""
    try:
        params = {}
        state = request.args.get('state')
        year = request.args.get('year')
        
        if state or year:
            where_clause = build_where_clause(state, year)
            params['$where'] = where_clause
            params['$limit'] = '1000'  # Reasonable default
            data = fetch_erate_data(params, full_dataset=False)
        else:
            data = fetch_erate_data({}, full_dataset=True)  # Full dataset

        table_data = format_table_data(data)
        
        return render_template('erate.html',
                               table_data=table_data,
                               filters={'state': state, 'year': year},
                               total=len(table_data))
    except Exception as e:
        current_app.logger.error(f"E-Rate dashboard error: {e}")
        return render_template('erate.html', error=str(e), table_data=[], filters={}, total=0)

@erate_bp.route('/download')
def download_csv():
    """Download filtered results as CSV."""
    try:
        params = {}
        state = request.args.get('state')
        year = request.args.get('year')
        
        if state or year:
            where_clause = build_where_clause(state, year)
            params['$where'] = where_clause

        data = fetch_erate_data(params, full_dataset=False)
        rows = data.get('data', [])
        
        output = StringIO()
        writer = csv.writer(output)
        
        # Headers (update as per schema)
        headers = ['ID', 'State', 'Funding Year', 'Entity Name', 'Address', 'ZIP', 'FRN', 'App Number', 'Status', 'Amount', 'Description']
        writer.writerow(headers)
        
        for row in rows:
            writer.writerow([row[i] for i in range(len(headers))])  # Adjust indices
        
        output.seek(0)
        
        filename = f"erate_{state or 'all'}_{year or 'all'}.csv"
        return send_file(
            StringIO(output.getvalue()),
            as_attachment=True,
            download_name=filename,
            mimetype='text/csv'
        )
    except Exception as e:
        current_app.logger.error(f"E-Rate download error: {e}")
        abort(500)

@erate_bp.route('/api/search')
def api_search():
    """AJAX endpoint for dynamic filtering (optional)."""
    try:
        params = request.args.to_dict()
        data = fetch_erate_data(params, full_dataset=False)
        table_data = format_table_data(data)
        return jsonify({'success': True, 'data': table_data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# === INIT ===
def init_erate():
    """Optional: Test connection on startup."""
    try:
        data = fetch_erate_data({'$limit': '1'})
        current_app.logger.info(f"E-Rate initialized. Sample row count: {len(data.get('data', []))}")
    except Exception as e:
        current_app.logger.warning(f"E-Rate init failed: {e}")
