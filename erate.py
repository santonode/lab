from flask import Blueprint, render_template, request, current_app
import requests
from typing import Dict, Any, Optional
from io import StringIO
import csv

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

# === CONFIG ===
API_BASE_URL = "https://opendata.usac.org/api/views/jp7a-89nd"
QUERY_URL = f"{API_BASE_URL}/query.json"  # For filtered queries
FULL_DATA_URL = f"{API_BASE_URL}/rows.csv?accessType=DOWNLOAD"  # For bulk CSV (no auth needed)

# === HELPERS ===
def fetch_erate_data(params: Dict[str, Any] = None) -> Dict[str, Any]:
    """Fetch filtered data (open – no auth needed)."""
    url = QUERY_URL
    if params is None:
        params = {}
    params.setdefault('$limit', '1000')  # Safe default (avoids bulk limits)

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        current_app.logger.error(f"E-Rate API error: {e}")
        raise

def format_table_data(data: Dict[str, Any]) -> list:
    """Format API response for table (based on Socrata schema)."""
    rows = data.get('data', [])
    if not rows:
        return []

    # Socrata schema: First row is headers, rest data
    headers = rows[0] if rows else []
    formatted_rows = []
    for row in rows[1:]:  # Skip header
        if len(row) < len(headers):
            continue
        row_dict = dict(zip(headers, row))
        formatted_rows.append({
            'id': row_dict.get('id', ''),
            'state': row_dict.get('state', ''),
            'funding_year': row_dict.get('funding_year', ''),
            'entity_name': row_dict.get('entity_name', ''),
            'address': row_dict.get('address', ''),
            'zip': row_dict.get('zip', ''),
            'frn': row_dict.get('frn', ''),
            'app_number': row_dict.get('app_number', ''),
            'status': row_dict.get('status', ''),
            'amount': row_dict.get('amount', ''),
            'description': row_dict.get('description', '')
        })
    return formatted_rows

def build_where_clause(state: Optional[str] = None, year: Optional[str] = None) -> str:
    """Build Socrata $where clause."""
    conditions = []
    if state:
        conditions.append(f"state='{state.upper()}'")
    if year:
        conditions.append(f"funding_year={year}")
    return ' AND '.join(conditions) if conditions else '1=1'

# === ROUTES ===
@erate_bp.route('/')
def erate_dashboard():
    """Main dashboard (filtered by default)."""
    try:
        state = request.args.get('state')
        year = request.args.get('year')
        load_all = request.args.get('load_all') == '1'

        params = {'$where': build_where_clause(state, year)}
        if load_all and not state and not year:
            params.pop('$limit', None)  # Full load
        else:
            params['$limit'] = '1000'  # Safe default

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
    """Download filtered or full CSV (streaming for large files)."""
    try:
        state = request.args.get('state')
        year = request.args.get('year')
        full = request.args.get('full') == '1'

        params = {}
        where = build_where_clause(state, year)
        if where != '1=1':
            params['$where'] = where

        if full and not state and not year:
            # Full bulk CSV (no auth, direct download)
            response = requests.get(FULL_DATA_URL, stream=True, timeout=300)
            response.raise_for_status()
            
            filename = f"erate_full_dataset_{int(time.time())}.csv"
            return send_file(
                io.BytesIO(response.content),
                as_attachment=True,
                download_name=filename,
                mimetype='text/csv'
            )
        else:
            # Filtered query, then to CSV
            data = fetch_erate_data(params)
            rows = data.get('data', [])
            
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(['ID', 'State', 'Funding Year', 'Entity Name', 'Address', 'ZIP', 'FRN', 'App Number', 'Status', 'Amount', 'Description'])
            
            for row in rows:
                writer.writerow(row)  # Raw rows (adjust if needed)
            
            output.seek(0)
            filename = f"erate_{state or 'all'}_{year or 'all'}.csv"
            return send_file(
                StringIO(output.getvalue()),
                as_attachment=True,
                download_name=filename,
                mimetype='text/csv'
            )
    except Exception as e:
        current_app.logger.error(f"Download error: {e}")
        return "Download failed.", 500

@erate_bp.route('/api/search')
def api_search():
    """AJAX for dynamic filtering."""
    try:
        params = request.args.to_dict()
        data = fetch_erate_data(params)
        table_data = format_table_data(data)
        return jsonify({'success': True, 'data': table_data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})
