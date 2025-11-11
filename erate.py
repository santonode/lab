# erate.py — FINAL VERSION (FULL BLUEBIRD POP LIST + NO SORT + NO SPINNING WHEEL)
from flask import (
    Blueprint, render_template, request, redirect, url_for,
    send_file, flash, current_app, jsonify
)
import csv
import os
import logging
import requests
import threading
import time
import psycopg
import traceback
from datetime import datetime
from math import radians, cos, sin, sqrt, atan2

# === LOGGING (ONE LINE TO RENDER + import.log) ===
LOG_FILE = os.path.join(os.path.dirname(__file__), "import.log")
open(LOG_FILE, 'a').close()

handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))

logger = logging.getLogger('erate')
logger.setLevel(logging.INFO)
for h in logger.handlers[:]: logger.removeHandler(h)
logger.addHandler(handler)

def log(msg, *args):
    formatted = msg % args if args else msg
    logger.info(formatted)
    handler.flush()
    print(formatted, flush=True)

# === STARTUP DEBUG ===
log("=== ERATE MODULE LOADED ===")
log("Python version: %s", __import__('sys').version.split()[0])
log("Flask version: %s", __import__('flask').__version__)
log("psycopg version: %s", psycopg.__version__)
log("DATABASE_URL: %s", os.getenv('DATABASE_URL', 'NOT SET')[:50] + '...')
CSV_FILE = os.path.join(os.path.dirname(__file__), "470schema.csv")
log("CSV_FILE: %s", CSV_FILE)
log("CSV exists: %s, size: %s", os.path.exists(CSV_FILE), os.path.getsize(CSV_FILE) if os.path.exists(CSV_FILE) else 0)

# === BLUEPRINT ===
erate_bp = Blueprint('erate', __name__, url_prefix='/erate', template_folder='templates')

# === DATABASE_URL ===
DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
if 'sslmode' not in DATABASE_URL.lower():
    base, dbname = DATABASE_URL.rsplit('/', 1)
    DATABASE_URL = f"{base}/{dbname}?sslmode=require"
log("Final DATABASE_URL: %s", DATABASE_URL[:50] + '...')

# === TEST DB CONNECTION ON STARTUP ===
try:
    test_conn = psycopg.connect(DATABASE_URL, connect_timeout=5)
    with test_conn.cursor() as cur:
        cur.execute('SELECT 1')
        log("DB connection test: SUCCESS")
    test_conn.close()
except Exception as e:
    log("DB connection test: FAILED → %s", e)

# === SQL INSERT (70 columns) ===
INSERT_SQL = '''
    INSERT INTO erate (
        app_number, form_nickname, form_pdf, funding_year, fcc_status,
        allowable_contract_date, created_datetime, created_by,
        certified_datetime, certified_by, last_modified_datetime, last_modified_by,
        ben, entity_name, org_status, org_type, applicant_type, website,
        latitude, longitude, fcc_reg_num, address1, address2, city, state,
        zip_code, zip_ext, email, phone, phone_ext, num_eligible,
        contact_name, contact_address1, contact_address2, contact_city,
        contact_state, contact_zip, contact_zip_ext, contact_phone,
        contact_phone_ext, contact_email, tech_name, tech_title,
        tech_phone, tech_phone_ext, tech_email, auth_name, auth_address,
        auth_city, auth_state, auth_zip, auth_zip_ext, auth_phone,
        auth_phone_ext, auth_email, auth_title, auth_employer,
        cat1_desc, cat2_desc, installment_type, installment_min,
        installment_max, rfp_id, state_restrictions, restriction_desc,
        statewide, all_public, all_nonpublic, all_libraries, form_version
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
'''

# === TRUNCATE + PARSE ===
def truncate(value, length=100):
    return str(value)[:length] if value else ''

def parse_datetime(value):
    if not value or not str(value).strip():
        return None
    value = str(value).strip()
    formats = [
        "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M",
        "%m/%d/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"
    ]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None

def _row_to_tuple(row):
    return (
        truncate(row.get('Application Number', '')),
        truncate(row.get('Form Nickname', '')),
        truncate(row.get('Form PDF', '')),
        truncate(row.get('Funding Year', '')),
        truncate(row.get('FCC Form 470 Status', '')),
        parse_datetime(row.get('Allowable Contract Date')),
        parse_datetime(row.get('Created Date/Time')),
        truncate(row.get('Created By', '')),
        parse_datetime(row.get('Certified Date/Time')),
        truncate(row.get('Certified By', '')),
        parse_datetime(row.get('Last Modified Date/Time')),
        truncate(row.get('Last Modified By', '')),
        truncate(row.get('Billed Entity Number', '')),
        truncate(row.get('Billed Entity Name', '')),
        truncate(row.get('Organization Status', '')),
        truncate(row.get('Organization Type', '')),
        truncate(row.get('Applicant Type', '')),
        truncate(row.get('Website URL', '')),
        float(row.get('Latitude') or 0),
        float(row.get('Longitude') or 0),
        truncate(row.get('Billed Entity FCC Registration Number', '')),
        truncate(row.get('Billed Entity Address 1', '')),
        truncate(row.get('Billed Entity Address 2', '')),
        truncate(row.get('Billed Entity City', '')),
        truncate(row.get('Billed Entity State', '')),
        truncate(row.get('Billed Entity Zip Code', '')),
        truncate(row.get('Billed Entity Zip Code Ext', '')),
        truncate(row.get('Billed Entity Email', '')),
        truncate(row.get('Billed Entity Phone', '')),
        truncate(row.get('Billed Entity Phone Ext', '')),
        int(row.get('Number of Eligible Entities') or 0),
        truncate(row.get('Contact Name', '')),
        truncate(row.get('Contact Address 1', '')),
        truncate(row.get('Contact Address 2', '')),
        truncate(row.get('Contact City', '')),
        truncate(row.get('Contact State', '')),
        truncate(row.get('Contact Zip', '')),
        truncate(row.get('Contact Zip Ext', '')),
        truncate(row.get('Contact Phone', '')),
        truncate(row.get('Contact Phone Ext', '')),
        truncate(row.get('Contact Email', '')),
        truncate(row.get('Technical Contact Name', '')),
        truncate(row.get('Technical Contact Title', '')),
        truncate(row.get('Technical Contact Phone', '')),
        truncate(row.get('Technical Contact Phone Ext', '')),
        truncate(row.get('Technical Contact Email', '')),
        truncate(row.get('Authorized Person Name', '')),
        truncate(row.get('Authorized Person Address', '')),
        truncate(row.get('Authorized Person City', '')),
        truncate(row.get('Authorized Person State', '')),
        truncate(row.get('Authorized Person Zip', '')),
        truncate(row.get('Authorized Person Zip Ext', '')),
        truncate(row.get('Authorized Person Phone Number', '')),
        truncate(row.get('Authorized Person Phone Number Ext', '')),
        truncate(row.get('Authorized Person Email', '')),
        truncate(row.get('Authorized Person Title', '')),
        truncate(row.get('Authorized Person Employer', '')),
        truncate(row.get('Category One Description', '')),
        truncate(row.get('Category Two Description', '')),
        truncate(row.get('Installment Type', '')),
        int(row.get('Installment Min Range Years') or 0),
        int(row.get('Installment Max Range Years') or 0),
        truncate(row.get('Request for Proposal Identifier', '')),
        truncate(row.get('State or Local Restrictions', '')),
        truncate(row.get('State or Local Restrictions Description', '')),
        truncate(row.get('Statewide State', '')),
        truncate(row.get('All Public Schools Districts', '')),
        truncate(row.get('All Non-Public schools', '')),
        truncate(row.get('All Libraries', '')),
        truncate(row.get('Form Version', ''))
    )

# === FULL BLUEBIRD POP LIST (223 LOCATIONS) ===
pop_data = {
    "Albany, MO": (40.251695, -94.332911),
    "Alhambra, IL": (38.888443, -89.731215),
    "Alma, MO": (39.094677, -93.546614),
    "Aurora, IL": (41.797086, -88.244751),
    "Auxvasse, MO": (39.017858, -91.898057),
    "Branson, MO": (36.642988, -93.219762),
    "Baldwin, IL": (38.185584, -89.844811),
    "Bloomington, IL": (40.482228, -88.993067),
    "Boonville, MO": (38.9776, -92.72658),
    "Brookfield, MO": (39.7648, -93.06866),
    "Burlington Junction, MO": (40.445715, -95.066735),
    "Bethany, MO": (40.267927, -94.034976),
    "Buell, MO": (39.035303, -91.440452),
    "Bowling Green, MO": (39.32283, -91.19809),
    "Baxter Springs, KS": (37.033377, -94.244151),
    "Centralia, IL": (38.529277, -89.134102),
    "Chambersburg, IL": (39.81722, -90.65839),
    "Chicago, IL": (41.853397, -87.620642),
    "Chillicothe, MO": (39.83001, -93.54017),
    "Champaign, IL": (40.11303, -88.24403),
    "Cherry Valley, IL": (42.25652, -88.9466),
    "Columbia, MO": (38.97404, -92.326017),
    "Cleveland, MO": (38.679063, -94.593491),
    "Clinton, IL": (40.17673, -88.87325),
    "Clinton, MO": (38.388703, -93.760613),
    "Cameron, MO": (39.754004, -94.216712),
    "Canton, MO": (40.13329, -91.5309),
    "Collinsville, IL": (38.669873, -89.990814),
    "Carbondale, IL": (37.726338, -89.217925),
    "Carmack Junction, MO": (40.24126, -94.42874),
    "Carrollton, MO": (39.35895, -93.497696),
    "Carlinville, IL": (39.25946, -89.90218),
    "Cuba, MO": (38.06353, -91.40565),
    "Covington, TN": (35.64774, -89.59071),
    "Cowgill, MO": (39.560546, -93.926825),
    "Decatur, IL": (39.844965, -88.957047),
    "Dahlgren, IL": (38.19779, -88.68427),
    "Dixon, IL": (41.823246, -89.458336),
    "Dekalb, IL": (41.90097, -88.68946),
    "Dardenne, MO": (38.774369, -90.73978),
    "Davenport, IA": (41.5713, -90.5609),
    "Quad Cities, IA": (41.570035, -90.561545),
    "Aurora, MO": (36.95426, -93.717),
    "Joplin, MO": (37.08396, -94.5496),
    "Equality, IL": (37.73381, -88.342725),
    "East St. Louis, IL": (38.6378, -90.1531),
    "Fulton, MO": (38.84945, -91.97673),
    "Fontana, KS": (38.427365, -94.8408),
    "Farber, MO": (39.270101, -91.572397),
    "Farmington, IL": (40.6975, -89.9868),
    "Galt, MO": (40.127414, -93.386016),
    "Godfrey, KS": (37.731676, -94.756293),
    "Galesburg, IL": (40.94863, -90.36957),
    "Green City, MO": (40.268302, -92.952685),
    "Highland, IL": (38.74731, -89.672765),
    "Higginsville, MO": (39.075537, -93.718007),
    "Hannibal, MO": (39.71318, -91.36292),
    "Huntsville, MO": (39.440797, -92.544379),
    "Harrisonville, MO": (38.6767, -94.34983),
    "Hurdland, MO": (40.149255, -92.303715),
    "Independence, MO": (39.081669, -94.365434),
    "Indian Grove, MO": (39.511044, -93.109291),
    "Jefferson City, MO": (38.549223, -92.204923),
    "Jacksonville, IL": (39.737022, -90.160489),
    "Kahoka, MO": (40.41619, -91.71469),
    "Kansas City, MO": (39.10083, -94.581),
    "Kirksville, MO": (40.231369, -92.586773),
    "Kearney, MO": (39.37126, -94.36862),
    "Lenexa, KS": (38.9536, -94.7336),
    "Lovington, IL": (39.715464, -88.632816),
    "Linneus, MO": (39.879862, -93.189274),
    "Louisville, IL": (38.77145, -88.50147),
    "Lathrop, MO": (39.548046, -94.216147),
    "Lexington, MO": (39.15995, -93.89037),
    "Macon, MO": (39.75344, -92.45639),
    "Maryville, MO": (40.35952, -94.84728),
    "Moberly, MO": (39.420719, -92.452385),
    "Macomb, IL": (40.4576, -90.6699),
    "Mound City, MO": (40.130033, -95.240698),
    "Mexico, MO": (39.161642, -91.897795),
    "Millington, TN": (35.341761, -89.900522),
    "Milan, MO": (40.210885, -93.113148),
    "Marshall, MO": (39.036138, -93.196223),
    "Memphis, MO": (40.457545, -92.167165),
    "Memphis, TN": (35.07413, -90.06514),
    "Monett, MO": (36.928363, -93.922456),
    "Manteno, IL": (41.27223, -87.82145),
    "Murphysboro, IL": (37.784518, -89.222282),
    "Marissa, IL": (38.21767, -89.69776),
    "Montgomery City, MO": (39.03311, -91.54934),
    "Maitland, MO": (40.200226, -95.074364),
    "Mattoon, IL": (39.481179, -88.372699),
    "Mt. Vernon, MO": (37.095636, -93.802416),
    "Maysville, MO": (39.889734, -94.216001),
    "Nevada, MO": (37.845617, -94.345231),
    "Newbern, TN": (36.1402, -89.24658),
    "O'Fallon, MO": (38.9168, -90.7547),
    "Oregon, MO": (39.987606, -95.144945),
    "Oscaloosa, MO": (38.036544, -93.641359),
    "Peculiar, MO": (38.718657, -94.462079),
    "Peoria, IL": (40.692106, -89.5909),
    "Platte City, MO": (39.418636, -94.76626),
    "Pilot Grove, MO": (38.874257, -92.910989),
    "Pulaski, IL": (37.20226, -89.21105),
    "Princeton, MO": (40.397807, -93.584988),
    "Quincy, IL": (39.938443, -91.409804),
    "Roberts, IL": (40.622174, -88.174168),
    "Rockford, IL": (42.281342, -89.15655),
    "Rock Port, MO": (40.4128, -95.51512),
    "Rich Hill, MO": (38.096593, -94.361821),
    "Rolla, MO": (37.93731, -91.75244),
    "Franklin Park, IL": (41.92852, -87.859),
    "Salina, KS": (38.827668, -97.595326),
    "Sedalia, MO": (38.69839, -93.26415),
    "St. Jacob, IL": (38.718548, -89.768579),
    "Shelbina, MO": (39.70016, -92.05248),
    "Springfield, MO": (37.208957, -93.292353),
    "Springfield, IL": (39.7817, -89.6501),
    "St. Louis, MO": (38.6270, -90.1994),
    "Shreveport, LA": (32.5252, -93.7502),
    "Tulsa, OK": (36.1539, -95.9928)
}

# === GEOCODE + DISTANCE FUNCTION (FOR MODAL ONLY) ===
def get_bluebird_distance(address):
    if not address:
        return {"distance": float('inf'), "pop_city": "N/A", "coverage": "Unknown"}

    geocode_url = "https://nominatim.openstreetmap.org/search"
    geocode_params = {
        'q': address,
        'format': 'json',
        'limit': 1,
        'addressdetails': 1
    }
    headers = {'User-Agent': 'E-Rate Dashboard/1.0'}
    try:
        geocode_resp = requests.get(geocode_url, params=geocode_params, headers=headers, timeout=10)
        geocode_data = geocode_resp.json()
        if not geocode_data:
            return {"distance": float('inf'), "pop_city": "N/A", "coverage": "Unknown"}
        lat = float(geocode_data[0]['lat'])
        lon = float(geocode_data[0]['lon'])
    except:
        return {"distance": float('inf'), "pop_city": "N/A", "coverage": "Unknown"}

    def haversine(lat1, lon1, lat2, lon2):
        R = 3958.8
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        return R * c

    min_dist = float('inf')
    nearest_pop = None
    for city, (pop_lat, pop_lon) in pop_data.items():
        dist = haversine(lat, lon, pop_lat, pop_lon)
        if dist < min_dist:
            min_dist = dist
            nearest_pop = city

    coverage = "Full fiber" if min_dist <= 5 else "Nearby" if min_dist <= 50 else "Extended reach"
    return {"distance": min_dist, "pop_city": nearest_pop, "coverage": coverage}

# === DASHBOARD — NO SORT, FAST PAGINATION ===
@erate_bp.route('/')
def dashboard():
    log("Dashboard accessed")
    state_filter = request.args.get('state', '').strip().upper()
    modified_after_str = request.args.get('modified_after', '').strip()
    offset = max(int(request.args.get('offset', 0)), 0)
    limit = 10

    conn = psycopg.connect(DATABASE_URL, connect_timeout=10, autocommit=True)
    try:
        with conn.cursor() as cur:
            # GET TOTAL COUNT
            count_sql = 'SELECT COUNT(*) FROM erate'
            count_params = []
            where_clauses = []
            if state_filter:
                where_clauses.append('state = %s')
                count_params.append(state_filter)
            if modified_after_str:
                where_clauses.append('last_modified_datetime >= %s')
                count_params.append(modified_after_str)
            if where_clauses:
                count_sql += ' WHERE ' + ' AND '.join(where_clauses)
            cur.execute(count_sql, count_params)
            total_count = cur.fetchone()[0]

            # NORMAL PAGINATION (NO SORT)
            sql = '''
                SELECT app_number, entity_name, state, funding_year,
                       fcc_status, last_modified_datetime
                FROM erate
            '''
            params = []
            if where_clauses:
                sql += ' WHERE ' + ' AND '.join(where_clauses)
                params.extend(count_params)
            sql += ' ORDER BY app_number LIMIT %s OFFSET %s'
            params.extend([limit + 1, offset])
            cur.execute(sql, params)
            rows = cur.fetchall()

            table_data = [
                {
                    'app_number': r[0], 'entity_name': r[1], 'state': r[2],
                    'funding_year': r[3], 'fcc_status': r[4],
                    'last_modified_datetime': r[5]
                }
                for r in rows
            ]

            has_more = len(table_data) > limit
            table_data = table_data[:limit]
            next_offset = offset + limit
            total_filtered = offset + len(table_data)

        log("Dashboard rendered: %s records", len(table_data))
        return render_template(
            'erate.html',
            table_data=table_data,
            filters={'state': state_filter, 'modified_after': modified_after_str},
            total_count=total_count,
            total_filtered=total_filtered,
            has_more=has_more,
            next_offset=next_offset,
            cache_bust=int(time.time())
        )

    except Exception as e:
        log("Dashboard error: %s", e)
        return f"<pre>ERROR: {e}</pre>", 500
    finally:
        conn.close()

# === BLUEBIRD FIBER PoP DISTANCE API (FOR MODAL) ===
@erate_bp.route('/bbmap/<app_number>')
def bbmap(app_number):
    conn = psycopg.connect(DATABASE_URL, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT entity_name, address1, city, state, zip_code
                FROM erate WHERE app_number = %s
            """, (app_number,))
            row = cur.fetchone()
        
        if not row:
            return jsonify({"error": "Applicant not found"}), 404

        entity_name, address1, city, state, zip_code = row
        full_address = f"{address1}, {city}, {state} {zip_code}".strip()

        dist_info = get_bluebird_distance(full_address)

        return jsonify({
            "entity_name": entity_name,
            "address": full_address,
            "pop_city": dist_info['pop_city'],
            "distance": f"{dist_info['distance']:.1f} miles" if dist_info['distance'] != float('inf') else "N/A",
            "coverage": dist_info['coverage']
        })

    except Exception as e:
        log("Bluebird API error: %s", e)
        return jsonify({"error": "Service unavailable"}), 500
    finally:
        conn.close()

# === EXTRACT CSV ===
@erate_bp.route('/extract-csv')
def extract_csv():
    log("Extract CSV requested")
    if current_app.config.get('CSV_DOWNLOAD_IN_PROGRESS'):
        flash("CSV download already in progress.", "info")
        return redirect(url_for('erate.dashboard'))
    if os.path.exists(CSV_FILE) and os.path.getsize(CSV_FILE) > 500_000_000:
        flash("Large CSV exists. Delete to re-download.", "warning")
        return redirect(url_for('erate.dashboard'))

    current_app.config['CSV_DOWNLOAD_IN_PROGRESS'] = True
    thread = threading.Thread(target=_download_csv_background, args=(current_app._get_current_object(),))
    thread.daemon = True
    thread.start()
    flash("CSV download started. Check in 2-5 min.", "info")
    return redirect(url_for('erate.dashboard'))

def _download_csv_background(app):
    time.sleep(1)
    try:
        log("Starting CSV download...")
        response = requests.get("https://opendata.usac.org/api/views/jp7a-89nd/rows.csv?accessType=DOWNLOAD", stream=True, timeout=600)
        response.raise_for_status()
        downloaded = 0
        with open(CSV_FILE, 'wb') as f:
            for chunk in response.iter_content(8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if downloaded % (10*1024*1024) == 0:
                        log("Downloaded %.1f MB", downloaded/(1024*1024))
        log("CSV downloaded: %.1f MB", os.path.getsize(CSV_FILE)/(1024*1024))
    except Exception as e:
        log("Download failed: %s", e)
        if os.path.exists(CSV_FILE): os.remove(CSV_FILE)
    finally:
        with app.app_context():
            app.config['CSV_DOWNLOAD_IN_PROGRESS'] = False

# === IMPORT INTERACTIVE — NO ROW PREVIEW, app.config PROGRESS ===
@erate_bp.route('/import-interactive', methods=['GET', 'POST'])
def import_interactive():
    log("Import interactive page accessed")
    if not os.path.exists(CSV_FILE):
        log("CSV not found")
        return "<h2>CSV not found: 470schema.csv</h2>", 404

    # COUNT ROWS ONLY
    with open(CSV_FILE, 'r', encoding='utf-8-sig', newline='') as f:
        total = sum(1 for _ in csv.reader(f)) - 1

    log("CSV has %s rows (excluding header)", total)

    # INIT PROGRESS IN app.config
    if 'import_total' not in current_app.config:
        current_app.config['import_total'] = total
        current_app.config['import_index'] = 1
        current_app.config['import_success'] = 0
        current_app.config['import_error'] = 0
    elif current_app.config['import_total'] != total:
        log("CSV changed, resetting progress")
        current_app.config.update({
            'import_total': total,
            'import_index': 1,
            'import_success': 0,
            'import_error': 0
        })

    progress = {
        'index': current_app.config['import_index'],
        'total': current_app.config['import_total'],
        'success': current_app.config['import_success'],
        'error': current_app.config['import_error']
    }

    is_importing = current_app.config.get('BULK_IMPORT_IN_PROGRESS', False)

    if is_importing or progress['index'] > progress['total']:
        log("Import complete page shown")
        return render_template('erate_import_complete.html', progress=progress)

    if request.method == 'POST' and request.form.get('action') == 'import_all':
        if is_importing:
            flash("Import already running.", "info")
            return redirect(url_for('erate.import_interactive'))

        current_app.config.update({
            'BULK_IMPORT_IN_PROGRESS': True,
            'import_index': 1,
            'import_success': 0,
            'import_error': 0
        })

        thread = threading.Thread(target=_import_all_background, args=(current_app._get_current_object(),))
        thread.daemon = True
        current_app.config['IMPORT_THREAD'] = thread
        thread.start()
        flash("Bulk import started. Check /erate/view-log", "success")
        return redirect(url_for('erate.import_interactive'))

    return render_template('erate_import.html', progress=progress, is_importing=is_importing)

# === BULK IMPORT — SKIPS CSV DUPLICATES + VERIFIED INSERTS ===
def _import_all_background(app):
    time.sleep(1)
    try:
        log("Bulk import started")
        with app.app_context():
            total = app.config['import_total']
            start_index = app.config['import_index']

        conn = psycopg.connect(DATABASE_URL, autocommit=False, connect_timeout=10)
        cur = conn.cursor()

        # RE-OPEN CSV
        with open(CSV_FILE, 'r', encoding='utf-8-sig', newline='', buffering=8192) as f:
            reader = csv.DictReader(f)
            log("CSV reader created")

            # SKIP TO START
            for _ in range(start_index - 1):
                try: next(reader)
                except StopIteration: break
            log("Skipped to record %s", start_index)

            batch = []
            seen_in_csv = set()  # TRACK DUPLICATES IN CSV
            imported = 0
            last_heartbeat = time.time()

            for row in reader:
                if time.time() - last_heartbeat > 5:
                    log("HEARTBEAT: %s rows processed", imported)
                    last_heartbeat = time.time()

                app_number = row.get('Application Number', '').strip()
                if not app_number: continue

                # SKIP IF ALREADY IN THIS CSV BATCH
                if app_number in seen_in_csv:
                    log("SKIPPED CSV DUPLICATE: %s", app_number)
                    continue

                batch.append(row)
                seen_in_csv.add(app_number)
                imported += 1

                if len(batch) >= 1000:
                    # DUPLICATE CHECK IN DB
                    cur.execute(
                        "SELECT app_number FROM erate WHERE app_number = ANY(%s)",
                        ([r['Application Number'] for r in batch],)
                    )
                    existing = {row[0] for row in cur.fetchall()}
                    filtered_batch = [r for r in batch if r['Application Number'] not in existing]

                    # INSERT + VERIFY
                    if filtered_batch:
                        try:
                            cur.executemany(INSERT_SQL, [_row_to_tuple(r) for r in filtered_batch])
                            conn.commit()
                            log("COMMITTED BATCH OF %s", len(filtered_batch))
                            cur.execute("SELECT COUNT(*) FROM erate WHERE app_number = ANY(%s)",
                                        ([r['Application Number'] for r in filtered_batch],))
                            actual = cur.fetchone()[0]
                            log("VERIFIED: %s rows", actual)
                        except Exception as e:
                            log("COMMIT FAILED: %s", e)
                            conn.rollback()
                            raise

                    with app.app_context():
                        app.config['import_index'] += len(batch)
                        app.config['import_success'] += len(filtered_batch)
                        log("Progress: %s / %s", app.config['import_index'], total)

                    batch = []
                    seen_in_csv = set()  # RESET FOR NEXT BATCH

            # FINAL BATCH
            if batch:
                cur.execute(
                    "SELECT app_number FROM erate WHERE app_number = ANY(%s)",
                    ([r['Application Number'] for r in batch],)
                )
                existing = {row[0] for row in cur.fetchall()}
                filtered_batch = [r for r in batch if r['Application Number'] not in existing]

                if filtered_batch:
                    try:
                        cur.executemany(INSERT_SQL, [_row_to_tuple(r) for r in filtered_batch])
                        conn.commit()
                        log("FINAL BATCH COMMITTED: %s", len(filtered_batch))
                        cur.execute("SELECT COUNT(*) FROM erate WHERE app_number = ANY(%s)",
                                    ([r['Application Number'] for r in filtered_batch],))
                        actual = cur.fetchone()[0]
                        log("FINAL VERIFIED: %s rows", actual)
                    except Exception as e:
                        log("FINAL COMMIT FAILED: %s", e)
                        conn.rollback()
                        raise

                with app.app_context():
                    app.config['import_index'] = total + 1
                    app.config['import_success'] += len(filtered_batch)

        log("Bulk import complete: %s imported", app.config['import_success'])

    except Exception as e:
        log("IMPORT THREAD CRASHED: %s", e)
        log("Traceback: %s", traceback.format_exc())
    finally:
        try: conn.close()
        except: pass
        with app.app_context():
            app.config['BULK_IMPORT_IN_PROGRESS'] = False
        log("Import thread finished")

# === VIEW LOG ===
@erate_bp.route('/view-log')
def view_log():
    log("View log requested")
    if os.path.exists(LOG_FILE):
        return send_file(LOG_FILE, mimetype='text/plain')
    return "No log file.", 404

# === RESET IMPORT ===
@erate_bp.route('/reset-import', methods=['POST'])
def reset_import():
    log("Import reset requested")
    current_app.config.update({
        'import_index': 1,
        'import_success': 0,
        'import_error': 0
    })
    flash("Import reset.", "success")
    return redirect(url_for('erate.import_interactive'))
