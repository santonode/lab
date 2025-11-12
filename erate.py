# erate.py — FINAL (Public dashboard + Admin-only CSV via login)
from flask import (
    Blueprint, render_template, request, redirect, url_for,
    send_file, flash, current_app, jsonify, Markup, session
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

# === LOGGING ===
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

# === BLUEPRINT — NO template_folder (uses app's templates/) ===
erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

# === DATABASE_URL ===
DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
if 'sslmode' not in DATABASE_URL.lower():
    base, dbname = DATABASE_URL.rsplit('/', 1)
    DATABASE_URL = f"{base}/{dbname}?sslmode=require"
log("Final DATABASE_URL: %s", DATABASE_URL[:50] + '...')

# === TEST DB CONNECTION ===
try:
    test_conn = psycopg.connect(DATABASE_URL, connect_timeout=5)
    with test_conn.cursor() as cur:
        cur.execute('SELECT 1')
        log("DB connection test: SUCCESS")
    test_conn.close()
except Exception as e:
    log("DB connection test: FAILED to %s", e)

# === ADMIN PASSWORD (FOR CSV DOWNLOAD ONLY) ===
ADMIN_PASS = os.getenv('ADMIN_PASS', 'defaultpass123')
log("ADMIN_PASS set: %s", "YES" if ADMIN_PASS != 'defaultpass123' else "NO (use env)")

# === ADMIN LOGIN + CSV DOWNLOAD ===
@erate_bp.route('/admin-login', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'POST':
        password = request.form.get('admin_pass', '')
        if password == ADMIN_PASS:
            session['admin_authenticated'] = True
            flash("Admin access granted. CSV download started in background.", "success")
            # Start CSV download
            if not current_app.config.get('CSV_DOWNLOAD_IN_PROGRESS'):
                current_app.config['CSV_DOWNLOAD_IN_PROGRESS'] = True
                thread = threading.Thread(target=_download_csv_background, args=(current_app._get_current_object(),))
                thread.daemon = True
                thread.start()
            return redirect(url_for('erate.dashboard'))  # Back to public dashboard
        else:
            flash("Incorrect password.", "error")
    return render_template('admin_login.html')

# === PUBLIC DASHBOARD (ANY USER) ===
@erate_bp.route('/')
def dashboard():
    log("Public dashboard accessed")
    state_filter = request.args.get('state', '').strip().upper()
    modified_after_str = request.args.get('modified_after', '').strip()
    offset = max(int(request.args.get('offset', 0)), 0)
    limit = 10

    conn = psycopg.connect(DATABASE_URL, connect_timeout=10, autocommit=True)
    try:
        with conn.cursor() as cur:
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
            cache_bust=int(time.time()),
            # Show download link only if admin
            show_download_link=session.get('admin_authenticated', False)
        )

    except Exception as e:
        log("Dashboard error: %s", e)
        return f"<pre>ERROR: {e}</pre>", 500
    finally:
        conn.close()

# === BLUEBIRD API (PUBLIC) ===
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
        }), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        log("Bluebird API error: %s", e)
        return jsonify({"error": "Service unavailable"}), 500
    finally:
        conn.close()

# === APPLICANT DETAILS API (PUBLIC) ===
@erate_bp.route('/details/<app_number>')
def details(app_number):
    conn = psycopg.connect(DATABASE_URL, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM erate WHERE app_number = %s", (app_number,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Applicant not found"}), 404

            row = row[1:]  # Skip id
            log("DETAILS ROW for %s: %s", app_number, row[:10])

            def fmt_date(dt):
                return dt.strftime('%m/%d/%Y') if isinstance(dt, datetime) else '—'
            def fmt_datetime(dt):
                return dt.strftime('%m/%d/%Y %I:%M %p') if isinstance(dt, datetime) else '—'

            data = {
                "form_nickname": row[1] or '—',
                "form_pdf": Markup(f'<a href="{row[2]}" target="_blank">View PDF</a>') if row[2] else '—',
                "funding_year": row[3] or '—',
                "fcc_status": row[4] or '—',
                "allowable_contract_date": fmt_date(row[5]),
                "created_datetime": fmt_datetime(row[6]),
                "created_by": row[7] or '—',
                "certified_datetime": fmt_datetime(row[8]),
                "certified_by": row[9] or '—',
                "last_modified_datetime": fmt_datetime(row[10]),
                "last_modified_by": row[11] or '—',
                "ben": row[12] or '—',
                "entity_name": row[13] or '—',
                "org_status": row[14] or '—',
                "org_type": row[15] or '—',
                "applicant_type": row[16] or '—',
                "website": row[17] or '—',
                "latitude": row[18],
                "longitude": row[19],
                "fcc_reg_num": row[20] or '—',
                "address1": row[21] or '',
                "address2": row[22] or '',
                "city": row[23] or '',
                "state": row[24] or '',
                "zip_code": row[25] or '',
                "zip_ext": row[26] or '',
                "email": row[27] or '—',
                "phone": row[28] or '',
                "phone_ext": row[29] or '',
                "num_eligible": row[30] if row[30] is not None else 0,
                "contact_name": row[31] or '—',
                "contact_address1": row[32] or '',
                "contact_address2": row[33] or '',
                "contact_city": row[34] or '',
                "contact_state": row[35] or '',
                "contact_zip": row[36] or '',
                "contact_zip_ext": row[37] or '',
                "contact_phone": row[38] or '',
                "contact_phone_ext": row[39] or '',
                "contact_email": row[40] or '—',
                "tech_name": row[41] or '—',
                "tech_title": row[42] or '—',
                "tech_phone": row[43] or '',
                "tech_phone_ext": row[44] or '',
                "tech_email": row[45] or '—',
                "auth_name": row[46] or '—',
                "auth_address": row[47] or '—',
                "auth_city": row[48] or '',
                "auth_state": row[49] or '',
                "auth_zip": row[50] or '',
                "auth_zip_ext": row[51] or '',
                "auth_phone": row[52] or '',
                "auth_phone_ext": row[53] or '',
                "auth_email": row[54] or '—',
                "auth_title": row[55] or '—',
                "auth_employer": row[56] or '—',
                "cat1_desc": row[57] or '—',
                "cat2_desc": row[58] or '—',
                "installment_type": row[59] or '—',
                "installment_min": row[60] if row[60] is not None else 0,
                "installment_max": row[61] if row[61] is not None else 0,
                "rfp_id": row[62] or '—',
                "state_restrictions": row[63] or '—',
                "restriction_desc": row[64] or '—',
                "statewide": row[65] or '—',
                "all_public": row[66] or '—',
                "all_nonpublic": row[67] or '—',
                "all_libraries": row[68] or '—',
                "form_version": row[69] or '—'
            }

            return jsonify(data), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        log("Details API error: %s", e)
        return jsonify({"error": "Service unavailable"}), 500
    finally:
        conn.close()

# === CSV DOWNLOAD BACKGROUND (TRIGGERED BY ADMIN LOGIN) ===
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

# === IMPORT INTERACTIVE — ADMIN REQUIRED ===
@erate_bp.route('/import-interactive', methods=['GET', 'POST'])
def import_interactive():
    if not session.get('admin_authenticated'):
        flash("Admin login required to import.", "warning")
        return redirect(url_for('erate.admin_login'))

    log("Admin: Import interactive accessed")
    if not os.path.exists(CSV_FILE):
        log("CSV not found")
        return "<h2>CSV not found: 470schema.csv</h2>", 404

    with open(CSV_FILE, 'r', encoding='utf-8-sig', newline='') as f:
        total = sum(1 for _ in csv.reader(f)) - 1

    log("CSV has %s rows (excluding header)", total)

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

# === BULK IMPORT ===
def _import_all_background(app):
    global CSV_HEADERS_LOGGED, ROW_DEBUG_COUNT
    CSV_HEADERS_LOGGED = False
    ROW_DEBUG_COUNT = 0
    log("=== IMPORT STARTED — DEBUG ENABLED ===")
    try:
        log("Bulk import started")
        with app.app_context():
            total = app.config['import_total']
            start_index = app.config['import_index']

        conn = psycopg.connect(DATABASE_URL, autocommit=False, connect_timeout=10)
        cur = conn.cursor()

        with open(CSV_FILE, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f, dialect='excel')
            log("CSV reader created with excel dialect")

            for _ in range(start_index - 1):
                try: next(reader)
                except StopIteration: break
            log("Skipped to record %s", start_index)

            batch = []
            seen_in_csv = set()
            imported = 0
            last_heartbeat = time.time()

            for row in reader:
                if time.time() - last_heartbeat > 5:
                    log("HEARTBEAT: %s rows processed", imported)
                    last_heartbeat = time.time()

                app_number = row.get('Application Number', '').strip()
                if not app_number: continue

                if app_number in seen_in_csv:
                    log("SKIPPED CSV DUPLICATE: %s", app_number)
                    continue

                batch.append(row)
                seen_in_csv.add(app_number)
                imported += 1

                if len(batch) >= 1000:
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
                    seen_in_csv = set()

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
        log("IMPORT thread CRASHED: %s", e)
        log("Traceback: %s", traceback.format_exc())
    finally:
        try: conn.close()
        except: pass
        with app.app_context():
            app.config['BULK_IMPORT_IN_PROGRESS'] = False
        log("Import thread finished")

# === VIEW LOG (ADMIN ONLY) ===
@erate_bp.route('/view-log')
def view_log():
    if not session.get('admin_authenticated'):
        flash("Admin login required.", "warning")
        return redirect(url_for('erate.admin_login'))
    log("View log requested")
    if os.path.exists(LOG_FILE):
        return send_file(LOG_FILE, mimetype='text/plain')
    return "No log file.", 404

# === RESET IMPORT (ADMIN ONLY) ===
@erate_bp.route('/reset-import', methods=['POST'])
def reset_import():
    if not session.get('admin_authenticated'):
        flash("Admin login required.", "warning")
        return redirect(url_for('erate.admin_login'))
    log("Import reset requested")
    current_app.config.update({
        'import_index': 1,
        'import_success': 0,
        'import_error': 0
    })
    flash("Import reset.", "success")
    return redirect(url_for('erate.import_interactive'))

# === FULL BLUEBIRD POP LIST + GEOCODE + _row_to_tuple + INSERT_SQL ===
# [ALL PREVIOUS CODE — UNCHANGED]
# ... (pop_data, get_bluebird_distance, _row_to_tuple, INSERT_SQL)
