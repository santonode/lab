# erate.py FINAL: Bluebird + FNA Member Routes + 223 PoP Distance + KMZ Map + FULL ADMIN AUTH + POINT SYSTEM + TEXT SEARCH
# + FIXED: Applicant pin never moves
# + NEW: FNA dropdown shows top 3 closest members first with star

# === LOCAL DEV ONLY LOAD .env AND USE PRODUCTION DB ===
#from dotenv import load_dotenv
#load_dotenv()  # reads your .env with DATABASE_URL

# Optional: nicer debug prints
import logging
logging.basicConfig(level=logging.INFO)

from flask import (
    Blueprint, render_template, request, redirect, url_for,
    send_file, flash, current_app, jsonify, session, abort
)
from markupsafe import Markup  # FIXED: Markup moved here in Flask 3
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
import zipfile
import xml.etree.ElementTree as ET
import hashlib
import re
import json

from io import BytesIO
from pypdf import PdfReader
from db import get_conn
from flask import Response, stream_with_context
from flask import jsonify
from models import Erate  # ← For querying the applicant
from flask import make_response

# === EXPORT SYSTEM — ADDED HERE ===
EXPORT_DIR = "exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

# === TRUE NEAREST FIBER DISTANCE (for table column) ===
def get_nearest_fiber_distance(lat, lon, kmz_path):
    if not lat or not lon or not os.path.exists(kmz_path):
        return None

    R = 3958.8  # Earth radius in miles
    min_dist = float('inf')

    try:
        with zipfile.ZipFile(kmz_path, 'r') as kmz:
            kml_files = [f for f in kmz.namelist() if f.lower().endswith('.kml')]
            if not kml_files:
                return None
            root = ET.fromstring(kmz.read(kml_files[0]))
            ns = {'kml': 'http://www.opengis.net/kml/2.2'}

            for coord_elem in root.findall('.//kml:coordinates', ns):
                if not coord_elem.text:
                    continue
                for point in coord_elem.text.strip().split():
                    parts = point.split(',')
                    if len(parts) < 2:
                        continue
                    try:
                        p_lon, p_lat = float(parts[0]), float(parts[1])
                        dlat = radians(p_lat - lat)
                        dlon = radians(p_lon - lon)
                        a = sin(dlat/2)**2 + cos(radians(lat)) * cos(radians(p_lat)) * sin(dlon/2)**2
                        c = 2 * atan2(sqrt(a), sqrt(1-a))
                        distance = R * c
                        if distance < min_dist:
                            min_dist = distance
                    except:
                        continue
        return round(min_dist, 1) if min_dist != float('inf') else None
    except:
        return None

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

# === TEST DB CONNECTION ===
try:
    test_conn = psycopg.connect(DATABASE_URL, connect_timeout=5)
    with test_conn.cursor() as cur:
        cur.execute('SELECT 1')
        log("DB connection test: SUCCESS")
    test_conn.close()
except Exception as e:
    log("DB connection test: FAILED to %s", e)

# === POINT SYSTEM ===
def deduct_point():
    if not session.get('username'):
        return
    username = session['username']
    if username == 'guest':
        ip = request.headers.get('X-Forwarded-For', request.remote_addr)
        if ip:
            ip = ip.split(',')[0].strip()
        else:
            ip = request.remote_addr or 'unknown'
        username = f"guest_{ip.replace('.', '')}"
 
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT points FROM users WHERE username = %s", (username,))
            row = cur.fetchone()
            if not row or row[0] <= 0:
                session.clear()
                return
            new_points = row[0] - 1
            cur.execute("UPDATE users SET points = %s WHERE username = %s", (new_points, username))
            conn.commit()

# === /points ENDPOINT ===
@erate_bp.route('/points')
def points():
    if not session.get('username'):
        return jsonify({"points": 0})
    username = session['username']
    if username == 'guest':
        ip = request.headers.get('X-Forwarded-For', request.remote_addr)
        if ip:
            ip = ip.split(',')[0].strip()
        else:
            ip = request.remote_addr or 'unknown'
        username = f"guest_{ip.replace('.', '')}"
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT points FROM users WHERE username = %s", (username,))
                row = cur.fetchone()
                return jsonify({"points": row[0] if row and row[0] is not None else 0})
    except Exception as e:
        log("Points API error: %s", e)
        return jsonify({"points": 0})

# === /out-of-points ENDPOINT ===
@erate_bp.route('/out-of-points')
def out_of_points():
    if not session.get('username'):
        return jsonify({"message": "Session expired."})
    username = session['username']
    if username == 'guest':
        return jsonify({"message": "You have run out of click points and your guest account has been removed."})
    else:
        return jsonify({"message": "You have run out of click points. Email sales@santoelectronics.com to top up your account."})

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

# === PARSE DATETIME ===
def parse_datetime(value):
    if not value or not str(value).strip():
        return None
    value = str(value).strip()
    if not any(c.isdigit() for c in value):
        return None
    formats = [
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M",
        "%m/%d/%Y", "%Y-%m-%d %H:%M:%S.%f",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None

# === CSV ROW → TUPLE ===
CSV_HEADERS_LOGGED = False
ROW_DEBUG_COUNT = 0
def _row_to_tuple(row):
    global CSV_HEADERS_LOGGED, ROW_DEBUG_COUNT
    if not CSV_HEADERS_LOGGED:
        log("CSV HEADERS: %s", list(row.keys()))
        CSV_HEADERS_LOGGED = True
    if ROW_DEBUG_COUNT < 3:
        log("DEBUG ROW %s: %s", ROW_DEBUG_COUNT + 1, dict(row))
        ROW_DEBUG_COUNT += 1
    form_pdf_raw = (
        row.get('Form PDF', '') or row.get('Form PDF Link', '') or
        row.get('PDF', '') or row.get('Form PDF Path', '') or ''
    ).strip()
    base = 'http://publicdata.usac.org/'
    while form_pdf_raw.startswith(base):
        form_pdf_raw = form_pdf_raw[len(base):]
    form_pdf = f"http://publicdata.usac.org{form_pdf_raw}" if form_pdf_raw else ''
    return (
        row.get('Application Number', ''),
        row.get('Form Nickname', ''),
        form_pdf,
        row.get('Funding Year', ''),
        row.get('FCC Form 470 Status', ''),
        parse_datetime(row.get('Allowable Contract Date')),
        parse_datetime(row.get('Created Date/Time')),
        row.get('Created By', ''),
        parse_datetime(row.get('Certified Date/Time')),
        row.get('Certified By', ''),
        parse_datetime(row.get('Last Modified Date/Time')),
        row.get('Last Modified By', ''),
        row.get('Billed Entity Number', ''),
        row.get('Billed Entity Name', ''),
        row.get('Organization Status', ''),
        row.get('Organization Type', ''),
        row.get('Applicant Type', ''),
        row.get('Website URL', ''),
        float(row.get('Latitude') or 0),
        float(row.get('Longitude') or 0),
        row.get('Billed Entity FCC Registration Number', ''),
        row.get('Billed Entity Address 1', ''),
        row.get('Billed Entity Address 2', ''),
        row.get('Billed Entity City', ''),
        row.get('Billed Entity State', ''),
        row.get('Billed Entity Zip Code', ''),
        row.get('Billed Entity Zip Code Ext', ''),
        row.get('Billed Entity Email', ''),
        row.get('Billed Entity Phone', ''),
        row.get('Billed Entity Phone Ext', ''),
        int(row.get('Number of Eligible Entities') or 0),
        row.get('Contact Name', ''),
        row.get('Contact Address 1', ''),
        row.get('Contact Address 2', ''),
        row.get('Contact City', ''),
        row.get('Contact State', ''),
        row.get('Contact Zip', ''),
        row.get('Contact Zip Ext', ''),
        row.get('Contact Phone', ''),
        row.get('Contact Phone Ext', ''),
        row.get('Contact Email', ''),
        row.get('Technical Contact Name', ''),
        row.get('Technical Contact Title', ''),
        row.get('Technical Contact Phone', ''),
        row.get('Technical Contact Phone Ext', ''),
        row.get('Technical Contact Email', ''),
        row.get('Authorized Person Name', ''),
        row.get('Authorized Person Address', ''),
        row.get('Authorized Person City', ''),
        row.get('Authorized Person State', ''),
        row.get('Authorized Person Zip', ''),
        row.get('Authorized Person Zip Ext', ''),
        row.get('Authorized Person Phone Number', ''),
        row.get('Authorized Person Phone Number Ext', ''),
        row.get('Authorized Person Email', ''),
        row.get('Authorized Person Title', ''),
        row.get('Authorized Person Employer', ''),
        row.get('Category One Description', ''),
        row.get('Category Two Description', ''),
        row.get('Installment Type', ''),
        int(row.get('Installment Min Range Years') or 0),
        int(row.get('Installment Max Range Years') or 0),
        row.get('Request for Proposal Identifier', ''),
        row.get('State or Local Restrictions', ''),
        row.get('State or Local Restrictions Description', ''),
        row.get('Statewide State', ''),
        row.get('All Public Schools Districts', ''),
        row.get('All Non-Public schools', ''),
        row.get('All Libraries', ''),
        row.get('Form Version', '')
    )

# === BLUEBIRD POP LIST (223) ===
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
    "Collinsville, IL": (38.669873, -89.990125),
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
    "/St. Jacob, IL": (38.718548, -89.768579),
    "Shelbina, MO": (39.70016, -92.05248),
    "Springfield, MO": (37.208957, -93.292353),
    "Springfield, IL": (39.7817, -89.6501),
    "St. Louis, MO": (38.6270, -90.1994),
    "Shreveport, LA": (32.5252, -93.7502),
    "Tulsa, OK": (36.1539, -95.9928)
}

# === GEOCODE + DISTANCE (223 PoPs) ===
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

# === KMZ PATHS ===
KMZ_PATH_SEGRA_EAST = "SEGRA_EAST.kmz"
KMZ_PATH_SEGRA_WEST = "SEGRA_WEST.kmz"
KMZ_PATH_BLUEBIRD = os.path.join(os.path.dirname(__file__), "BBN Map KMZ 122023.kmz")
FNA_MEMBERS_DIR = os.path.join(os.path.dirname(__file__), "fna_members")
FNA_MEMBERS = {}

# === LOAD FNA MEMBERS FROM SPLIT KMZ FILES ===
def _load_fna_members():
    global FNA_MEMBERS
    if FNA_MEMBERS:
        return
    if not os.path.exists(FNA_MEMBERS_DIR):
        log("FNA members directory not found: %s", FNA_MEMBERS_DIR)
        return
    for file in os.listdir(FNA_MEMBERS_DIR):
        if file.lower().endswith('.kmz'):
            member_name = os.path.splitext(file)[0].replace('_', ' ')
            FNA_MEMBERS[member_name] = os.path.join(FNA_MEMBERS_DIR, file)
    log("Loaded %d FNA members", len(FNA_MEMBERS))

_load_fna_members()

# === GLOBAL MAP DATA (LAZY LOADED) ===
MAP_DATA = {
    "bluebird": {"pops": None, "routes": None, "loaded": False},
    "segra_east": {"pops": None, "routes": None, "loaded": False},
    "segra_west": {"pops": None, "routes": None, "loaded": False},
}

# === GENERALIZED KMZ LOADER — Bluebird + Segra cached, FNA fresh ===
def _load_kmz(arg):
    """
    arg can be:
    - "bluebird" (use cached MAP_DATA)
    - "segra_east" or "segra_west" (use cached)
    - direct path string (e.g. FNA member path — no cache)
    """
    # Check if it's a cachable provider
    cache_key = None
    if arg in ["bluebird", "segra_east", "segra_west"]:
        cache_key = arg

    # Use cache if available
    if cache_key and MAP_DATA[cache_key]["loaded"]:
        log(f"Returning cached data for {cache_key}")
        return MAP_DATA[cache_key]["pops"], MAP_DATA[cache_key]["routes"]

    # Determine path
    if arg == "bluebird":
        path = KMZ_PATH_BLUEBIRD
    elif arg == "segra_east":
        path = KMZ_PATH_SEGRA_EAST
    elif arg == "segra_west":
        path = KMZ_PATH_SEGRA_WEST
    else:
        path = arg  # direct path for FNA members — no cache

    if not os.path.exists(path):
        log("KMZ not found: %s", path)
        if cache_key:
            MAP_DATA[cache_key]["loaded"] = True
            MAP_DATA[cache_key]["pops"] = []
            MAP_DATA[cache_key]["routes"] = []
        return [], []

    pops = []
    routes = []
    pops_count = 0
    routes_count = 0
    try:
        log("Loading KMZ: %s", path)
        with zipfile.ZipFile(path, 'r') as kmz:
            kml_files = [f for f in kmz.namelist() if f.lower().endswith('.kml')]
            if not kml_files:
                log("No .kml in %s", path)
                if cache_key:
                    MAP_DATA[cache_key]["loaded"] = True
                return [], []
            kml_data = kmz.read(kml_files[0])
        root = ET.fromstring(kml_data)
        ns = {'kml': 'http://www.opengis.net/kml/2.2'}
        for placemark in root.findall('.//kml:Placemark', ns):
            name_elem = placemark.find('kml:name', ns)
            name = name_elem.text.strip() if name_elem is not None and name_elem.text else "Unnamed"
            # POINT (PoP)
            point = placemark.find('.//kml:Point/kml:coordinates', ns)
            if point is not None and point.text:
                parts = point.text.strip().split(',')
                if len(parts) >= 2:
                    try:
                        lon, lat = float(parts[0]), float(parts[1])
                        pops.append({"name": name, "lon": lon, "lat": lat})
                        pops_count += 1
                    except ValueError:
                        pass
            # LINESTRING — SUPPORT MultiGeometry
            line_strings = placemark.findall('.//kml:LineString/kml:coordinates', ns)
            if not line_strings:
                line_strings = placemark.findall('.//kml:MultiGeometry/kml:LineString/kml:coordinates', ns)
            for line in line_strings:
                if line.text:
                    coords = []
                    for pair in line.text.strip().split():
                        parts = pair.split(',')
                        if len(parts) >= 2:
                            try:
                                lon, lat = float(parts[0]), float(parts[1])
                                coords.append([lat, lon])
                            except ValueError:
                                continue
                    if len(coords) > 1:
                        routes.append({"name": name, "coords": coords})
                        routes_count += 1
        log("KMZ loaded [%s] – %d PoPs, %d routes", os.path.basename(path), pops_count, routes_count)
        # Cache if it's a cachable provider
        if cache_key:
            MAP_DATA[cache_key]["pops"] = pops
            MAP_DATA[cache_key]["routes"] = routes
            MAP_DATA[cache_key]["loaded"] = True
        return pops, routes
    except Exception as e:
        log("KMZ parse error [%s]: %s", path, e)
        if cache_key:
            MAP_DATA[cache_key]["loaded"] = True
            MAP_DATA[cache_key]["pops"] = []
            MAP_DATA[cache_key]["routes"] = []
        return [], []

# === FINAL WORKING BBMap API — FNA RANKING FIXED + TRUE DISTANCE + NO OOM ===
@erate_bp.route('/bbmap/<app_number>')
def bbmap(app_number):
    fna_member = request.args.get('fna_member')
    distance_only = request.args.get('distance_only') == '1'

    # Get user's Provider (guest = Bluebird)
    provider = 'bluebird'
    if 'username' in session:
        try:
            conn = psycopg.connect(DATABASE_URL)
            with conn.cursor() as cur:
                cur.execute('SELECT "Provider" FROM users WHERE username = %s', (session['username'],))
                row = cur.fetchone()
            conn.close()
            if row and row[0]:
                provider_raw = row[0].strip()
                if provider_raw == 'Segra EAST':
                    provider = 'segra_east'
                elif provider_raw == 'Segra WEST':
                    provider = 'segra_west'
                elif provider_raw == 'FNA Network':
                    provider = 'fna'
                elif provider_raw == 'Bluebird Network':
                    provider = 'bluebird'
                else:
                    provider = 'bluebird'
        except Exception as e:
            log("Failed to get user provider: %s", e)

    # DEBUG
    log("bbmap called — unique request ID: %s", request.args.get('request_id', 'no_id'))

    log("bbmap request: app=%s provider=%s member=%s distance_only=%s",
        app_number, provider, fna_member, distance_only)

    conn = psycopg.connect(DATABASE_URL, connect_timeout=10)
    with conn.cursor() as cur:
        cur.execute("SELECT entity_name, address1, address2, city, state, zip_code, latitude, longitude FROM erate WHERE app_number = %s", (app_number,))
        row = cur.fetchone()
    conn.close()

    if not row:
        return jsonify({"error": "Applicant not found"}), 404

    entity_name, address1, address2, city, state, zip_code, db_lat, db_lon = row
    full_address = f"{address1 or ''} {address2 or ''}, {city or ''}, {state or ''} {zip_code or ''}".strip(', ')

    applicant_lat = float(db_lat) if db_lat and str(db_lat).strip() and float(db_lat) != 0 else None
    applicant_lon = float(db_lon) if db_lon and str(db_lon).strip() and float(db_lon) != 0 else None

    if not (applicant_lat and applicant_lon):
        try:
            r = requests.get("https://nominatim.openstreetmap.org/search",
                             params={'q': full_address, 'format': 'json', 'limit': 1},
                             headers={'User-Agent': 'E-Rate/1.0'}, timeout=10)
            geo = r.json()
            if geo:
                applicant_lat = float(geo[0]['lat'])
                applicant_lon = float(geo[0]['lon'])
        except:
            pass

    final_applicant_coords = [applicant_lat, applicant_lon] if applicant_lat and applicant_lon else None

    # === SELECT CORRECT KMZ PATH FOR DISTANCE ONLY ===
    if provider == 'segra_east':
        kmz_path = KMZ_PATH_SEGRA_EAST
    elif provider == 'segra_west':
        kmz_path = KMZ_PATH_SEGRA_WEST
    elif provider == 'fna':
        if fna_member:
            clean = fna_member.lstrip('★ ').split(' (')[0].strip()
            kmz_path = FNA_MEMBERS.get(clean) or KMZ_PATH_BLUEBIRD
        else:
            # closest member for FNA ranking
            closest_path = KMZ_PATH_BLUEBIRD
            min_d = float('inf')
            for name, path in FNA_MEMBERS.items():
                if not os.path.exists(path):
                    continue
                d = get_nearest_fiber_distance(applicant_lat, applicant_lon, path)
                if d is not None and d < min_d:
                    min_d = d
                    closest_path = path
            kmz_path = closest_path
    else:
        kmz_path = KMZ_PATH_BLUEBIRD

    # === FAST PATH: Only return nearest fiber distance (for table) ===
    if distance_only:
        dist = get_nearest_fiber_distance(applicant_lat, applicant_lon, kmz_path)
        dist_str = "<1 mi" if dist and dist < 1 else f"{dist:.1f} mi" if dist else "—"
        return jsonify({"nearest_fiber_distance": dist_str})

    # === FNA RANKING ===
    if provider == "fna" and not fna_member:
        log("Calculating true closest FNA members for %s", full_address)
        def haversine(lat1, lon1, lat2, lon2):
            R = 3958.8
            dlat = radians(lat2 - lat1)
            dlon = radians(lon2 - lon1)
            a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
            c = 2 * atan2(sqrt(a), sqrt(1-a))
            return R * c
        rankings = []
        for name, path in FNA_MEMBERS.items():
            if not os.path.exists(path):
                continue
            try:
                with zipfile.ZipFile(path, 'r') as kmz:
                    kml_file = next((f for f in kmz.namelist() if f.lower().endswith('.kml')), None)
                    if not kml_file:
                        continue
                    root = ET.fromstring(kmz.read(kml_file))
                    ns = {'kml': 'http://www.opengis.net/kml/2.2'}
                    coords = []
                    for c in root.findall('.//kml:coordinates', ns):
                        if c.text:
                            for pair in c.text.strip().split():
                                p = pair.split(',')
                                if len(p) >= 2:
                                    try:
                                        lon, lat = float(p[0]), float(p[1])
                                        coords.append((lat, lon))
                                    except: pass
                    if coords and applicant_lat:
                        distances = [haversine(applicant_lat, applicant_lon, lat, lon) for lat, lon in coords]
                        rankings.append((name, min(distances)))
                    else:
                        rankings.append((name, 99999))
            except:
                rankings.append((name, 99999))
        rankings.sort(key=lambda x: x[1])
        display_list = []
        for i, (name, dist) in enumerate(rankings):
            if i < 3 and dist < 100:
                display_list.append(f"★ {name} ({dist:.0f}mi)")
            else:
                display_list.append(name)
        return jsonify({
            "fna_members": display_list,
            "pops": [], "routes": [],
            "applicant_coords": final_applicant_coords,
            "address": full_address,
            "nearest_kmz_coords": None,
            "nearest_fiber_distance": None,
            "nearest_kmz_pop": None
        })

    # === FULL MAP RESPONSE ===
    pops, routes = [], []
    nearest_kmz_coords = None
    nearest_fiber_distance = "—"

    if provider == "bluebird":
        if not MAP_DATA["bluebird"]["loaded"]:
            _load_kmz("bluebird")
        pops = MAP_DATA["bluebird"]["pops"]
        routes = MAP_DATA["bluebird"]["routes"]
    elif provider == "segra_east":
        pops, routes = _load_kmz(KMZ_PATH_SEGRA_EAST)
    elif provider == "segra_west":
        pops, routes = _load_kmz(KMZ_PATH_SEGRA_WEST)
    elif provider == "fna" and fna_member:
        clean = fna_member.lstrip('★ ').split(' (')[0].strip()
        path = FNA_MEMBERS.get(clean) or FNA_MEMBERS.get(fna_member)
        if not path or not os.path.exists(path):
            return jsonify({"error": "Member not found"}), 404
        pops, routes = _load_kmz(path)
    else:
        # fallback
        pops, routes = [], []

    # === TRUE NEAREST FIBER DISTANCE (red line on map) ===
    if final_applicant_coords and routes:
        app_lat, app_lon = final_applicant_coords
        min_d = float('inf')
        nearest_point = None
        for r in routes:
            for lat, lon in r["coords"]:
                d = ((app_lat - lat)**2 + (app_lon - lon)**2)**0.5 * 69
                if d < min_d:
                    min_d = d
                    nearest_point = [lat, lon]
        if nearest_point:
            nearest_kmz_coords = nearest_point
            nearest_fiber_distance = "<1 mi" if min_d < 1 else f"{min_d:.1f} mi"

    dist_info = get_bluebird_distance(full_address)

    return jsonify({
        "entity_name": entity_name,
        "address": full_address,
        "applicant_coords": final_applicant_coords,
        "pop_city": dist_info['pop_city'],
        "distance": f"{dist_info['distance']:.1f} miles" if dist_info['distance'] != float('inf') else "N/A",
        "coverage": dist_info['coverage'],
        "nearest_kmz_pop": "Nearest fiber",
        "nearest_kmz_coords": nearest_kmz_coords,
        "nearest_fiber_distance": nearest_fiber_distance,
        "pops": pops,
        "routes": routes,
        "network": provider,
        "fna_member": fna_member
    })

# === DASHBOARD (WITH AUTH CHECK + ADVANCED TEXT FILTER PARSING + DUAL C1/C2 SUFFIX) ===
@erate_bp.route('/')
def dashboard():
    log("Dashboard accessed")
    # GUEST USER (not logged in)
    if not session.get('username'):
        resp = make_response(render_template('erate.html',
            table_data=[],
            total_count=0,
            total_filtered=0,
            filters={},
            has_more=False,
            next_offset=0,
            offset=0,
            cache_bust=int(time.time())
        ))
        resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        resp.headers['Pragma'] = 'no-cache'
        resp.headers['Expires'] = '0'
        return resp

    # LOGGED-IN USER
    # DEDUCT POINT ON ANY FILTER
    if any(request.args.get(k) for k in ['state', 'modified_after', 'text']):
        deduct_point()

    state_filter = request.args.get('state', '').strip().upper()
    modified_after_str = request.args.get('modified_after', '').strip()
    raw_text = request.args.get('text', '').strip()
    offset = max(int(request.args.get('offset', 0)), 0)
    limit = 10

    conn = psycopg.connect(DATABASE_URL, connect_timeout=10, autocommit=True)
    try:
        with conn.cursor() as cur:
            where_clauses = []
            count_params = []
            params = []

            if state_filter:
                where_clauses.append('state = %s')
                count_params.append(state_filter)
                params.append(state_filter)

            if modified_after_str:
                where_clauses.append('last_modified_datetime >= %s')
                count_params.append(modified_after_str)
                params.append(modified_after_str)

            # === ADVANCED TEXT FILTER PARSING ===
            if raw_text:
                tokens = raw_text.split()
                c1_terms = [t[3:].strip() for t in tokens if t.lower().startswith('c1:') and len(t) > 3]
                c2_terms = [t[3:].strip() for t in tokens if t.lower().startswith('c2:') and len(t) > 3]
                plain_terms = [t.strip() for t in tokens if not t.lower().startswith(('c1:', 'c2:'))]

                has_c1_only = any(t.lower() == 'c1:' for t in tokens)
                has_c2_only = any(t.lower() == 'c2:' for t in tokens)

                search_clauses = []

                # C1 terms
                for term in c1_terms:
                    search_clauses.append("cat1_desc ILIKE %s")
                    pattern = f"%{term}%"
                    count_params.append(pattern)
                    params.append(pattern)

                # C2 terms
                for term in c2_terms:
                    search_clauses.append("cat2_desc ILIKE %s")
                    pattern = f"%{term}%"
                    count_params.append(pattern)
                    params.append(pattern)

                # Plain terms — search both
                if plain_terms:
                    combined_pattern = '%' + '%'.join(plain_terms) + '%'
                    search_clauses.append("(cat1_desc ILIKE %s OR cat2_desc ILIKE %s)")
                    count_params.extend([combined_pattern, combined_pattern])
                    params.extend([combined_pattern, combined_pattern])

                # C1: or C2: alone — has content
                if has_c1_only:
                    search_clauses.append("cat1_desc IS NOT NULL AND cat1_desc != ''")

                if has_c2_only:
                    search_clauses.append("cat2_desc IS NOT NULL AND cat2_desc != ''")

                if search_clauses:
                    where_clauses.append('(' + ' AND '.join(search_clauses) + ')')

            # Total count query
            count_sql = 'SELECT COUNT(*) FROM erate'
            if where_clauses:
                count_sql += ' WHERE ' + ' AND '.join(where_clauses)
            cur.execute(count_sql, count_params)
            total_count = cur.fetchone()[0]

            # Main data query
            sql = '''
                SELECT app_number, entity_name, state, last_modified_datetime,
                       latitude, longitude
                FROM erate
            '''
            if where_clauses:
                sql += ' WHERE ' + ' AND '.join(where_clauses)

            sql += ' ORDER BY last_modified_datetime DESC, app_number LIMIT %s OFFSET %s'
            params.extend([limit + 1, offset])
            cur.execute(sql, params)
            rows = cur.fetchall()

            table_data = [
                {
                    'app_number': r[0],
                    'entity_name': r[1],
                    'state': r[2],
                    'last_modified_datetime': r[3],
                    'latitude': float(r[4]) if r[4] else None,
                    'longitude': float(r[5]) if r[5] else None,
                }
                for r in rows
            ]

            # === ADD C1/C2 SUFFIX TO ENTITY NAME — SHOW BOTH IF BOTH FIELDS HAVE TEXT ===
            app_numbers = [row['app_number'] for row in table_data if row['app_number']]
            category_map = {}
            if app_numbers:
                try:
                    cur.execute("""
                        SELECT app_number, cat1_desc, cat2_desc
                        FROM erate
                        WHERE app_number = ANY(%s)
                    """, (app_numbers,))
                    for app_num, cat1, cat2 in cur.fetchall():
                        cats = []
                        if cat1 and str(cat1).strip():
                            cats.append('C1')
                        if cat2 and str(cat2).strip():
                            cats.append('C2')
                        if cats:
                            category_map[app_num] = ' '.join(cats)  # e.g., "C1 C2"
                except Exception as e:
                    log("C1/C2 category query failed: %s", e)

            for row in table_data:
                cat = category_map.get(row['app_number'])
                row['entity_name_display'] = f"{row['entity_name']}{' - ' + cat if cat else ''}"

            has_more = len(table_data) > limit
            table_data = table_data[:limit]
            next_offset = offset + limit
            total_filtered = offset + len(table_data)

        # Render template
        response = make_response(render_template(
            'erate.html',
            table_data=table_data,
            filters={
                'state': state_filter,
                'modified_after': modified_after_str,
                'text': raw_text
            },
            total_count=total_count,
            total_filtered=total_filtered,
            has_more=has_more,
            next_offset=next_offset,
            offset=offset,
            cache_bust=int(time.time())
        ))
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response
    except Exception as e:
        log("Dashboard error: %s", e)
        return f"<pre>ERROR: {e}</pre>", 500
    finally:
        conn.close()

# === 471 DASHBOARD — USES erate2 TABLE ===
@erate_bp.route('/erate471')
def dashboard471():
    log("471 Dashboard accessed")

    # GUEST USER (not logged in)
    if not session.get('username'):
        return redirect(url_for('erate.dashboard'))

    # Simple BEN filter
    ben_filter = request.args.get('ben', '').strip()

    limit = 10  # 10 rows per page for 471
    offset = max(int(request.args.get('offset', 0)), 0)

    conn = psycopg.connect(DATABASE_URL, connect_timeout=10, autocommit=True)
    try:
        with conn.cursor() as cur:
            # Count total
            count_sql = 'SELECT COUNT(*) FROM erate2'
            params = []
            where = []
            if ben_filter:
                where.append("billed_entity_number = %s")
                params.append(ben_filter)
            if where:
                count_sql += ' WHERE ' + ' AND '.join(where)
            cur.execute(count_sql, params)
            total_count = cur.fetchone()[0]

            # Fetch page
            sql = '''
                SELECT application_number, funding_year, organization_name,
                       status, categories_of_service, billed_entity_number,
                       total_funding_commitment_request_amount
                FROM erate2
            '''
            if where:
                sql += ' WHERE ' + ' AND '.join(where)
            sql += ' ORDER BY funding_year DESC, application_number DESC LIMIT %s OFFSET %s'
            params.extend([limit + 1, offset])
            cur.execute(sql, params)
            rows = cur.fetchall()

            table_data = [
                {
                    'application_number': r[0],
                    'funding_year': r[1],
                    'organization_name': r[2],
                    'status': r[3],
                    'categories_of_service': r[4],
                    'billed_entity_number': r[5],
                    'commitment_amount': float(r[6]) if r[6] is not None else 0.0
                }
                for r in rows
            ]

            has_more = len(table_data) > limit
            table_data = table_data[:limit]
            next_offset = offset + limit
            total_filtered = offset + len(table_data)

        return render_template(
            'erate2.html',
            table_data=table_data,
            filters={'ben': ben_filter},
            total_count=total_count,
            total_filtered=total_filtered,
            has_more=has_more,
            next_offset=next_offset,
            offset=offset,
            cache_bust=int(time.time())
        )

    except Exception as e:
        log("471 Dashboard error: %s", e)
        return f"<pre>ERROR: {e}</pre>", 500
    finally:
        conn.close()

# === APPLICANT DETAILS API – FINAL FIXED (NO MORE FORCED /SL/) ===
@erate_bp.route('/details/<app_number>')
def details(app_number):
    deduct_point()
    conn = psycopg.connect(DATABASE_URL, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM erate WHERE app_number = %s", (app_number,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Applicant not found"}), 404

            # Get column names and values
            columns = [desc[0] for desc in cur.description]
            data = dict(zip(columns, row))

            # USE THE URL DIRECTLY FROM DB — NO REPLACEMENT
            form_pdf = data.get('form_pdf', '') or ''
            if form_pdf and not form_pdf.startswith(('http://', 'https://')):
                form_pdf = 'http://' + form_pdf

            def fmt_date(dt):
                return dt.strftime('%m/%d/%Y') if isinstance(dt, datetime) else '—'
            def fmt_datetime(dt):
                return dt.strftime('%m/%d/%Y %I:%M %p') if isinstance(dt, datetime) else '—'

            return jsonify({
                "form_nickname": data.get('form_nickname') or '—',
                "form_pdf": Markup(f'<a href="{form_pdf}" target="_blank" rel="noopener">View PDF</a>') if form_pdf else '—',
                "funding_year": data.get('funding_year') or '—',
                "fcc_status": data.get('fcc_status') or '—',
                "allowable_contract_date": fmt_date(data.get('allowable_contract_date')),
                "created_datetime": fmt_datetime(data.get('created_datetime')),
                "created_by": data.get('created_by') or '—',
                "certified_datetime": fmt_datetime(data.get('certified_datetime')),
                "certified_by": data.get('certified_by') or '—',
                "last_modified_datetime": fmt_datetime(data.get('last_modified_datetime')),
                "last_modified_by": data.get('last_modified_by') or '—',
                "ben": data.get('ben') or '—',
                "entity_name": data.get('entity_name') or '—',
                "org_status": data.get('org_status') or '—',
                "org_type": data.get('org_type') or '—',
                "applicant_type": data.get('applicant_type') or '—',
                "website": data.get('website') or '—',
                "latitude": data.get('latitude'),
                "longitude": data.get('longitude'),
                "fcc_reg_num": data.get('fcc_reg_num') or '—',
                "address1": data.get('address1') or '',
                "address2": data.get('address2') or '',
                "city": data.get('city') or '',
                "state": data.get('state') or '',
                "zip_code": data.get('zip_code') or '',
                "zip_ext": data.get('zip_ext') or '',
                "email": data.get('email') or '—',
                "phone": data.get('phone') or '',
                "phone_ext": data.get('phone_ext') or '',
                "num_eligible": data.get('num_eligible') if data.get('num_eligible') is not None else 0,
                "contact_name": data.get('contact_name') or '—',
                "contact_address1": data.get('contact_address1') or '',
                "contact_address2": data.get('contact_address2') or '',
                "contact_city": data.get('contact_city') or '',
                "contact_state": data.get('contact_state') or '',
                "contact_zip": data.get('contact_zip') or '',
                "contact_zip_ext": data.get('contact_zip_ext') or '',
                "contact_phone": data.get('contact_phone') or '',
                "contact_phone_ext": data.get('contact_phone_ext') or '',
                "contact_email": data.get('contact_email') or '—',
                "tech_name": data.get('tech_name') or '—',
                "tech_title": data.get('tech_title') or '—',
                "tech_phone": data.get('tech_phone') or '',
                "tech_phone_ext": data.get('tech_phone_ext') or '',
                "tech_email": data.get('tech_email') or '—',
                "auth_name": data.get('auth_name') or '—',
                "auth_address": data.get('auth_address') or '—',
                "auth_city": data.get('auth_city') or '',
                "auth_state": data.get('auth_state') or '',
                "auth_zip": data.get('auth_zip') or '',
                "auth_zip_ext": data.get('auth_zip_ext') or '',
                "auth_phone": data.get('auth_phone') or '',
                "auth_phone_ext": data.get('auth_phone_ext') or '',
                "auth_email": data.get('auth_email') or '—',
                "auth_title": data.get('auth_title') or '—',
                "auth_employer": data.get('auth_employer') or '—',
                "cat1_desc": data.get('cat1_desc') or '—',
                "cat2_desc": data.get('cat2_desc') or '—',
                "installment_type": data.get('installment_type') or '—',
                "installment_min": data.get('installment_min') if data.get('installment_min') is not None else 0,
                "installment_max": data.get('installment_max') if data.get('installment_max') is not None else 0,
                "rfp_id": data.get('rfp_id') or '—',
                "state_restrictions": data.get('state_restrictions') or '—',
                "restriction_desc": data.get('restriction_desc') or '—',
                "statewide": data.get('statewide') or '—',
                "all_public": data.get('all_public') or '—',
                "all_nonpublic": data.get('all_nonpublic') or '—',
                "all_libraries": data.get('all_libraries') or '—',
                "form_version": data.get('form_version') or '—'
            })

    except Exception as e:
        log("Details API error: %s", e)
        return jsonify({"error": "Service unavailable"}), 500
    finally:
        conn.close()

# =================================
# === 471 APPLICANT DETAILS API (ALL 57 FIELDS + FIXED MATCH2 EXTRACTION) ===
# =================================
import requests
from io import BytesIO
from pypdf import PdfReader
import re

@erate_bp.route('/erate471/details/<app_number>')
def details471(app_number):
    log("471 Details requested for app: %s", app_number)
    conn = psycopg.connect(DATABASE_URL, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            # Get the requested row
            cur.execute("SELECT * FROM erate2 WHERE application_number = %s", (app_number,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "471 Application not found"}), 404

            columns = [desc[0] for desc in cur.description]
            data = dict(zip(columns, row))

            # Get PDF URL (prefer Original if Current is blank)
            pdf_url = data.get('form_pdf')
            if not pdf_url or not pdf_url.strip():
                cur.execute("""
                    SELECT form_pdf 
                    FROM erate2 
                    WHERE application_number = %s AND form_version = 'Original' AND form_pdf IS NOT NULL AND form_pdf != ''
                """, (app_number,))
                original_row = cur.fetchone()
                if original_row:
                    pdf_url = original_row[0].strip()

            # Extract from PDF if URL exists
            service_provider = "Not found"
            services_end_date = "Not found"
            if pdf_url:
                try:
                    response = requests.get(pdf_url, timeout=15)
                    response.raise_for_status()
                    pdf_reader = PdfReader(BytesIO(response.content))
                    text = ""
                    for page in pdf_reader.pages:
                        text += page.extract_text() + "\n"

                    # Extract Service Provider
                    sp_match = re.search(r"Service Provider\s*[:\-]?\s*([^(]+?)\s*\(SPN:\s*\d+\)", text, re.IGNORECASE)
                    if sp_match:
                        service_provider = sp_match.group(1).strip()

                    # End Date extraction — match2 first (more prevalent)
                    services_end_date = "Not found"

                    # match2: "What is the date your contract expires for the current term of the contract?"
                    # Flexible for line breaks and spacing
                    match2 = re.search(
                        r"What\s+is\s+the\s+date\s+your\s+contract\s+expires\s+for\s+the\s+current\s+term\s+of\s+the\s+contract\?\s*([A-Za-z]+\s+\d{1,2}\s*,\s*\d{4})",
                        text,
                        re.IGNORECASE | re.DOTALL
                    )
                    if match2:
                        services_end_date = match2.group(1).strip()

                    # match1: "When will the services end?"
                    if services_end_date == "Not found":
                        match1 = re.search(
                            r"When\s+will\s+the\s+services\s+end\?\s*([A-Za-z]+\s+\d{1,2}\s*,\s*\d{4})",
                            text,
                            re.IGNORECASE | re.DOTALL
                        )
                        if match1:
                            services_end_date = match1.group(1).strip()

                except Exception as e:
                    log("PDF extraction failed for %s: %s", pdf_url, e)
                    service_provider = "Error extracting"
                    services_end_date = "Error extracting"

            # Safe formatters
            def fmt_money(val):
                if val is None:
                    return "$0.00"
                try:
                    return f"${float(val):,.2f}"
                except:
                    return "$0.00"

            def fmt_date(dt):
                if dt is None:
                    return '—'
                try:
                    return dt.strftime('%m/%d/%Y %I:%M %p')
                except:
                    return '—'

            def fmt(val, default='—'):
                return val if val is not None and str(val).strip() != '' else default

            return jsonify({
                # Extracted from PDF
                "service_provider": service_provider,
                "services_end_date": services_end_date,

                # Basic Info
                "application_number": fmt(data.get('application_number')),
                "form_pdf": pdf_url or '',
                "funding_year": fmt(data.get('funding_year')),
                "billed_entity_state": fmt(data.get('billed_entity_state')),
                "form_version": fmt(data.get('form_version')),
                "window_status": fmt(data.get('window_status')),
                "nickname": fmt(data.get('nickname')),
                "status": fmt(data.get('status')),
                "categories_of_service": fmt(data.get('categories_of_service')),

                # Organization
                "organization_name": fmt(data.get('organization_name')),
                "billed_entity_address1": fmt(data.get('billed_entity_address1')),
                "billed_entity_address2": fmt(data.get('billed_entity_address2')),
                "billed_entity_city": fmt(data.get('billed_entity_city')),
                "billed_entity_zip_code": fmt(data.get('billed_entity_zip_code')),
                "billed_entity_zip_code_ext": fmt(data.get('billed_entity_zip_code_ext')),
                "billed_entity_phone": fmt(data.get('billed_entity_phone')),
                "billed_entity_phone_ext": fmt(data.get('billed_entity_phone_ext')),
                "billed_entity_email": fmt(data.get('billed_entity_email')),
                "billed_entity_number": fmt(data.get('billed_entity_number')),
                "fcc_registration_number": fmt(data.get('fcc_registration_number')),
                "applicant_type": fmt(data.get('applicant_type')),

                # Contact Person
                "contact_first_name": fmt(data.get('contact_first_name')),
                "contact_middle_initial": fmt(data.get('contact_middle_initial')),
                "contact_last_name": fmt(data.get('contact_last_name')),
                "contact_email": fmt(data.get('contact_email')),
                "contact_phone_number": fmt(data.get('contact_phone_number')),
                "contact_phone_ext": fmt(data.get('contact_phone_ext')),

                # Authorized Person
                "authorized_first_name": fmt(data.get('authorized_first_name')),
                "authorized_middle_name": fmt(data.get('authorized_middle_name')),
                "authorized_last_name": fmt(data.get('authorized_last_name')),
                "authorized_title": fmt(data.get('authorized_title')),
                "authorized_employer": fmt(data.get('authorized_employer')),
                "authorized_address_line_1": fmt(data.get('authorized_address_line_1')),
                "authorized_address_line_2": fmt(data.get('authorized_address_line_2')),
                "authorized_city": fmt(data.get('authorized_city')),
                "authorized_state": fmt(data.get('authorized_state')),
                "authorized_zip_code": fmt(data.get('authorized_zip_code')),
                "authorized_zip_code_ext": fmt(data.get('authorized_zip_code_ext')),
                "authorized_phone": fmt(data.get('authorized_phone')),
                "authorized_phone_extension": fmt(data.get('authorized_phone_extension')),
                "authorized_email": fmt(data.get('authorized_email')),

                # Certification & Enrollment
                "certified_datetime": fmt_date(data.get('certified_datetime')),
                "fulltime_enrollment": fmt(data.get('fulltime_enrollment')),
                "nslp_count": fmt(data.get('nslp_count')),
                "nslp_percentage": f"{float(data.get('nslp_percentage') or 0):.0%}",
                "urban_rural_status": fmt(data.get('urban_rural_status')),

                # Discount Rates
                "category_one_discount_rate": f"{float(data.get('category_one_discount_rate') or 0):.0%}",
                "category_two_discount_rate": f"{float(data.get('category_two_discount_rate') or 0):.0%}",
                "voice_discount_rate": f"{float(data.get('voice_discount_rate') or 0):.0%}",

                # Funding Amounts
                "pre_discount_amount": fmt_money(data.get('total_funding_year_pre_discount_eligible_amount')),
                "commitment_amount": fmt_money(data.get('total_funding_commitment_request_amount')),
                "non_discount_share": fmt_money(data.get('total_applicant_non_discount_share')),

                # Flags
                "funds_from_service_provider": fmt(data.get('funds_from_service_provider')),
                "service_provider_filed_by_billed_entity": fmt(data.get('service_provider_filed_by_billed_entity')),

                # Last Updated & Location
                "last_updated_datetime": fmt_date(data.get('last_updated_datetime')),
                "latitude": data.get('latitude'),
                "longitude": data.get('longitude')
            })
    except Exception as e:
        log("471 Details API error: %s", e)
        return jsonify({"error": "Service unavailable"}), 500
    finally:
        conn.close()

# === EXTRACT CSV, IMPORT, LOG, RESET ===
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
        log("Starting FULL CSV download...")
        url = "https://opendata.usac.org/api/views/jp7a-89nd/rows.csv?accessType=DOWNLOAD&funding_year=ALL"
        response = requests.get(url, stream=True, timeout=600)
        response.raise_for_status()
        downloaded = 0
        with open(CSV_FILE, 'wb') as f:
            for chunk in response.iter_content(8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if downloaded % (10*1024*1024) == 0:
                        log("Downloaded %.1f MB", downloaded/(1024*1024))
        log("FULL CSV downloaded: %.1f MB", os.path.getsize(CSV_FILE)/(1024*1024))
    except Exception as e:
        log("Download failed: %s", e)
        if os.path.exists(CSV_FILE):
            os.remove(CSV_FILE)
    finally:
        with app.app_context():
            app.config['CSV_DOWNLOAD_IN_PROGRESS'] = False

# =====================================================
# === IMPORT — STABLE ONLY ADDS DOES NOT HASH CHECK ===
# =====================================================
@erate_bp.route('/import-interactive', methods=['GET', 'POST'])
def import_interactive():
    log("Import interactive page accessed")
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
            imported = 0
            last_heartbeat = time.time()
            for row in reader:
                if time.time() - last_heartbeat > 5:
                    log("HEARTBEAT: %s rows processed", imported)
                    last_heartbeat = time.time()
                app_number = row.get('Application Number', '').strip()
                if not app_number: continue
                batch.append(row)
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

@erate_bp.route('/view-log')
def view_log():
    log("View log requested")
    if os.path.exists(LOG_FILE):
        return send_file(LOG_FILE, mimetype='text/plain')
    return "No log file.", 404

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

# =====================================================
# === HASH-BASED SMART IMPORT — FULL RUN MODE (SAFE) ===
# =====================================================
def run_full_hash_import(app, username):
    with app.app_context():
        log(f"=== FULL SMART HASH IMPORT STARTED (BACKGROUND) — User: {username} ===")
        start_time = time.time()

        try:
            with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            conn = psycopg.connect(DATABASE_URL)
            updated = 0

            with conn.cursor() as cur:
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='erate' AND column_name != 'app_number'")
                valid_columns = {row[0] for row in cur.fetchall()}

                for i, row in enumerate(rows, 1):
                    if i % 5000 == 0:
                        elapsed = int(time.time() - start_time)
                        log(f"PROGRESS: {i:,}/{len(rows):,} rows | Updated: {updated} | Elapsed: {elapsed}s")

                    app_number = (row.get('Applicant #') or row.get('Application Number') or '').strip()
                    if not app_number:
                        continue

                    cur.execute("SELECT 1 FROM erate WHERE app_number = %s", (app_number,))
                    if not cur.fetchone():
                        continue

                    clean_row = {}
                    for k, v in row.items():
                        db_col = k.strip()
                        if db_col in ['Application Number', 'Applicant #']:
                            continue
                        if db_col in ['Billed Entity Name', 'BEN Name']:
                            db_col = 'entity_name'
                        if db_col in ['Form Nickname', 'Nickname']:
                            db_col = 'form_nickname'
                        if db_col in valid_columns:
                            clean_row[db_col] = v or ''

                    row_hash = hashlib.md5(str(sorted(clean_row.items())).encode()).hexdigest()

                    cur.execute("SELECT row_hash FROM erate_hash WHERE app_number = %s", (app_number,))
                    db_hash = cur.fetchone()

                    if db_hash and db_hash[0] != row_hash:
                        update_parts = [f'"{k}"=%s' for k in clean_row]
                        cur.execute(f"UPDATE erate SET {', '.join(update_parts)} WHERE app_number = %s",
                                    list(clean_row.values()) + [app_number])
                        cur.execute("""INSERT INTO erate_hash (app_number, row_hash) VALUES (%s,%s) 
                                       ON CONFLICT (app_number) DO UPDATE SET row_hash = EXCLUDED.row_hash""",
                                    (app_number, row_hash))
                        updated += 1

                        if updated % 1000 == 0:
                            conn.commit()

                conn.commit()

            total_time = int(time.time() - start_time)
            log(f"FULL SMART HASH IMPORT FINISHED — Updated {updated} records in {total_time}s")

        except Exception as e:
            log(f"SMART HASH IMPORT FAILED: {e}")
        finally:
            conn.close()

@erate_bp.route('/import-hash')
def import_hash_start():
    if 'username' not in session:
        flash("Login required", "error")
        return redirect(url_for('erate.dashboard'))

    if not os.path.exists(CSV_FILE):
        flash("470schema.csv not found", "error")
        return redirect(url_for('erate.import_interactive'))

    username = session['username']
    thread = threading.Thread(target=run_full_hash_import, args=(current_app._get_current_object(), username))
    thread.daemon = True
    thread.start()

    flash("Smart Import started in background — check Render logs for live progress", "success")
    return redirect(url_for('erate.dashboard'))

# =====================================================
# === 471 IMPORT — USES erate2 TABLE (IL DATA) — NO DEDUPLICATION + MAX DEBUG ===
# =====================================================

# Path to the 471 CSV (in /src)
CSV_FILE_471 = os.path.join(os.path.dirname(__file__), "471schema.csv")

@erate_bp.route('/import471-interactive', methods=['GET', 'POST'])
def import471_interactive():
    log("471 Import interactive page accessed")
    if not os.path.exists(CSV_FILE_471):
        log("471 CSV not found: %s", CSV_FILE_471)
        return "<h2>471 CSV not found: 471schema-IL.csv</h2>", 404

    with open(CSV_FILE_471, 'r', encoding='utf-8-sig', newline='') as f:
        total = sum(1 for _ in csv.reader(f)) - 1  # exclude header
    log("471 CSV has %s rows (excluding header)", total)

    # Use separate config keys for 471 import
    if 'import471_total' not in current_app.config:
        current_app.config['import471_total'] = total
        current_app.config['import471_index'] = 1
        current_app.config['import471_success'] = 0
        current_app.config['import471_error'] = 0
    elif current_app.config['import471_total'] != total:
        log("471 CSV changed, resetting progress")
        current_app.config.update({
            'import471_total': total,
            'import471_index': 1,
            'import471_success': 0,
            'import471_error': 0
        })

    progress = {
        'index': current_app.config['import471_index'],
        'total': current_app.config['import471_total'],
        'success': current_app.config['import471_success'],
        'error': current_app.config['import471_error']
    }

    is_importing = current_app.config.get('IMPORT471_IN_PROGRESS', False)

    if is_importing or progress['index'] > progress['total']:
        return render_template('erate2_import_complete.html', progress=progress)

    if request.method == 'POST' and request.form.get('action') == 'import_all':
        if is_importing:
            flash("471 Import already running.", "info")
            return redirect(url_for('erate.import471_interactive'))

        current_app.config.update({
            'IMPORT471_IN_PROGRESS': True,
            'import471_index': 1,
            'import471_success': 0,
            'import471_error': 0
        })

        thread = threading.Thread(target=_import471_all_background, args=(current_app._get_current_object(),))
        thread.daemon = True
        current_app.config['IMPORT471_THREAD'] = thread
        thread.start()

        flash("471 Bulk import started with MAX DEBUG. Check /erate/view-log", "success")
        return redirect(url_for('erate.import471_interactive'))

    return render_template('erate2_import.html', progress=progress, is_importing=is_importing)

def _import471_all_background(app):
    log("=== 471 BULK IMPORT STARTED (MAX DEBUG) ===")
    try:
        with app.app_context():
            total = app.config['import471_total']
            start_index = app.config['import471_index']

        conn = psycopg.connect(DATABASE_URL, autocommit=False, connect_timeout=10)
        cur = conn.cursor()

        with open(CSV_FILE_471, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            log("471 CSV headers: %s", list(reader.fieldnames))

            # Skip to start_index
            for _ in range(start_index - 1):
                try:
                    next(reader)
                except StopIteration:
                    break

            batch = []
            imported = 0
            last_heartbeat = time.time()

            for row_num, row in enumerate(reader, start=start_index):
                if time.time() - last_heartbeat > 5:
                    log("471 HEARTBEAT: %s rows processed", imported)
                    last_heartbeat = time.time()

                app_number = row.get('Application Number', '').strip()
                if not app_number:
                    log("471 Skipping row %s: missing Application Number", row_num)
                    continue

                # DEBUG: Log key funding fields for first 10 rows
                if row_num < start_index + 10:
                    log("471 Row %s - App: %s | Pre-Discount: %s | Commitment: %s | Non-Discount: %s",
                        row_num, app_number,
                        row.get('Total Funding Year Pre-Discount Eligible Amount'),
                        row.get('Total Funding Commitment Request Amount'),
                        row.get('Total Applicant Non-Discount Share'))

                batch.append(row)
                imported += 1

                if len(batch) >= 1000:
                    _process_471_batch(cur, conn, app, batch, total)
                    batch = []

            # Final batch
            if batch:
                _process_471_batch(cur, conn, app, batch, total)

        log("471 Bulk import complete: %s rows inserted", app.config['import471_success'])
    except Exception as e:
        log("471 IMPORT thread CRASHED: %s", e)
        log("Traceback: %s", traceback.format_exc())
    finally:
        try:
            conn.close()
        except:
            pass
        with app.app_context():
            app.config['IMPORT471_IN_PROGRESS'] = False
        log("471 Import thread finished")

def _process_471_batch(cur, conn, app, batch, total):
    # Safe numeric conversion
    def get_numeric(key):
        val = r.get(key, '').strip().replace('$', '').replace(',', '')
        try:
            return float(val) if val else None
        except:
            return None

    values = []
    for r in batch:
        values.append((
            r.get('Application Number', ''),
            r.get('Form PDF', None) or None,
            r.get('Funding Year', ''),
            r.get('Billed Entity State', ''),
            r.get('Form Version', ''),
            r.get('Window Status', ''),
            r.get('Nickname', ''),
            r.get('Status', ''),
            r.get('Categories of Service', ''),
            r.get("Applicant's Organization Name", ''),
            r.get('Billed Entity Address1', ''),
            r.get('Billed Entity Address2', ''),
            r.get('Billed Entity City', ''),
            r.get('Billed Entity Zip Code', ''),
            r.get('Billed Entity Zip Code Ext', ''),
            r.get('Billed Entity Phone', ''),
            r.get('Billed Entity Phone Ext', ''),
            r.get('Billed Entity Email', ''),
            r.get('Billed Entity Number', ''),
            r.get('FCC Registration Number', ''),
            r.get('Applicant Type', ''),
            r.get('Contact First Name', ''),
            r.get('Contact Middle Initial', ''),
            r.get('Contact Last Name', ''),
            r.get('Contact Email', ''),
            r.get('Contact Phone Number', ''),
            r.get('Contact Phone Ext', ''),
            r.get('Authorized First Name', ''),
            r.get('Authorized Middle Name', ''),
            r.get('Authorized Last Name', ''),
            r.get('Authorized Title', ''),
            r.get('Authorized Employer', ''),
            r.get('Authorized Address Line 1', ''),
            r.get('Authorized Address Line 2', ''),
            r.get('Authorized City', ''),
            r.get('Authorized State', ''),
            r.get('Authorized Zip Code', ''),
            r.get('Authorized Zip Code Ext', ''),
            r.get('Authorized Phone', ''),
            r.get('Authorized Phone Extension', ''),
            r.get('Authorized Email', ''),
            r.get('Certified Date/Time'),  # string — Postgres casts
            get_numeric('Fulltime Enrollment'),
            get_numeric('NSLP Count'),
            get_numeric('NSLP Percentage'),
            r.get('Urban/ Rural Status', ''),
            get_numeric('Category One Discount Rate'),
            get_numeric('Category Two Discount Rate'),
            get_numeric('Voice Discount Rate'),
            get_numeric('Total Funding Year Pre-Discount Eligible Amount'),
            get_numeric('Total Funding Commitment Request Amount'),
            get_numeric('Total Applicant Non-Discount Share'),
            r.get('Funds from Service Provider', ''),
            r.get('Service Provider Filed by Billed Entity', ''),
            r.get('Last Updated Date/Time'),
            get_numeric('Latitude'),
            get_numeric('Longitude')
        ))

    if values:
        log("471 Sample insert values (first row): %s", values[0] if values else "empty")

        cur.executemany("""
            INSERT INTO erate2 (
                application_number, form_pdf, funding_year, billed_entity_state, form_version,
                window_status, nickname, status, categories_of_service, organization_name,
                billed_entity_address1, billed_entity_address2, billed_entity_city,
                billed_entity_zip_code, billed_entity_zip_code_ext, billed_entity_phone,
                billed_entity_phone_ext, billed_entity_email, billed_entity_number,
                fcc_registration_number, applicant_type, contact_first_name,
                contact_middle_initial, contact_last_name, contact_email,
                contact_phone_number, contact_phone_ext, authorized_first_name,
                authorized_middle_name, authorized_last_name, authorized_title,
                authorized_employer, authorized_address_line_1, authorized_address_line_2,
                authorized_city, authorized_state, authorized_zip_code,
                authorized_zip_code_ext, authorized_phone, authorized_phone_extension,
                authorized_email, certified_datetime, fulltime_enrollment, nslp_count,
                nslp_percentage, urban_rural_status, category_one_discount_rate,
                category_two_discount_rate, voice_discount_rate,
                total_funding_year_pre_discount_eligible_amount,
                total_funding_commitment_request_amount, total_applicant_non_discount_share,
                funds_from_service_provider, service_provider_filed_by_billed_entity,
                last_updated_datetime, latitude, longitude
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            ON CONFLICT DO NOTHING
        """, values)
        conn.commit()
        log("471 COMMITTED BATCH OF %s rows", len(values))

    with app.app_context():
        app.config['import471_index'] += len(batch)
        app.config['import471_success'] += len(values)
        log("471 Progress: %s / %s", app.config['import471_index'], total)

@erate_bp.route('/reset-import471', methods=['POST'])
def reset_import471():
    log("471 Import reset requested")
    current_app.config.update({
        'import471_index': 1,
        'import471_success': 0,
        'import471_error': 0
    })
    flash("471 Import reset.", "success")
    return redirect(url_for('erate.import471_interactive'))

# ================================================
# === AUTH SYSTEM + GUEST → DASHBOARD + LOGOUT ===
# ================================================
def hash_password(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

@erate_bp.before_request
def load_user():
    if 'username' not in session:
        session['username'] = None
        session['is_santo'] = False

@erate_bp.route('/admin', methods=['GET', 'POST'])
def admin():
    # LOGOUT
    if request.args.get('logout'):
        session.clear()
        flash("Logged out", "success")
        return redirect(url_for('erate.dashboard'))

    if request.method == 'POST':
        action = request.form.get('action')

        # REGISTER
        if action == 'register':
            username = request.form['username'].strip()
            password = request.form['password']
            if len(username) < 3 or len(password) < 4:
                flash("Username ≥3, Password ≥4", "error")
                return redirect(url_for('erate.admin'))
            try:
                with psycopg.connect(DATABASE_URL) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT id FROM users WHERE username = %s", (username,))
                        if cur.fetchone():
                            flash("Username taken", "error")
                            return redirect(url_for('erate.admin'))
                        cur.execute(
                            "INSERT INTO users (username, password, user_type, points) VALUES (%s, %s, %s, %s)",
                            (username, hash_password(password), 'Member', 100)
                        )
                    conn.commit()
            except Exception as e:
                flash(f"Register error: {e}", "error")
            return redirect(url_for('erate.admin'))

        # NORMAL LOGIN
        elif action == 'login':
            username = request.form['username'].strip()
            password = request.form['password']
            try:
                with psycopg.connect(DATABASE_URL) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT password FROM users WHERE username = %s", (username,))
                        row = cur.fetchone()
                        if row and row[0] == hash_password(password):
                            session['username'] = username
                            session['is_santo'] = (username == 'santo')
                            flash(Markup(f"Welcome back, {username}! Buy more Points <a href='https://map4.net/services-store' target='_blank' rel='noopener' style='color:#007bff; text-decoration:underline; font-weight:600;'>here</a>."), "success")
                            return redirect(url_for('erate.dashboard'))
                flash("Invalid login", "error")
            except Exception as e:
                flash(f"Login error: {e}", "error")
            return redirect(url_for('erate.admin'))

        # SUPERUSER BACKDOOR
        elif 'admin_pass' in request.form:
            if request.form['admin_pass'] == os.getenv('ADMIN_PASSWORD'):
                session['username'] = 'santo'
                session['is_santo'] = True
                flash("SANTO ADMIN ACCESS GRANTED", "success")
            else:
                flash("Invalid admin password", "error")
            return redirect(url_for('erate.admin'))

        # ADMIN ACTIONS
        if session.get('is_santo'):
            try:
                with psycopg.connect(DATABASE_URL) as conn:
                    with conn.cursor() as cur:
                        if 'delete_user' in request.form:
                            user_id = request.form['delete_user']
                            cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
                            flash("User deleted", "success")
                        elif 'edit_user_id' in request.form:
                            user_id = request.form['edit_user_id']
                            username = request.form['new_username'].strip()
                            password = request.form.get('new_password', '').strip()
                            points = int(request.form['new_points'])
                            user_type = request.form['new_user_type']
                            email = request.form.get('new_email', '').strip()
                            mystate = (request.form.get('new_mystate') or 'KS')[:2].upper()
                            provider = request.form.get('new_provider', '').strip()
                            ft = max(10, min(1000, int(request.form.get('new_ft', 100))))
                            dm = max(0.1, min(100.0, float(request.form.get('new_dm', 5.0))))
                            sets = ["username=%s", "points=%s", "user_type=%s", "ft=%s", "dm=%s", '"Email"=%s', '"MyState"=%s', '"Provider"=%s']
                            vals = [username, points, user_type, ft, dm, email, mystate, provider]
                            if password:
                                sets.append("password=%s")
                                vals.append(hash_password(password))
                            vals.append(user_id)
                            cur.execute(f"UPDATE users SET {', '.join(sets)} WHERE id = %s", vals)
                            flash("User updated", "success")
                        elif 'add_user' in request.form:
                            username = request.form['username']
                            password = request.form['password']
                            user_type = request.form['user_type']
                            cur.execute(
                                "INSERT INTO users (username, password, user_type, points) VALUES (%s, %s, %s, %s)",
                                (username, hash_password(password), user_type, 0)
                            )
                            flash("User added", "success")
                    conn.commit()
            except Exception as e:
                flash(f"Admin action failed: {e}", "error")

    # LOAD ALL USERS
    users = []
    if session.get('is_santo'):
        try:
            with psycopg.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT id, username, COALESCE(password, ''), user_type, points,
                               COALESCE("Email", ''), COALESCE("MyState", ''), COALESCE("Provider", ''),
                               COALESCE(ft, 100), COALESCE(dm, 5.0)
                        FROM users
                        ORDER BY id
                    """)
                    rows = cur.fetchall()
                    users = [
                        {
                            'id': row[0],
                            'username': row[1],
                            'password': row[2],
                            'user_type': row[3],
                            'points': row[4],
                            'Email': row[5],
                            'MyState': row[6],
                            'Provider': row[7],
                            'ft': row[8],
                            'dm': row[9]
                        }
                        for row in rows
                    ]
            flash(f"Loaded {len(users)} users", "success")
        except Exception as e:
            flash(f"DB error: {e}", "error")

    return render_template('eadmin.html', users=users, session=session)

# === Set Guest ====
@erate_bp.route('/set_guest', methods=['POST'])
def set_guest():
    ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    if ip:
        ip = ip.split(',')[0].strip()
    else:
        ip = request.remote_addr or 'unknown'
    guest_ip_name = f"guest_{ip.replace('.', '')}"
    session['username'] = 'guest'
    session['is_santo'] = False
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT points FROM users WHERE username = %s", (guest_ip_name,))
            row = cur.fetchone()
            if not row:
                cur.execute(
                    "INSERT INTO users (username, user_type, ip_address, points) VALUES (%s, %s, %s, %s)",
                    (guest_ip_name, 'Guest', ip, 25)
                )
            elif row[0] <= 0:
                pass
            conn.commit()
    return redirect(url_for('erate.dashboard'))

# === USER SETTINGS API – FULL 2025 VERSION (supports Email, State, Provider) ===
@erate_bp.route('/user_settings', methods=['GET', 'POST'])
def user_settings():
    username = session.get('username') or ''

    # BLOCK GUESTS
    if username.startswith('guest_'):
        return jsonify({"error": "Settings disabled for guest accounts"}), 403
        
    if not session.get('username'):
        return jsonify({"ft": 100, "dm": 5.0})

    username = session['username']
    if username.startswith('guest_'):
        return jsonify({"ft": 100, "dm": 5.0})

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            if request.method == 'POST':
                data = request.form
                password = data.get('password', '').strip()
                ft = max(10, min(1000, int(data.get('ft', 100))))
                dm = max(0.0, min(100.0, float(data.get('dm', 5.0))))
                email = data.get('email', '').strip()
                mystate = (data.get('mystate', '')[:2] or '').upper()
                provider = data.get('provider', '').strip()

                # Build dynamic update
                sets = ['ft = %s', 'dm = %s', '"Email" = %s', '"MyState" = %s', '"Provider" = %s']
                vals = [ft, dm, email, mystate, provider]

                if password:
                    sets.append('password = %s')
                    vals.append(hash_password(password))

                vals.append(username)

                cur.execute(f"UPDATE users SET {', '.join(sets)} WHERE username = %s", vals)
                conn.commit()
                return jsonify({"status": "success"})

            # GET – return all values
            cur.execute('SELECT ft, dm, "Email", "MyState", "Provider" FROM users WHERE username = %s', (username,))
            row = cur.fetchone()
            if row:
                return jsonify({
                    "ft": row[0] if row[0] is not None else 100,
                    "dm": float(row[1]) if row[1] is not None else 5.0,
                    "Email": row[2] or '',
                    "MyState": row[3] or '',
                    "Provider": row[4] or ''
                })
            else:
                return jsonify({"ft": 100, "dm": 5.0, "Email": "", "MyState": "", "Provider": ""})

# === DYNAMIC COVERAGE REPORT — PURE DATA ONLY (FOR MODAL) ===
@erate_bp.route('/coverage-report')
def coverage_report():
    view = request.args.get('view', 'by_provider')  # by_provider or by_state

    import zipfile
    import xml.etree.ElementTree as ET
    from collections import defaultdict
    import os

    # === STATE BOUNDS ===
    STATE_BOUNDS = {
        "AL": (30.2, 35.0, -88.5, -84.9), "AK": (51.2, 71.4, -179.2, -129.9),
        "AZ": (31.3, 37.0, -114.8, -109.0), "AR": (33.0, 36.5, -94.6, -89.6),
        "CA": (32.5, 42.0, -124.4, -114.1), "CO": (37.0, 41.0, -109.1, -102.0),
        "CT": (40.9, 42.1, -73.7, -71.8), "DE": (38.4, 39.8, -75.8, -75.0),
        "FL": (24.5, 31.0, -87.6, -80.0), "GA": (30.4, 35.0, -85.6, -80.8),
        "ID": (42.0, 49.0, -117.0, -111.0), "IL": (37.0, 42.5, -91.5, -87.5),
        "IN": (37.8, 41.8, -88.1, -84.8), "IA": (40.4, 43.5, -96.6, -90.1),
        "KS": (37.0, 40.0, -102.1, -94.6), "KY": (36.5, 39.1, -89.6, -81.9),
        "LA": (28.9, 33.0, -94.0, -88.8), "ME": (43.1, 47.5, -71.1, -66.9),
        "MD": (37.9, 39.7, -79.5, -75.0), "MA": (41.2, 42.9, -73.5, -69.9),
        "MI": (41.7, 48.3, -90.4, -82.4), "MN": (43.5, 49.4, -97.2, -89.5),
        "MS": (30.2, 35.0, -91.7, -88.1), "MO": (36.0, 40.6, -95.8, -89.1),
        "MT": (44.4, 49.0, -116.0, -104.0), "NE": (40.0, 43.0, -104.1, -95.3),
        "NV": (35.0, 42.0, -120.0, -114.0), "NH": (42.7, 45.3, -72.6, -70.6),
        "NJ": (38.9, 41.4, -75.6, -73.9), "NM": (31.3, 37.0, -109.1, -103.0),
        "NY": (40.5, 45.0, -79.8, -71.9), "NC": (33.8, 36.6, -84.3, -75.4),
        "ND": (45.9, 49.0, -104.1, -96.5), "OH": (38.4, 41.9, -84.8, -80.5),
        "OK": (33.6, 37.0, -103.0, -94.4), "OR": (42.0, 46.3, -124.6, -116.5),
        "PA": (39.7, 42.3, -80.6, -74.7), "RI": (41.1, 42.0, -71.9, -71.1),
        "SC": (32.0, 35.2, -83.4, -78.5), "SD": (42.5, 45.9, -104.1, -96.5),
        "TN": (34.9, 36.7, -90.3, -81.6), "TX": (25.8, 36.5, -106.6, -93.5),
        "UT": (37.0, 42.0, -114.1, -109.0), "VT": (42.7, 45.0, -73.4, -71.5),
        "VA": (36.5, 39.5, -83.7, -75.2), "WA": (45.5, 49.0, -124.8, -116.9),
        "WV": (37.2, 40.6, -82.6, -77.7), "WI": (42.5, 47.1, -92.9, -86.8),
        "WY": (41.0, 45.0, -111.1, -104.1)
    }

    def point_in_state(lat, lon):
        for state, (min_lat, max_lat, min_lon, max_lon) in STATE_BOUNDS.items():
            if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
                return state
        return None

    # Build coverage
    provider_to_states = defaultdict(set)
    state_to_providers = defaultdict(set)

    # Bluebird Network
    if os.path.exists(KMZ_PATH_BLUEBIRD):
        with zipfile.ZipFile(KMZ_PATH_BLUEBIRD, 'r') as kmz:
            kml_files = [f for f in kmz.namelist() if f.lower().endswith('.kml')]
            if kml_files:
                root = ET.fromstring(kmz.read(kml_files[0]))
                ns = {'kml': 'http://www.opengis.net/kml/2.2'}
                for coord_elem in root.findall('.//kml:coordinates', ns):
                    if coord_elem.text:
                        for point in coord_elem.text.strip().split():
                            parts = point.split(',')
                            if len(parts) >= 2:
                                try:
                                    lon, lat = float(parts[0]), float(parts[1])
                                    state = point_in_state(lat, lon)
                                    if state:
                                        provider_to_states["Bluebird Network"].add(state)
                                        state_to_providers[state].add("Bluebird Network")
                                except:
                                    pass

    # FNA Members
    for filename in os.listdir(FNA_MEMBERS_DIR):
        if not filename.lower().endswith('.kmz'):
            continue
        member_name = os.path.splitext(filename)[0].replace('_', ' ').strip()
        path = os.path.join(FNA_MEMBERS_DIR, filename)
        if not os.path.exists(path):
            continue
        try:
            with zipfile.ZipFile(path, 'r') as kmz:
                kml_files = [f for f in kmz.namelist() if f.lower().endswith('.kml')]
                if not kml_files:
                    continue
                root = ET.fromstring(kmz.read(kml_files[0]))
                ns = {'kml': 'http://www.opengis.net/kml/2.2'}
                for coord_elem in root.findall('.//kml:coordinates', ns):
                    if coord_elem.text:
                        for point in coord_elem.text.strip().split():
                            parts = point.split(',')
                            if len(parts) >= 2:
                                try:
                                    lon, lat = float(parts[0]), float(parts[1])
                                    state = point_in_state(lat, lon)
                                    if state:
                                        provider_to_states[member_name].add(state)
                                        state_to_providers[state].add(member_name)
                                except:
                                    pass
        except:
            pass

    # === RETURN PURE LIST — NO HEADERS, NO BUTTONS ===
    lines = []
    if view == 'by_state':
        for state in sorted(state_to_providers.keys()):
            providers = sorted(state_to_providers[state])
            lines.append(f"<strong>{state}</strong>: {', '.join(providers) or 'None'}")
    else:
        for provider in sorted(provider_to_states.keys()):
            states = sorted(provider_to_states[provider])
            lines.append(f"<strong>{provider}</strong>: {', '.join(states) or 'None'}")

    return "<br>".join(lines), 200, {'Content-Type': 'text/html; charset=utf-8'}

# =======================================================
# === FULL NATIONAL MAP =================================
# =======================================================
@erate_bp.route('/coverage-map-data')
def coverage_map_data():
    print("\n=== NATIONAL FIBER MAP – NDJSON STREAMING v4 – INSTANT RENDER ===")

    def process_kmz(kmz_path, provider_name, color):
        if not os.path.exists(kmz_path):
            print(f" [MISSING] {kmz_path}")
            return

        try:
            with zipfile.ZipFile(kmz_path, 'r') as z:
                kml_files = [f for f in z.namelist() if f.lower().endswith('.kml')]
                if not kml_files:
                    return

                root = ET.fromstring(z.read(kml_files[0]))
                ns = {'kml': 'http://www.opengis.net/kml/2.2'}
                added = 0

                # Standard LineString parsing
                for coord_elem in root.findall('.//kml:LineString/kml:coordinates', ns):
                    if not coord_elem.text:
                        continue
                    coords = []
                    for token in coord_elem.text.strip().split():
                        parts = token.split(',')
                        if len(parts) >= 2:
                            try:
                                lon = float(parts[0])
                                lat = float(parts[1])
                                coords.append([lat, lon])
                            except:
                                continue
                    if len(coords) >= 2:
                        line = {"name": provider_name, "color": color, "coords": coords}
                        yield json.dumps(line, separators=(',', ':')) + '\n'
                        added += 1

                # Segra West gx:Track fallback
                if added == 0 and 'segra' in provider_name.lower() and 'west' in provider_name.lower():
                    gx_ns = 'http://www.google.com/kml/ext/2.2'
                    for track in root.findall(f'.//{{{gx_ns}}}Track'):
                        coords = []
                        for coord in track.findall(f'{{{gx_ns}}}coord'):
                            if coord.text:
                                parts = coord.text.strip().split()
                                if len(parts) >= 2:
                                    try:
                                        lon = float(parts[0])
                                        lat = float(parts[1])
                                        coords.append([lat, lon])
                                    except:
                                        continue
                            if len(coords) >= 2:
                                line = {"name": provider_name, "color": color, "coords": coords}
                                yield json.dumps(line, separators=(',', ':')) + '\n'
                                added += 1

                print(f" → {provider_name}: {added} lines streamed")
        except Exception as e:
            print(f" [ERROR] {kmz_path}: {e}")

    def generate():
        if os.path.exists(KMZ_PATH_BLUEBIRD):
            yield from process_kmz(KMZ_PATH_BLUEBIRD, "Bluebird Network", "#0066cc")

        colors = ["#dc3545","#28a745","#fd7e14","#6f42c1","#20c997","#e83e8c","#6610f2","#17a2b8","#ffc107","#6c757d"]
        idx = 0
        if os.path.isdir(FNA_MEMBERS_DIR):
            for f in sorted(os.listdir(FNA_MEMBERS_DIR)):
                if f.lower().endswith('.kmz'):
                    name = os.path.splitext(f)[0].replace('_', ' ').title()
                    yield from process_kmz(os.path.join(FNA_MEMBERS_DIR, f), name, colors[idx % len(colors)])
                    idx += 1

    return Response(generate(), mimetype='application/x-ndjson')


# =======================================================
# === NEW NATIONAL MAP =================================
# =======================================================
@erate_bp.route('/national-map')
def national_map():
    return render_template('local_test.html')

@erate_bp.route('/stream-national')
def stream_national():
    print("=== STREAMING NATIONAL FIBER MAP — STATE + PROVIDER FILTER ===")
    requested_state = request.args.get('state', '').upper()
    requested_provider = request.args.get('provider', '').strip().lower()  # normalize to lowercase for comparison
    print(f"State: '{requested_state}' | Provider: '{requested_provider}'")

    import zipfile
    import xml.etree.ElementTree as ET
    import json

    def stream_kmz(path, name, color):
        if not os.path.exists(path):
            print(f"Missing KMZ: {path}")
            return
        try:
            with zipfile.ZipFile(path, 'r') as z:
                kml_files = [f for f in z.namelist() if f.lower().endswith('.kml')]
                if not kml_files:
                    return
                root = ET.fromstring(z.read(kml_files[0]))
                ns = {'kml': 'http://www.opengis.net/kml/2.2'}
                count = 0
                for coords in root.findall('.//kml:LineString/kml:coordinates', ns):
                    if not coords.text:
                        continue
                    points = []
                    for token in coords.text.strip().split():
                        p = token.split(',')
                        if len(p) >= 2:
                            try:
                                lon, lat = float(p[0]), float(p[1])
                                points.append([lat, lon])
                            except:
                                continue
                    if len(points) >= 2:
                        yield json.dumps({"name": name, "color": color, "coords": points}) + "\n"
                        count += 1
                if count:
                    print(f" → {name}: {count} lines (KMZ)")
        except Exception as e:
            print(f"KMZ error {path}: {e}")

    def stream_kml(path, name, color):
        if not os.path.exists(path):
            print(f"Missing KML: {path}")
            return
        try:
            with open(path, 'r', encoding='utf-8') as f:
                root = ET.fromstring(f.read())
            ns = {'kml': 'http://www.opengis.net/kml/2.2'}
            count = 0
            for coords in root.findall('.//kml:LineString/kml:coordinates', ns):
                if not coords.text:
                    continue
                points = []
                for token in coords.text.strip().split():
                    p = token.split(',')
                    if len(p) >= 2:
                        try:
                            lon, lat = float(p[0]), float(p[1])
                            points.append([lat, lon])
                        except:
                            continue
                if len(points) >= 2:
                    yield json.dumps({"name": name, "color": color, "coords": points}) + "\n"
                    count += 1
            if count:
                print(f" → {name}: {count} lines (KML)")
        except Exception as e:
            print(f"KML error {path}: {e}")

    def generate():
        # BLUEBIRD — show if no provider or Bluebird selected
        if not requested_provider or "bluebird" in requested_provider:
            if os.path.exists("BBN Map KMZ 122023.kmz"):
                print("STREAMING BLUEBIRD")
                yield from stream_kmz("BBN Map KMZ 122023.kmz", "Bluebird Network", "#0066cc")

        # SEGRA EAST — show if no provider or Segra East selected
        if not requested_provider or "segra east" in requested_provider:
            if os.path.exists("SEGRA_EAST.kmz"):
                print("STREAMING SEGRA EAST")
                yield from stream_kmz("SEGRA_EAST.kmz", "Segra East", "#ff6600")  # Orange

        # SEGRA WEST — show if no provider or Segra West selected
        if not requested_provider or "segra west" in requested_provider:
            if os.path.exists("SEGRA_WEST.kmz"):
                print("STREAMING SEGRA WEST")
                yield from stream_kmz("SEGRA_WEST.kmz", "Segra West", "#ffaa00")  # Lighter orange

        # CDT — show only if no provider or CDT selected
        if not requested_provider or requested_provider == "cdt":
            if os.path.exists("CDT.kml"):
                print("STREAMING CDT.kml")
                yield from stream_kml("CDT.kml", "CDT", "#00ff00")

        # FNA MEMBERS — filter by state and provider
        fna_dir = "fna_members"
        if os.path.isdir(fna_dir):
            colors = ["#dc3545","#28a745","#fd7e14","#6f42c1","#20c997","#e83e8c","#6610f2","#17a2b8","#ffc107","#6c757d"]
            idx = 0
            for f in sorted(os.listdir(fna_dir)):
                if f.lower().endswith('.kmz'):
                    path = os.path.join(fna_dir, f)
                    name = os.path.splitext(f)[0].replace('_', ' ').title()
                    # STATE FILTER
                    if requested_state and requested_state not in name.upper():
                        continue
                    # PROVIDER FILTER
                    if requested_provider and requested_provider not in name.lower():
                        continue
                    yield from stream_kmz(path, name, colors[idx % len(colors)])
                    idx += 1

    return Response(generate(), mimetype='application/x-ndjson')

# =======================================================
# === RETURN STATE BOUNDS  =================================
# =======================================================
@erate_bp.route('/state-bounds')
def state_bounds():
    bounds = {
        "AL": [[30.2, -88.5], [35.0, -85.0]],
        "AR": [[33.0, -94.6], [36.5, -89.6]],
        "AZ": [[31.3, -114.8], [37.0, -109.0]],
        "CA": [[32.5, -124.4], [42.0, -114.1]],
        "CO": [[37.0, -109.1], [41.0, -102.0]],
        "FL": [[24.5, -87.6], [31.0, -80.0]],
        "GA": [[30.4, -85.6], [35.0, -80.8]],
        "IA": [[40.4, -96.6], [43.5, -90.2]],
        "IL": [[37.0, -91.5], [42.5, -87.5]],
        "IN": [[37.8, -88.1], [41.8, -84.8]],
        "KS": [[37.0, -102.1], [40.0, -94.6]],
        "KY": [[36.5, -89.6], [39.1, -82.0]],
        "LA": [[28.9, -94.0], [33.0, -89.0]],
        "MD": [[37.9, -79.5], [39.7, -75.1]],
        "MI": [[41.7, -90.4], [48.2, -82.2]],
        "MO": [[36.0, -95.8], [40.6, -89.1]],
        "MS": [[30.2, -91.7], [35.0, -88.1]],
        "NC": [[33.8, -84.3], [36.6, -75.5]],
        "NE": [[40.0, -104.1], [43.0, -95.3]],
        "NJ": [[38.9, -75.6], [41.4, -73.9]],
        "NM": [[31.3, -109.1], [37.0, -103.0]],
        "NY": [[40.5, -79.8], [45.0, -71.9]],
        "OH": [[38.4, -84.8], [42.0, -80.5]],
        "OK": [[33.6, -103.0], [37.0, -94.4]],
        "PA": [[39.7, -80.5], [42.3, -74.7]],
        "SC": [[32.0, -83.4], [35.2, -78.5]],
        "TN": [[34.9, -90.3], [36.7, -81.7]],
        "TX": [[25.8, -106.7], [36.5, -93.5]],
        "VA": [[36.5, -83.7], [39.5, -75.2]],
        "WV": [[37.2, -82.6], [40.6, -77.7]]
    }
    return jsonify(bounds)

# =======================================================
# === RETURN PROVIDERS FROM KMZ or KMLs ==========================
# =======================================================
@erate_bp.route('/providers')
def providers():
    providers = ["Bluebird Network", "CDT"]

    # FNA Members (from fna_members directory)
    if os.path.isdir("fna_members"):
        for f in os.listdir("fna_members"):
            if f.lower().endswith('.kmz'):
                name = os.path.splitext(f)[0].replace('_', ' ').title()
                providers.append(name)

    # Segra East and Segra West (files in root /src)
    if os.path.exists("SEGRA_EAST.kmz"):
        providers.append("Segra East")
    if os.path.exists("SEGRA_WEST.kmz"):
        providers.append("Segra West")

    # Remove duplicates and sort alphabetically
    return jsonify(sorted(set(providers)))

# === ADD TO EXPORT FILE ON CLICK =======================
@erate_bp.route('/add-to-export', methods=['POST'])
def add_to_export():
    if 'username' not in session:
        return jsonify({"error": "Not logged in"}), 401

    app_number = request.json.get('app_number')
    distance = request.json.get('distance', '')
    if not app_number:
        return jsonify({"error": "Missing app_number"}), 400

    username = session['username']
    filename = f"exports/{username}_001.csv"
    os.makedirs("exports", exist_ok=True)

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM erate WHERE app_number = %s", (app_number,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Applicant not found"}), 404

            columns = [desc[0] for desc in cur.description]
            export_row = dict(zip(columns, row))

        # FIXED: USE SESSION TO DETERMINE CURRENT NETWORK
        current_network = session.get('current_network', 'bluebird')
        network_name = "FNA Network" if current_network == 'fna' else "Bluebird Network"

        export_row["Distance"] = distance
        export_row["Current Network"] = network_name

        # Build clean address and phone
        addr1 = export_row.get('address1') or ''
        addr2 = export_row.get('address2') or ''
        city = export_row.get('city') or ''
        state = export_row.get('state') or ''
        zip_code = export_row.get('zip_code') or ''
        full_address = f"{addr1} {addr2}, {city}, {state} {zip_code}".strip(" ,")
        phone = export_row.get('phone') or ''
        phone_ext = export_row.get('phone_ext') or ''
        full_phone = f"{phone}{' x' + phone_ext if phone_ext else ''}".strip()

        ordered_row = {
            "Applicant #": export_row.get("app_number", ""),
            "Form Nickname": export_row.get("form_nickname", ""),
            "Entity Name": export_row.get("entity_name", ""),
            "BEN#": export_row.get("ben", ""),
            "BEN Address": full_address,
            "BEN Phone": full_phone,
            "BEN Email": export_row.get("email", ""),
            "State": export_row.get("state", ""),
            "Modified": export_row.get("last_modified_datetime").strftime('%m/%d/%Y') if export_row.get("last_modified_datetime") else "",
            "Distance": distance,
            "Current Network": network_name,
        }

        for k, v in export_row.items():
            if k not in ordered_row:
                ordered_row[k] = v if v is not None else ""

        # Duplicate check
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                if any(r.get("Applicant #") == app_number for r in reader):
                    return jsonify({"status": "already_added"})

        # Write row
        file_exists = os.path.exists(filename)
        with open(filename, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=ordered_row.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(ordered_row)

        return jsonify({"status": "added"})
    except Exception as e:
        current_app.logger.error(f"Export failed: {e}")
        return jsonify({"error": "Server error"}), 500

# ======= DOWNLOAD EXPORT CSV WITH 50 POINTS DEDUCT ============ 
@erate_bp.route('/download-export')
def download_export():
    if 'username' not in session:
        return abort(403)

    username = session['username']
    filename = f"exports/{username}_001.csv"

    if not os.path.exists(filename):
        flash("No export file yet. Click some red distances first!", "error")
        return redirect(url_for('erate.dashboard'))

    # COUNT DATA ROWS ONLY
    try:
        with open(filename, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f)
            rows = list(reader)
            row_count = len(rows) - 1 if len(rows) > 0 else 0
        cost = row_count * 50
    except Exception as e:
        flash("Error reading export file", "error")
        return redirect(url_for('erate.dashboard'))

    # CHECK AND DEDUCT POINTS
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT points FROM users WHERE username = %s", (username,))
                current_points = cur.fetchone()[0]

                if current_points < cost:
                    flash(f"Not enough points! Need {cost}, you have {current_points}", "error")
                    return redirect(url_for('erate.dashboard'))

                # DEDUCT POINTS — NO SUCCESS FLASH
                cur.execute("UPDATE users SET points = points - %s WHERE username = %s", (cost, username))
            conn.commit()
    except Exception as e:
        flash("Point deduction failed", "error")
        return redirect(url_for('erate.dashboard'))

    # SEND THE FILE
    return send_file(
        filename,
        as_attachment=True,
        download_name=f"{username}_export.csv",
        mimetype='text/csv'
    )

@erate_bp.route('/guest-reset', methods=['POST'])
def guest_reset():
    if 'username' not in session:
        session.clear()  # kills the 0-point guest cookie
    return '', 204

@erate_bp.route('/get-provider')
def get_provider():
    provider = 'bluebird'  # default for guest
    if 'username' in session:
        try:
            conn = psycopg.connect(DATABASE_URL)
            with conn.cursor() as cur:
                cur.execute('SELECT "Provider" FROM users WHERE username = %s', (session['username'],))
                row = cur.fetchone()
            conn.close()
            if row and row[0]:
                provider_raw = row[0].strip()
                if provider_raw == 'Segra EAST':
                    provider = 'segra_east'
                elif provider_raw == 'Segra WEST':
                    provider = 'segra_west'
                elif provider_raw == 'FNA Network':
                    provider = 'fna'
                elif provider_raw == 'Bluebird Network':
                    provider = 'bluebird'
                else:
                    provider = 'bluebird'
        except Exception as e:
            log("Failed to get provider: %s", e)
    return jsonify({'provider': provider})

@erate_bp.route('/set-network', methods=['POST'])
def set_network():
    data = request.get_json()
    network = data.get('network', 'bluebird')
    session['current_network'] = network
    return jsonify({"status": "ok"})

@erate_bp.route('/logout')
def logout():
    session.clear()
    flash("Logged out", "success")
    return redirect(url_for('erate.dashboard'))
