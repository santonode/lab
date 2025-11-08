# erate.py
from flask import Blueprint, render_template, request, session, redirect, url_for
import csv
import os
from db import get_conn, init_db
from datetime import datetime

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')
CSV_FILE = os.path.join(os.path.dirname(__file__), "470schema.csv")

@erate_bp.route('/')
def dashboard():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT app_number, entity_name, state, fcc_status FROM erate ORDER BY app_number LIMIT 10')
            rows = cur.fetchall()
    return render_template('erate.html', table_data=rows)

@erate_bp.route('/import-interactive')
def import_interactive():
    if not os.path.exists(CSV_FILE):
        return "CSV not found", 404

    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        total = sum(1 for _ in f) - 1

    if 'progress' not in session:
        session['progress'] = {'index': 1, 'total': total, 'success': 0, 'error': 0}

    progress = session['progress']

    if progress['index'] > progress['total']:
        return "IMPORT COMPLETE"

    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for _ in range(progress['index'] - 1):
            next(reader)
        row = next(reader)

    return render_template('erate_import.html', row=row, progress=progress)
