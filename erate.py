# erate.py
from flask import Blueprint, render_template, request, current_app, redirect, url_for
import requests
import csv
import io
from app import db  # ← your Flask-SQLAlchemy db
from models import Erate  # ← create this model
from urllib.parse import quote_plus
from typing import Optional

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

# === CONFIG ===
API_BASE_URL = "https://opendata.usac.org/api/views/jp7a-89nd"
ROWS_CSV_URL = f"{API_BASE_URL}/rows.csv"
BATCH_SIZE = 1000

# === POSTGRES MODEL (models.py) ===
"""
class Erate(db.Model):
    __tablename__ = 'erate'
    id = db.Column(db.String, primary_key=True)
    state = db.Column(db.String(2))
    funding_year = db.Column(db.String(4))
    entity_name = db.Column(db.Text)
    address = db.Column(db.Text)
    zip_code = db.Column(db.String(10))
    frn = db.Column(db.String(20))
    app_number = db.Column(db.String(20))
    status = db.Column(db.String(50))
    amount = db.Column(db.Float)
    description = db.Column(db.Text)
    last_modified = db.Column(db.DateTime)
"""

# === BATCH IMPORT ===
def import_csv_to_postgres():
    url = ROWS_CSV_URL + "?accessType=DOWNLOAD"
    response = requests.get(url, stream=True, timeout=30)
    response.raise_for_status()

    # Stream + decode
    lines = response.iter_lines()
    reader = csv.reader(io.TextIOWrapper(lines, encoding='utf-8'))

    header = next(reader)
    batch = []

    for row in reader:
        if len(row) < 11: continue

        # Map columns
        erate = Erate(
            id=row[0],
            state=row[1],
            funding_year=row[2],
            entity_name=row[3],
            address=row[4],
            zip_code=row[5],
            frn=row[6],
            app_number=row[7],
            status=row[8],
            amount=float(row[9]) if row[9] else 0.0,
            description=row[10],
            last_modified=row[11] if len(row) > 11 else None
        )
        batch.append(erate)

        if len(batch) >= BATCH_SIZE:
            db.session.bulk_save_objects(batch)
            db.session.commit()
            batch = []

    if batch:
        db.session.bulk_save_objects(batch)
        db.session.commit()

# === ROUTES ===
@erate_bp.route('/import')
def import_data():
    try:
        import_csv_to_postgres()
        return "E-Rate data imported to Postgres!"
    except Exception as e:
        return f"Error: {e}"

@erate_bp.route('/')
def erate_dashboard():
    try:
        state = request.args.get('state', 'KS')
        min_date = request.args.get('min_date', '2025-01-01')
        offset = int(request.args.get('offset', 0))
        limit = 10

        query = Erate.query.filter(
            Erate.state == state.upper(),
            Erate.last_modified >= min_date
        ).order_by(Erate.id)

        total = query.count()
        data = query.offset(offset).limit(limit + 1).all()
        has_more = len(data) > limit
        table_data = data[:limit]

        return render_template(
            'erate.html',
            table_data=table_data,
            filters={'state': state, 'min_date': min_date},
            total=offset + len(table_data),
            total_filtered=total,
            has_more=has_more,
            next_offset=offset + limit
        )
    except Exception as e:
        current_app.logger.error(f"E-Rate error: {e}")
        return render_template(
            'erate.html',
            error=str(e),
            table_data=[],
            total=0,
            total_filtered=0,
            filters={'state': 'KS', 'min_date': '2025-01-01'},
            has_more=False,
            next_offset=0
        )

@erate_bp.route('/download')
def download_csv():
    state = request.args.get('state', 'KS')
    min_date = request.args.get('min_date', '2025-01-01')

    where = f"`Billed Entity State` = '{state.upper()}' AND `Last Modified Date/Time` >= '{min_date}T00:00:00.000'"
    encoded = quote_plus(where)
    url = f"{ROWS_CSV_URL}?$where={encoded}&$order=`ID` ASC"
    return redirect(url)
