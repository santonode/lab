# erate.py
from flask import Blueprint, render_template, request, current_app, redirect
import requests
import csv
import io
from extensions import db
from models import Erate
from urllib.parse import quote_plus

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

API_BASE_URL = "https://opendata.usac.org/api/views/jp7a-89nd"
ROWS_CSV_URL = f"{API_BASE_URL}/rows.csv"
BATCH_SIZE = 1000

def import_csv_to_postgres():
    url = ROWS_CSV_URL + "?accessType=DOWNLOAD"
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    # CORRECT: Use iter_content + TextIOWrapper
    stream = io.TextIOWrapper(response.raw, encoding='utf-8')
    reader = csv.reader(stream)
    next(reader)  # Skip header

    batch = []
    for row in reader:
        if len(row) < 12: continue
        try:
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
                last_modified=row[11] if row[11] else None
            )
            batch.append(erate)
            if len(batch) >= BATCH_SIZE:
                db.session.bulk_save_objects(batch)
                db.session.commit()
                batch = []
        except Exception as e:
            current_app.logger.error(f"Row error: {e}")
            continue
    if batch:
        db.session.bulk_save_objects(batch)
        db.session.commit()

    stream.close()

@erate_bp.route('/import')
def import_data():
    try:
        import_csv_to_postgres()
        return "E-Rate data imported successfully!"
    except Exception as e:
        current_app.logger.error(f"Import error: {e}")
        return f"Import failed: {str(e)}"

# ... rest of dashboard and download routes unchanged
