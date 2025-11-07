# erate.py
from flask import Blueprint, render_template, request, current_app, redirect
import requests
import csv
import io
from extensions import db
from models import Erate
from urllib.parse import quote_plus

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

# === CONFIG ===
API_BASE_URL = "https://opendata.usac.org/api/views/jp7a-89nd"
ROWS_CSV_URL = f"{API_BASE_URL}/rows.csv"
BATCH_SIZE = 1000

# === BATCH IMPORT WITH GZIP HANDLING ===
def import_csv_to_postgres():
    url = ROWS_CSV_URL + "?accessType=DOWNLOAD"
    
    # Allow GZIP compression
    headers = {'Accept-Encoding': 'gzip, deflate'}
    response = requests.get(url, stream=True, timeout=60, headers=headers)
    response.raise_for_status()

    # Enable auto-decompression
    response.raw.decode_content = True
    stream = io.TextIOWrapper(response.raw, encoding='utf-8')
    
    reader = csv.reader(stream)
    next(reader, None)  # Skip header

    batch = []
    for row in reader:
        if len(row) < 12:
            continue
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
                amount=float(row[9]) if row[9].strip() else 0.0,
                description=row[10],
                last_modified=row[11] if len(row) > 11 and row[11].strip() else None
            )
            batch.append(erate)

            if len(batch) >= BATCH_SIZE:
                db.session.bulk_save_objects(batch)
                db.session.commit()
                batch = []
        except Exception as e:
            current_app.logger.error(f"Row import error: {e} | Row: {row[:5]}")
            continue

    if batch:
        db.session.bulk_save_objects(batch)
        db.session.commit()

    stream.close()

# === ROUTES ===
@erate_bp.route('/import')
def import_data():
    try:
        import_csv_to_postgres()
        return "E-Rate data imported successfully! (300k+ rows)"
    except Exception as e:
        current_app.logger.error(f"Import failed: {e}")
        return f"Import failed: {str(e)}"

@erate_bp.route('/')
def erate_dashboard():
    try:
        state = request.args.get('state', 'KS').strip().upper()
        min_date = request.args.get('min_date', '2025-01-01')
        offset = max(int(request.args.get('offset', 0)), 0)
        limit = 10

        # Build query
        query = Erate.query.filter(
            Erate.state == state,
            Erate.last_modified >= min_date
        ).order_by(Erate.id)

        total_filtered = query.count()
        data = query.offset(offset).limit(limit + 1).all()
        has_more = len(data) > limit
        table_data = data[:limit]

        return render_template(
            'erate.html',
            table_data=table_data,
            filters={'state': state, 'min_date': min_date},
            total=offset + len(table_data),
            total_filtered=total_filtered,
            has_more=has_more,
            next_offset=offset + limit
        )
    except Exception as e:
        current_app.logger.error(f"E-Rate dashboard error: {e}")
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
    state = request.args.get('state', 'KS').strip().upper()
    min_date = request.args.get('min_date', '2025-01-01')
    
    where = f"`Billed Entity State` = '{state}' AND `Last Modified Date/Time` >= '{min_date}T00:00:00.000'"
    encoded_where = quote_plus(where)
    url = f"{ROWS_CSV_URL}?$where={encoded_where}&$order=`ID` ASC"
    return redirect(url)
