# erate.py
from flask import Blueprint, render_template, request, session, redirect, current_app
import csv
import os
from extensions import db
from models import Erate
from datetime import datetime

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

# === FULL PATH TO CSV IN src/ ===
CSV_FILE = os.path.join(os.path.dirname(__file__), "470schema.csv")

# === HELPERS ===
def safe_float(value, default=0.0):
    try:
        return float(value) if value and str(value).strip() else default
    except:
        return default

def safe_int(value, default=None):
    try:
        return int(value) if value and str(value).strip() else default
    except:
        return default

def safe_date(value):
    if not value or not str(value).strip():
        return None
    value = str(value).strip()

    formats = [
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    current_app.logger.warning(f"Unparseable date: {value}")
    return None

# === INTERACTIVE IMPORT ===
@erate_bp.route('/import-interactive', methods=['GET', 'POST'])
def import_interactive():
    # === DEBUG: CSV INFO ===
    current_app.logger.info(f"CSV path: {CSV_FILE}")
    current_app.logger.info(f"File exists: {os.path.exists(CSV_FILE)}")
    if os.path.exists(CSV_FILE):
        current_app.logger.info(f"File size: {os.path.getsize(CSV_FILE)} bytes")

    if not os.path.exists(CSV_FILE):
        return f"""
        <div style="text-align:center; margin:60px auto; max-width:600px; font-family:Arial, sans-serif; color:#555;">
            <h2 style="color:#dc3545;">CSV File Not Found</h2>
            <p><code>{CSV_FILE}</code></p>
            <p>Upload to <code>src/</code> and <code>git push</code>.</p>
            <p><a href="/erate" style="color:#007bff;">Dashboard</a></p>
        </div>
        """

    # === DYNAMIC TOTAL FROM CSV (EVERY TIME) ===
    try:
        with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
            total_rows = sum(1 for _ in f) - 1
            total = max(total_rows, 1)
            current_app.logger.info(f"CSV has {total} records")
    except Exception as e:
        total = 1
        current_app.logger.error(f"Failed to count CSV rows: {e}")
        return f"<h2>CSV Count Error: {e}</h2>"

    # === FORCE CLEAR SESSION & SET NEW TOTAL ===
    session.clear()
    session['import_progress'] = {
        'index': 1,
        'success': 0,
        'error': 0,
        'total': total
    }

    progress = session['import_progress']
    current_app.logger.info(f"Session reset: {progress}")

    # === POST: ACTIONS ===
    if request.method == 'POST':
        action = request.form.get('action', '').lower()

        # RESET
        if action == 'reset':
            session.clear()
            session['import_progress'] = {'index': 1, 'success': 0, 'error': 0, 'total': total}
            current_app.logger.info("Manual session reset")
            return redirect('/erate/import-interactive')

        # IMPORT ONE
        if action == 'import' and request.form.get('confirm') == 'ok':
            try:
                with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    fieldnames = [name.strip().lstrip('\ufeff') for name in reader.fieldnames]
                    reader.fieldnames = fieldnames
                    current_app.logger.info(f"CSV headers: {fieldnames}")

                    for i in range(progress['index'] - 1):
                        next(reader)

                    row = next(reader)
                    current_app.logger.info(f"Importing row {progress['index']}: {dict(list(row.items())[:5])}")

                    app_number = row.get('Application Number', '').strip()
                    if not app_number:
                        progress['error'] += 1
                        progress['index'] += 1
                        session['import_progress'] = progress
                        return render_template('erate_import.html', row=row, progress=progress,
                                             success=False, error="Skipped: No Application Number")

                    if db.session.get(Erate, app_number):
                        progress['error'] += 1
                        progress['index'] += 1
                        session['import_progress'] = progress
                        return render_template('erate_import.html', row=row, progress=progress,
                                             success=False, error="Skipped: Already in DB")

                    erate = Erate(
                        app_number=app_number,
                        form_nickname=row.get('Form Nickname', ''),
                        form_pdf=row.get('Form PDF', ''),
                        funding_year=row.get('Funding Year', ''),
                        fcc_status=row.get('FCC Form 470 Status', ''),
                        allowable_contract_date=safe_date(row.get('Allowable Contract Date')),
                        created_datetime=safe_date(row.get('Created Date/Time')),
                        created_by=row.get('Created By', ''),
                        certified_datetime=safe_date(row.get('Certified Date/Time')),
                        certified_by=row.get('Certified By', ''),
                        last_modified_datetime=safe_date(row.get('Last Modified Date/Time')),
                        last_modified_by=row.get('Last Modified By', ''),
                        ben=row.get('Billed Entity Number', ''),
                        entity_name=row.get('Billed Entity Name', ''),
                        org_status=row.get('Organization Status', ''),
                        org_type=row.get('Organization Type', ''),
                        applicant_type=row.get('Applicant Type', ''),
                        website=row.get('Website URL', ''),
                        latitude=safe_float(row.get('Latitude')),
                        longitude=safe_float(row.get('Longitude')),
                        fcc_reg_num=row.get('Billed Entity FCC Registration Number', ''),
                        address1=row.get('Billed Entity Address 1', ''),
                        address2=row.get('Billed Entity Address 2', ''),
                        city=row.get('Billed Entity City', ''),
                        state=row.get('Billed Entity State', ''),
                        zip_code=row.get('Billed Entity Zip Code', ''),
                        zip_ext=row.get('Billed Entity Zip Code Ext', ''),
                        email=row.get('Billed Entity Email', ''),
                        phone=row.get('Billed Entity Phone', ''),
                        phone_ext=row.get('Billed Entity Phone Ext', ''),
                        num_eligible=safe_int(row.get('Number of Eligible Entities')),
                        contact_name=row.get('Contact Name', ''),
                        contact_address1=row.get('Contact Address 1', ''),
                        contact_address2=row.get('Contact Address 2', ''),
                        contact_city=row.get('Contact City', ''),
                        contact_state=row.get('Contact State', ''),
                        contact_zip=row.get('Contact Zip', ''),
                        contact_zip_ext=row.get('Contact Zip Ext', ''),
                        contact_phone=row.get('Contact Phone', ''),
                        contact_phone_ext=row.get('Contact Phone Ext', ''),
                        contact_email=row.get('Contact Email', ''),
                        tech_name=row.get('Technical Contact Name', ''),
                        tech_title=row.get('Technical Contact Title', ''),
                        tech_phone=row.get('Technical Contact Phone', ''),
                        tech_phone_ext=row.get('Technical Contact Phone Ext', ''),
                        tech_email=row.get('Technical Contact Email', ''),
                        auth_name=row.get('Authorized Person Name', ''),
                        auth_address=row.get('Authorized Person Address', ''),
                        auth_city=row.get('Authorized Person City', ''),
                        auth_state=row.get('Authorized Person State', ''),
                        auth_zip=row.get('Authorized Person Zip', ''),
                        auth_zip_ext=row.get('Authorized Person Zip Ext', ''),
                        auth_phone=row.get('Authorized Person Phone Number', ''),
                        auth_phone_ext=row.get('Authorized Person Phone Number Ext', ''),
                        auth_email=row.get('Authorized Person Email', ''),
                        auth_title=row.get('Authorized Person Title', ''),
                        auth_employer=row.get('Authorized Person Employer', ''),
                        cat1_desc=row.get('Category One Description', ''),
                        cat2_desc=row.get('Category Two Description', ''),
                        installment_type=row.get('Installment Type', ''),
                        installment_min=safe_int(row.get('Installment Min Range Years')),
                        installment_max=safe_int(row.get('Installment Max Range Years')),
                        rfp_id=row.get('Request for Proposal Identifier', ''),
                        state_restrictions=row.get('State or Local Restrictions', ''),
                        restriction_desc=row.get('State or Local Restrictions Description', ''),
                        statewide=row.get('Statewide State', ''),
                        all_public=row.get('All Public Schools Districts', ''),
                        all_nonpublic=row.get('All Non-Public schools', ''),
                        all_libraries=row.get('All Libraries', ''),
                        form_version=row.get('Form Version', '')
                    )
                    db.session.add(erate)
                    db.session.commit()

                    progress['success'] += 1
                    progress['index'] += 1
                    session['import_progress'] = progress

                    return render_template('erate_import.html', row=row, progress=progress, success=True)

            except Exception as e:
                db.session.rollback()
                progress['error'] += 1
                progress['index'] += 1
                session['import_progress'] = progress
                current_app.logger.error(f"Import error: {e}")
                return render_template('erate_import.html', row={}, progress=progress, error=str(e))

        # === IMPORT ALL ===
        if action == 'import_all':
            try:
                remaining = progress['total'] - progress['index'] + 1
                if remaining <= 0:
                    return "<h1>Nothing to import!</h1><a href='/erate'>Dashboard</a>"

                imported = 0
                skipped = 0

                with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    reader.fieldnames = [name.strip().lstrip('\ufeff') for name in reader.fieldnames]

                    for _ in range(progress['index'] - 1):
                        next(reader)

                    for row in reader:
                        app_number = row.get('Application Number', '').strip()
                        if not app_number:
                            skipped += 1
                            continue
                        if db.session.get(Erate, app_number):
                            skipped += 1
                            continue

                        erate = Erate(
                            app_number=app_number,
                            form_nickname=row.get('Form Nickname', ''),
                            form_pdf=row.get('Form PDF', ''),
                            funding_year=row.get('Funding Year', ''),
                            fcc_status=row.get('FCC Form 470 Status', ''),
                            allowable_contract_date=safe_date(row.get('Allowable Contract Date')),
                            created_datetime=safe_date(row.get('Created Date/Time')),
                            created_by=row.get('Created By', ''),
                            certified_datetime=safe_date(row.get('Certified Date/Time')),
                            certified_by=row.get('Certified By', ''),
                            last_modified_datetime=safe_date(row.get('Last Modified Date/Time')),
                            last_modified_by=row.get('Last Modified By', ''),
                            ben=row.get('Billed Entity Number', ''),
                            entity_name=row.get('Billed Entity Name', ''),
                            org_status=row.get('Organization Status', ''),
                            org_type=row.get('Organization Type', ''),
                            applicant_type=row.get('Applicant Type', ''),
                            website=row.get('Website URL', ''),
                            latitude=safe_float(row.get('Latitude')),
                            longitude=safe_float(row.get('Longitude')),
                            fcc_reg_num=row.get('Billed Entity FCC Registration Number', ''),
                            address1=row.get('Billed Entity Address 1', ''),
                            address2=row.get('Billed Entity Address 2', ''),
                            city=row.get('Billed Entity City', ''),
                            state=row.get('Billed Entity State', ''),
                            zip_code=row.get('Billed Entity Zip Code', ''),
                            zip_ext=row.get('Billed Entity Zip Code Ext', ''),
                            email=row.get('Billed Entity Email', ''),
                            phone=row.get('Billed Entity Phone', ''),
                            phone_ext=row.get('Billed Entity Phone Ext', ''),
                            num_eligible=safe_int(row.get('Number of Eligible Entities')),
                            contact_name=row.get('Contact Name', ''),
                            contact_address1=row.get('Contact Address 1', ''),
                            contact_address2=row.get('Contact Address 2', ''),
                            contact_city=row.get('Contact City', ''),
                            contact_state=row.get('Contact State', ''),
                            contact_zip=row.get('Contact Zip', ''),
                            contact_zip_ext=row.get('Contact Zip Ext', ''),
                            contact_phone=row.get('Contact Phone', ''),
                            contact_phone_ext=row.get('Contact Phone Ext', ''),
                            contact_email=row.get('Contact Email', ''),
                            tech_name=row.get('Technical Contact Name', ''),
                            tech_title=row.get('Technical Contact Title', ''),
                            tech_phone=row.get('Technical Contact Phone', ''),
                            tech_phone_ext=row.get('Technical Contact Phone Ext', ''),
                            tech_email=row.get('Technical Contact Email', ''),
                            auth_name=row.get('Authorized Person Name', ''),
                            auth_address=row.get('Authorized Person Address', ''),
                            auth_city=row.get('Authorized Person City', ''),
                            auth_state=row.get('Authorized Person State', ''),
                            auth_zip=row.get('Authorized Person Zip', ''),
                            auth_zip_ext=row.get('Authorized Person Zip Ext', ''),
                            auth_phone=row.get('Authorized Person Phone Number', ''),
                            auth_phone_ext=row.get('Authorized Person Phone Number Ext', ''),
                            auth_email=row.get('Authorized Person Email', ''),
                            auth_title=row.get('Authorized Person Title', ''),
                            auth_employer=row.get('Authorized Person Employer', ''),
                            cat1_desc=row.get('Category One Description', ''),
                            cat2_desc=row.get('Category Two Description', ''),
                            installment_type=row.get('Installment Type', ''),
                            installment_min=safe_int(row.get('Installment Min Range Years')),
                            installment_max=safe_int(row.get('Installment Max Range Years')),
                            rfp_id=row.get('Request for Proposal Identifier', ''),
                            state_restrictions=row.get('State or Local Restrictions', ''),
                            restriction_desc=row.get('State or Local Restrictions Description', ''),
                            statewide=row.get('Statewide State', ''),
                            all_public=row.get('All Public Schools Districts', ''),
                            all_nonpublic=row.get('All Non-Public schools', ''),
                            all_libraries=row.get('All Libraries', ''),
                            form_version=row.get('Form Version', '')
                        )
                        db.session.add(erate)
                        imported += 1

                        if imported % 100 == 0:
                            db.session.commit()

                    db.session.commit()

                progress['success'] += imported
                progress['error'] += skipped
                progress['index'] = progress['total'] + 1
                session['import_progress'] = progress

                return f"""
                <div style="text-align:center; margin:60px auto; max-width:600px; font-family:Arial, sans-serif;">
                    <h1 style="color:#28a745;">BULK IMPORT COMPLETE!</h1>
                    <p>Imported: <strong>{imported}</strong> | Skipped: <strong>{skipped}</strong></p>
                    <p><a href="/erate" style="color:#007bff; text-decoration:none; font-weight:600;">Go to Dashboard</a></p>
                </div>
                """

            except Exception as e:
                db.session.rollback()
                return f"<h2 style='color:#dc3545;'>Bulk import failed: {e}</h2><a href='/erate/import-interactive'>Retry</a>"

    # GET
    if progress['index'] > progress['total']:
        return "<h1>IMPORT COMPLETE!</h1><a href='/erate'>Dashboard</a>"

    try:
        with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            fieldnames = [name.strip().lstrip('\ufeff') for name in reader.fieldnames]
            reader.fieldnames = fieldnames
            current_app.logger.info(f"CSV headers: {fieldnames}")

            for i in range(progress['index'] - 1):
                next(reader)

            row = next(reader)
            current_app.logger.info(f"Row {progress['index']}: {dict(list(row.items())[:5])}")
    except StopIteration:
        progress['index'] = progress['total'] + 1
        session['import_progress'] = progress
        return "<h1>IMPORT COMPLETE!</h1><a href='/erate'>Dashboard</a>"
    except Exception as e:
        current_app.logger.error(f"CSV row error at index {progress['index']}: {e}")
        return f"<h2>Row Error at {progress['index']}: {e}</h2><p><a href='/erate'>Dashboard</a></p>"

    return render_template('erate_import.html', row=row, progress=progress, success=False, error=None)

# === DASHBOARD (FROM POSTGRES) ===
@erate_bp.route('/')
def dashboard():
    state = request.args.get('state', '').strip().upper()
    min_date = request.args.get('min_date', '')
    offset = max(int(request.args.get('offset', 0)), 0)
    limit = 10

    query = Erate.query
    if state:
        query = query.filter(Erate.state == state)
    if min_date:
        query = query.filter(Erate.last_modified_datetime >= min_date)

    total_filtered = query.count()
    data = query.order_by(Erate.app_number).offset(offset).limit(limit + 1).all()
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
