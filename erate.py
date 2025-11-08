# erate.py
from flask import Blueprint, render_template, request, session, redirect, current_app
import csv
import os
from extensions import db
from models import Erate
from datetime import datetime

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')

CSV_FILE = "470schema.csv"  # YOUR FILE

# === HELPERS ===
def safe_date(value):
    try:
        if value and str(value).strip():
            return datetime.strptime(value.strip(), "%m/%d/%Y %I:%M:%S %p")
        return None
    except:
        return None

def safe_int(value, default=None):
    try:
        return int(value) if value and str(value).strip() else default
    except:
        return default

# === INTERACTIVE IMPORT FROM FILE ===
@erate_bp.route('/import-interactive', methods=['GET', 'POST'])
def import_interactive():
    if not os.path.exists(CSV_FILE):
        return f"CSV not found: {CSV_FILE}. Upload it to src/"

    # Session
    if 'import_progress' not in session:
        session['import_progress'] = {'index': 1, 'success': 0, 'error': 0, 'total': 0}
    progress = session['import_progress']

    # Count total
    if progress['total'] == 0:
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            progress['total'] = sum(1 for _ in f) - 1
        session['import_progress'] = progress

    # POST
    if request.method == 'POST':
        action = request.form.get('action', '').lower()

        if action == 'reset':
            session.clear()
            session['import_progress'] = {'index': 1, 'success': 0, 'error': 0, 'total': progress['total']}
            return redirect('/erate/import-interactive')

        if action == 'import' and request.form.get('confirm') == 'ok':
            try:
                with open(CSV_FILE, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for _ in range(progress['index'] - 1):
                        next(reader)
                    row = next(reader)

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
                return render_template('erate_import.html', row={}, progress=progress, error=str(e))

    # GET
    if progress['index'] > progress['total']:
        return "<h1>COMPLETE!</h1><a href='/erate'>Dashboard</a>"

    try:
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for _ in range(progress['index'] - 1):
                next(reader)
            row = next(reader)
    except Exception as e:
        return f"Error: {e}"

    return render_template('erate_import.html', row=row, progress=progress, success=False, error=None)
