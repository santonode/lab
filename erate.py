# erate.py
from flask import Blueprint, render_template, request, session, redirect, url_for, flash
import csv, os
from models import Erate
from extensions import SessionLocal
from datetime import datetime

erate_bp = Blueprint('erate', __name__, url_prefix='/erate')
CSV_FILE = os.path.join(os.path.dirname(__file__), "470schema.csv")

# ---------- helpers ----------
def safe_float(v, d=0.0): return float(v) if v and str(v).strip() else d
def safe_int(v, d=None):   return int(v) if v and str(v).strip() else d
def safe_date(v):
    if not v or not str(v).strip(): return None
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S",
                "%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M",
                "%m/%d/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try: return datetime.strptime(str(v).strip(), fmt)
        except ValueError: continue
    return None

# ---------- dashboard ----------
@erate_bp.route('/')
def dashboard():
    db = next(SessionLocal())
    try:
        q = db.query(Erate)
        state = request.args.get('state', '').strip().upper()
        if state: q = q.filter(Erate.state == state)

        total = q.count()
        rows = q.order_by(Erate.app_number).limit(11).all()
        has_more = len(rows) > 10
        rows = rows[:10]

        return render_template('erate.html',
                               table_data=rows,
                               total_filtered=total,
                               has_more=has_more,
                               filters={'state': state})
    finally:
        db.close()

# ---------- import interactive ----------
@erate_bp.route('/import-interactive', methods=['GET', 'POST'])
def import_interactive():
    if not os.path.exists(CSV_FILE):
        return "<h2>CSV not found – place 470schema.csv in src/</h2>", 404

    # ---- total rows ----
    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        total = sum(1 for _ in f) - 1 or 1

    # ---- session init ----
    prog = session.get('import_progress')
    if not prog:
        prog = {'index': 1, 'success': 0, 'error': 0, 'total': total}
        session['import_progress'] = prog
    else:
        prog = prog.copy()

    # ---- POST actions ----
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'reset':
            session.pop('import_progress', None)
            return redirect(url_for('erate.import_interactive'))

        if action == 'import_one' and request.form.get('confirm', '').lower() == 'ok':
            return _import_one(prog)
        if action == 'import_all':
            return _import_all(prog)

    # ---- GET – show current row ----
    if prog['index'] > prog['total']:
        return render_template('erate_import.html', progress=prog)

    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        reader.fieldnames = [n.strip().lstrip('\ufeff') for n in reader.fieldnames]
        for _ in range(prog['index']-1): next(reader)
        row = next(reader)

    return render_template('erate_import.html', row=row, progress=prog)

def _import_one(prog):
    db = next(SessionLocal())
    try:
        with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [n.strip().lstrip('\ufeff') for n in reader.fieldnames]
            for _ in range(prog['index']-1): next(reader)
            row = next(reader)

        app = row.get('Application Number', '').strip()
        if not app:
            prog['error'] += 1
            prog['index'] += 1
            session['import_progress'] = prog
            return render_template('erate_import.html', row=row, progress=prog,
                                   error='Missing Application Number')

        if db.get(Erate, app):
            prog['error'] += 1
            prog['index'] += 1
            session['import_progress'] = prog
            return render_template('erate_import.html', row=row, progress=prog,
                                   error='Already exists')

        er = Erate(
            # BASIC INFO
            app_number=app,
            form_nickname=row.get('Form Nickname',''),
            form_pdf=row.get('Form PDF',''),
            funding_year=row.get('Funding Year',''),
            fcc_status=row.get('FCC Form 470 Status',''),
            allowable_contract_date=safe_date(row.get('Allowable Contract Date')),
            created_datetime=safe_date(row.get('Created Date/Time')),
            created_by=row.get('Created By',''),
            certified_datetime=safe_date(row.get('Certified Date/Time')),
            certified_by=row.get('Certified By',''),
            last_modified_datetime=safe_date(row.get('Last Modified Date/Time')),
            last_modified_by=row.get('Last Modified By',''),

            # BILLED ENTITY
            ben=row.get('Billed Entity Number',''),
            entity_name=row.get('Billed Entity Name',''),
            org_status=row.get('Organization Status',''),
            org_type=row.get('Organization Type',''),
            applicant_type=row.get('Applicant Type',''),
            website=row.get('Website URL',''),
            latitude=safe_float(row.get('Latitude')),
            longitude=safe_float(row.get('Longitude')),
            fcc_reg_num=row.get('Billed Entity FCC Registration Number',''),
            address1=row.get('Billed Entity Address 1',''),
            address2=row.get('Billed Entity Address 2',''),
            city=row.get('Billed Entity City',''),
            state=row.get('Billed Entity State',''),
            zip_code=row.get('Billed Entity Zip Code',''),
            zip_ext=row.get('Billed Entity Zip Code Ext',''),
            email=row.get('Billed Entity Email',''),
            phone=row.get('Billed Entity Phone',''),
            phone_ext=row.get('Billed Entity Phone Ext',''),

            # ELIGIBLE ENTITIES
            num_eligible=safe_int(row.get('Number of Eligible Entities')),

            # CONTACT
            contact_name=row.get('Contact Name',''),
            contact_address1=row.get('Contact Address 1',''),
            contact_address2=row.get('Contact Address 2',''),
            contact_city=row.get('Contact City',''),
            contact_state=row.get('Contact State',''),
            contact_zip=row.get('Contact Zip',''),
            contact_zip_ext=row.get('Contact Zip Ext',''),
            contact_phone=row.get('Contact Phone',''),
            contact_phone_ext=row.get('Contact Phone Ext',''),
            contact_email=row.get('Contact Email',''),

            # TECHNICAL CONTACT
            tech_name=row.get('Technical Contact Name',''),
            tech_title=row.get('Technical Contact Title',''),
            tech_phone=row.get('Technical Contact Phone',''),
            tech_phone_ext=row.get('Technical Contact Phone Ext',''),
            tech_email=row.get('Technical Contact Email',''),

            # AUTHORIZED PERSON
            auth_name=row.get('Authorized Person Name',''),
            auth_address=row.get('Authorized Person Address',''),
            auth_city=row.get('Authorized Person City',''),
            auth_state=row.get('Authorized Person State',''),
            auth_zip=row.get('Authorized Person Zip',''),
            auth_zip_ext=row.get('Authorized Person Zip Ext',''),
            auth_phone=row.get('Authorized Person Phone Number',''),
            auth_phone_ext=row.get('Authorized Person Phone Number Ext',''),
            auth_email=row.get('Authorized Person Email',''),
            auth_title=row.get('Authorized Person Title',''),
            auth_employer=row.get('Authorized Person Employer',''),

            # SERVICES
            cat1_desc=row.get('Category One Description',''),
            cat2_desc=row.get('Category Two Description',''),
            installment_type=row.get('Installment Type',''),
            installment_min=safe_int(row.get('Installment Min Range Years')),
            installment_max=safe_int(row.get('Installment Max Range Years')),
            rfp_id=row.get('Request for Proposal Identifier',''),
            state_restrictions=row.get('State or Local Restrictions',''),
            restriction_desc=row.get('State or Local Restrictions Description',''),
            statewide=row.get('Statewide State',''),
            all_public=row.get('All Public Schools Districts',''),
            all_nonpublic=row.get('All Non-Public schools',''),
            all_libraries=row.get('All Libraries',''),
            form_version=row.get('Form Version','')
        )
        db.add(er)
        db.commit()
        prog['success'] += 1
    except Exception as e:
        db.rollback()
        prog['error'] += 1
        session['import_progress'] = prog
        return render_template('erate_import.html', row=row, progress=prog, error=str(e))
    finally:
        db.close()

    prog['index'] += 1
    session['import_progress'] = prog
    return render_template('erate_import.html', row=row, progress=prog, success=True)

def _import_all(prog):
    db = next(SessionLocal())
    try:
        with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [n.strip().lstrip('\ufeff') for n in reader.fieldnames]
            for _ in range(prog['index']-1): next(reader)

            imported = skipped = 0
            for row in reader:
                app = row.get('Application Number', '').strip()
                if not app: 
                    skipped += 1
                    continue
                if db.get(Erate, app):
                    skipped += 1
                    continue

                er = Erate(
                    app_number=app,
                    form_nickname=row.get('Form Nickname',''),
                    form_pdf=row.get('Form PDF',''),
                    funding_year=row.get('Funding Year',''),
                    fcc_status=row.get('FCC Form 470 Status',''),
                    allowable_contract_date=safe_date(row.get('Allowable Contract Date')),
                    created_datetime=safe_date(row.get('Created Date/Time')),
                    created_by=row.get('Created By',''),
                    certified_datetime=safe_date(row.get('Certified Date/Time')),
                    certified_by=row.get('Certified By',''),
                    last_modified_datetime=safe_date(row.get('Last Modified Date/Time')),
                    last_modified_by=row.get('Last Modified By',''),
                    ben=row.get('Billed Entity Number',''),
                    entity_name=row.get('Billed Entity Name',''),
                    org_status=row.get('Organization Status',''),
                    org_type=row.get('Organization Type',''),
                    applicant_type=row.get('Applicant Type',''),
                    website=row.get('Website URL',''),
                    latitude=safe_float(row.get('Latitude')),
                    longitude=safe_float(row.get('Longitude')),
                    fcc_reg_num=row.get('Billed Entity FCC Registration Number',''),
                    address1=row.get('Billed Entity Address 1',''),
                    address2=row.get('Billed Entity Address 2',''),
                    city=row.get('Billed Entity City',''),
                    state=row.get('Billed Entity State',''),
                    zip_code=row.get('Billed Entity Zip Code',''),
                    zip_ext=row.get('Billed Entity Zip Code Ext',''),
                    email=row.get('Billed Entity Email',''),
                    phone=row.get('Billed Entity Phone',''),
                    phone_ext=row.get('Billed Entity Phone Ext',''),
                    num_eligible=safe_int(row.get('Number of Eligible Entities')),
                    contact_name=row.get('Contact Name',''),
                    contact_address1=row.get('Contact Address 1',''),
                    contact_address2=row.get('Contact Address 2',''),
                    contact_city=row.get('Contact City',''),
                    contact_state=row.get('Contact State',''),
                    contact_zip=row.get('Contact Zip',''),
                    contact_zip_ext=row.get('Contact Zip Ext',''),
                    contact_phone=row.get('Contact Phone',''),
                    contact_phone_ext=row.get('Contact Phone Ext',''),
                    contact_email=row.get('Contact Email',''),
                    tech_name=row.get('Technical Contact Name',''),
                    tech_title=row.get('Technical Contact Title',''),
                    tech_phone=row.get('Technical Contact Phone',''),
                    tech_phone_ext=row.get('Technical Contact Phone Ext',''),
                    tech_email=row.get('Technical Contact Email',''),
                    auth_name=row.get('Authorized Person Name',''),
                    auth_address=row.get('Authorized Person Address',''),
                    auth_city=row.get('Authorized Person City',''),
                    auth_state=row.get('Authorized Person State',''),
                    auth_zip=row.get('Authorized Person Zip',''),
                    auth_zip_ext=row.get('Authorized Person Zip Ext',''),
                    auth_phone=row.get('Authorized Person Phone Number',''),
                    auth_phone_ext=row.get('Authorized Person Phone Number Ext',''),
                    auth_email=row.get('Authorized Person Email',''),
                    auth_title=row.get('Authorized Person Title',''),
                    auth_employer=row.get('Authorized Person Employer',''),
                    cat1_desc=row.get('Category One Description',''),
                    cat2_desc=row.get('Category Two Description',''),
                    installment_type=row.get('Installment Type',''),
                    installment_min=safe_int(row.get('Installment Min Range Years')),
                    installment_max=safe_int(row.get('Installment Max Range Years')),
                    rfp_id=row.get('Request for Proposal Identifier',''),
                    state_restrictions=row.get('State or Local Restrictions',''),
                    restriction_desc=row.get('State or Local Restrictions Description',''),
                    statewide=row.get('Statewide State',''),
                    all_public=row.get('All Public Schools Districts',''),
                    all_nonpublic=row.get('All Non-Public schools',''),
                    all_libraries=row.get('All Libraries',''),
                    form_version=row.get('Form Version','')
                )
                db.add(er)
                imported += 1
                if imported % 100 == 0: db.commit()
            db.commit()
        prog['success'] += imported
        prog['error'] += skipped
        prog['index'] = prog['total'] + 1
        session['import_progress'] = prog
        return redirect(url_for('erate.import_interactive'))
    except Exception as e:
        db.rollback()
        flash(f"Bulk import failed: {e}")
        return redirect(url_for('erate.import_interactive'))
    finally:
        db.close()
