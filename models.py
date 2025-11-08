# models.py
from extensions import db
from datetime import datetime

class Erate(db.Model):
    __tablename__ = 'erate'

    # === PRIMARY KEY ===
    app_number = db.Column(db.String(20), primary_key=True)

    # === BASIC INFO ===
    form_nickname = db.Column(db.String(255))
    form_pdf = db.Column(db.Text)
    funding_year = db.Column(db.String(10))
    fcc_status = db.Column(db.String(50))
    allowable_contract_date = db.Column(db.Date)
    created_datetime = db.Column(db.DateTime)
    created_by = db.Column(db.String(100))
    certified_datetime = db.Column(db.DateTime)
    certified_by = db.Column(db.String(100))
    last_modified_datetime = db.Column(db.DateTime)  # ← NOW DateTime
    last_modified_by = db.Column(db.String(100))

    # === BILLED ENTITY ===
    ben = db.Column(db.String(20))
    entity_name = db.Column(db.Text)
    org_status = db.Column(db.String(50))
    org_type = db.Column(db.String(50))
    applicant_type = db.Column(db.String(50))
    website = db.Column(db.Text)
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    fcc_reg_num = db.Column(db.String(20))
    address1 = db.Column(db.Text)
    address2 = db.Column(db.Text)
    city = db.Column(db.String(100))
    state = db.Column(db.String(2))
    zip_code = db.Column(db.String(10))
    zip_ext = db.Column(db.String(10))
    email = db.Column(db.String(255))
    phone = db.Column(db.String(20))
    phone_ext = db.Column(db.String(10))

    # === CONTACT ===
    num_eligible = db.Column(db.Integer)
    contact_name = db.Column(db.String(100))
    contact_address1 = db.Column(db.Text)
    contact_address2 = db.Column(db.Text)
    contact_city = db.Column(db.String(100))
    contact_state = db.Column(db.String(2))
    contact_zip = db.Column(db.String(10))
    contact_zip_ext = db.Column(db.String(10))
    contact_phone = db.Column(db.String(20))
    contact_phone_ext = db.Column(db.String(10))
    contact_email = db.Column(db.String(255))

    # === TECHNICAL CONTACT ===
    tech_name = db.Column(db.String(100))
    tech_title = db.Column(db.String(100))
    tech_phone = db.Column(db.String(20))
    tech_phone_ext = db.Column(db.String(10))
    tech_email = db.Column(db.String(255))

    # === AUTHORIZED PERSON ===
    auth_name = db.Column(db.String(100))
    auth_address = db.Column(db.Text)
    auth_city = db.Column(db.String(100))
    auth_state = db.Column(db.String(2))
    auth_zip = db.Column(db.String(10))
    auth_zip_ext = db.Column(db.String(10))
    auth_phone = db.Column(db.String(20))
    auth_phone_ext = db.Column(db.String(10))
    auth_email = db.Column(db.String(255))
    auth_title = db.Column(db.String(100))
    auth_employer = db.Column(db.String(255))

    # === SERVICES ===
    cat1_desc = db.Column(db.Text)
    cat2_desc = db.Column(db.Text)
    installment_type = db.Column(db.String(50))
    installment_min = db.Column(db.Integer)
    installment_max = db.Column(db.Integer)
    rfp_id = db.Column(db.String(100))
    state_restrictions = db.Column(db.String(10))
    restriction_desc = db.Column(db.Text)
    statewide = db.Column(db.String(10))
    all_public = db.Column(db.String(10))
    all_nonpublic = db.Column(db.String(10))
    all_libraries = db.Column(db.String(10))
    form_version = db.Column(db.String(50))

    def __repr__(self):
        return f"<Erate {self.app_number}>"
