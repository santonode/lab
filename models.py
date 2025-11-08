# models.py
from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class Erate(Base):
    __tablename__ = 'erate'

    # === PRIMARY KEY ===
    app_number = Column(String(20), primary_key=True)

    # === BASIC INFO ===
    form_nickname = Column(String(255))
    form_pdf = Column(Text)
    funding_year = Column(String(10))
    fcc_status = Column(String(50))
    allowable_contract_date = Column(DateTime)
    created_datetime = Column(DateTime)
    created_by = Column(String(100))
    certified_datetime = Column(DateTime)
    certified_by = Column(String(100))
    last_modified_datetime = Column(DateTime)
    last_modified_by = Column(String(100))

    # === BILLED ENTITY ===
    ben = Column(String(20))
    entity_name = Column(Text)
    org_status = Column(String(50))
    org_type = Column(String(50))
    applicant_type = Column(String(50))
    website = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    fcc_reg_num = Column(String(20))
    address1 = Column(Text)
    address2 = Column(Text)
    city = Column(String(100))
    state = Column(String(2))
    zip_code = Column(String(10))
    zip_ext = Column(String(10))
    email = Column(String(255))
    phone = Column(String(20))
    phone_ext = Column(String(10))

    # === ELIGIBLE ENTITIES ===
    num_eligible = Column(Integer)

    # === CONTACT ===
    contact_name = Column(String(100))
    contact_address1 = Column(Text)
    contact_address2 = Column(Text)
    contact_city = Column(String(100))
    contact_state = Column(String(2))
    contact_zip = Column(String(10))
    contact_zip_ext = Column(String(10))
    contact_phone = Column(String(20))
    contact_phone_ext = Column(String(10))
    contact_email = Column(String(255))

    # === TECHNICAL CONTACT ===
    tech_name = Column(String(100))
    tech_title = Column(String(100))
    tech_phone = Column(String(20))
    tech_phone_ext = Column(String(10))
    tech_email = Column(String(255))

    # === AUTHORIZED PERSON ===
    auth_name = Column(String(100))
    auth_address = Column(Text)
    auth_city = Column(String(100))
    auth_state = Column(String(2))
    auth_zip = Column(String(10))
    auth_zip_ext = Column(String(10))
    auth_phone = Column(String(20))
    auth_phone_ext = Column(String(10))
    auth_email = Column(String(255))
    auth_title = Column(String(100))
    auth_employer = Column(String(255))

    # === SERVICES ===
    cat1_desc = Column(Text)
    cat2_desc = Column(Text)
    installment_type = Column(String(50))
    installment_min = Column(Integer)
    installment_max = Column(Integer)
    rfp_id = Column(String(100))
    state_restrictions = Column(String(10))
    restriction_desc = Column(Text)
    statewide = Column(String(10))
    all_public = Column(String(10))
    all_nonpublic = Column(String(10))
    all_libraries = Column(String(10))
    form_version = Column(String(50))

    def __repr__(self):
        return f"<Erate {self.app_number}>"
