# models471.py — SQLAlchemy model for USAC E-Rate Form 471 Basic Information dataset
# Based on the full column list from USAC Open Data portal (57 columns)
# Table name changed to 'erate2' as requested
from sqlalchemy import Column, Integer, String, DateTime, Float, Numeric, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Form471Basic(Base):
    __tablename__ = 'erate2'  # ← Table name set to erate2

    # Primary key — Application Number is unique
    application_number = Column(String(20), primary_key=True)

    # Basic Info
    form_pdf = Column(Text)
    funding_year = Column(String(10))
    billed_entity_state = Column(String(2))
    form_version = Column(String(50))
    window_status = Column(String(50))
    nickname = Column(String(255))
    status = Column(String(50))
    categories_of_service = Column(String(50))  # e.g., "Category1", "Category2"

    # Applicant's Organization
    organization_name = Column(Text)
    billed_entity_address1 = Column(Text)
    billed_entity_address2 = Column(Text)
    billed_entity_city = Column(String(100))
    billed_entity_zip_code = Column(String(10))
    billed_entity_zip_code_ext = Column(String(10))
    billed_entity_phone = Column(String(20))
    billed_entity_phone_ext = Column(String(10))
    billed_entity_email = Column(String(255))
    billed_entity_number = Column(String(20))  # BEN
    fcc_registration_number = Column(String(20))
    applicant_type = Column(String(100))

    # Contact Person
    contact_first_name = Column(String(100))
    contact_middle_initial = Column(String(10))
    contact_last_name = Column(String(100))
    contact_email = Column(String(255))
    contact_phone_number = Column(String(20))
    contact_phone_ext = Column(String(10))

    # Authorized Person
    authorized_first_name = Column(String(100))
    authorized_middle_name = Column(String(10))
    authorized_last_name = Column(String(100))
    authorized_title = Column(String(100))
    authorized_employer = Column(String(255))
    authorized_address_line_1 = Column(Text)
    authorized_address_line_2 = Column(Text)
    authorized_city = Column(String(100))
    authorized_state = Column(String(2))
    authorized_zip_code = Column(String(10))
    authorized_zip_code_ext = Column(String(10))
    authorized_phone = Column(String(20))
    authorized_phone_extension = Column(String(10))
    authorized_email = Column(String(255))

    # Certification & Enrollment
    certified_datetime = Column(DateTime)
    fulltime_enrollment = Column(Integer)
    nslp_count = Column(Integer)
    nslp_percentage = Column(Float)
    urban_rural_status = Column(String(10))  # "Urban" or "Rural"

    # Discount Rates
    category_one_discount_rate = Column(Float)
    category_two_discount_rate = Column(Float)
    voice_discount_rate = Column(Float)

    # Funding Amounts
    total_funding_year_pre_discount_eligible_amount = Column(Numeric(12, 2))
    total_funding_commitment_request_amount = Column(Numeric(12, 2))
    total_applicant_non_discount_share = Column(Numeric(12, 2))

    # Flags
    funds_from_service_provider = Column(String(10))  # "Yes"/"No"
    service_provider_filed_by_billed_entity = Column(String(10))  # "Yes"/"No"

    # Last Updated & Location
    last_updated_datetime = Column(DateTime)
    latitude = Column(Float)
    longitude = Column(Float)

    def __repr__(self):
        return f"<Form471Basic {self.application_number} - {self.organization_name}>"
