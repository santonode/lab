-- recreate_erate.sql
-- DROP + RECREATE erate TABLE WITH TEXT FIELDS + FULL PDF SUPPORT
-- Run this to fix: truncated descriptions + correct PDF links

DROP TABLE IF EXISTS erate CASCADE;

CREATE TABLE erate (
    id SERIAL PRIMARY KEY,
    app_number VARCHAR(20) NOT NULL UNIQUE,

    -- BASIC INFO
    form_nickname VARCHAR(255),
    form_pdf TEXT,
    funding_year VARCHAR(10),
    fcc_status VARCHAR(100),
    allowable_contract_date TIMESTAMP,
    created_datetime TIMESTAMP,
    created_by VARCHAR(100),
    certified_datetime TIMESTAMP,
    certified_by VARCHAR(100),
    last_modified_datetime TIMESTAMP,
    last_modified_by VARCHAR(100),

    -- BILLED ENTITY
    ben VARCHAR(20),
    entity_name TEXT,
    org_status VARCHAR(100),
    org_type VARCHAR(100),
    applicant_type VARCHAR(100),
    website TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    fcc_reg_num VARCHAR(20),
    address1 TEXT,
    address2 TEXT,
    city VARCHAR(100),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    zip_ext VARCHAR(10),
    email VARCHAR(255),
    phone VARCHAR(20),
    phone_ext VARCHAR(10),
    num_eligible INTEGER,

    -- CONTACT
    contact_name VARCHAR(100),
    contact_address1 TEXT,
    contact_address2 TEXT,
    contact_city VARCHAR(100),
    contact_state VARCHAR(2),
    contact_zip VARCHAR(10),
    contact_zip_ext VARCHAR(10),
    contact_phone VARCHAR(20),
    contact_phone_ext VARCHAR(10),
    contact_email VARCHAR(255),

    -- TECHNICAL CONTACT
    tech_name VARCHAR(100),
    tech_title VARCHAR(100),
    tech_phone VARCHAR(20),
    tech_phone_ext VARCHAR(10),
    tech_email VARCHAR(255),

    -- AUTHORIZED PERSON
    auth_name VARCHAR(100),
    auth_address TEXT,
    auth_city VARCHAR(100),
    auth_state VARCHAR(2),
    auth_zip VARCHAR(10),
    auth_zip_ext VARCHAR(10),
    auth_phone VARCHAR(20),
    auth_phone_ext VARCHAR(10),
    auth_email VARCHAR(255),
    auth_title VARCHAR(100),
    auth_employer VARCHAR(255),

    -- SERVICES (TEXT = NO TRUNCATION)
    cat1_desc TEXT,
    cat2_desc TEXT,
    installment_type VARCHAR(100),
    installment_min INTEGER,
    installment_max INTEGER,
    rfp_id VARCHAR(100),
    state_restrictions VARCHAR(100),
    restriction_desc TEXT,
    statewide VARCHAR(10),
    all_public VARCHAR(10),
    all_nonpublic VARCHAR(10),
    all_libraries VARCHAR(10),
    form_version VARCHAR(50)
);

-- INDEXES
CREATE INDEX idx_erate_app_number ON erate(app_number);
CREATE INDEX idx_erate_state ON erate(state);
CREATE INDEX idx_erate_funding_year ON erate(funding_year);
CREATE INDEX idx_erate_last_modified ON erate(last_modified_datetime);
CREATE INDEX idx_erate_entity_name ON erate(entity_name);
CREATE INDEX idx_erate_fcc_status ON erate(fcc_status);

-- OPTIMIZE
VACUUM ANALYZE erate;
