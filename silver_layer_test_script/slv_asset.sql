-- slv_asset

-- DROP TABLE IF EXISTS silver.slv_asset;

-- CREATE TABLE silver.slv_asset
-- DISTKEY(loan_application_id)
-- SORTKEY(property_detail_id, loan_application_id)
-- AS

WITH dedup_valuation AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY loanapplicationid ORDER BY lastmodifiedon DESC) rn
    FROM dmihfclos.tblloantechnicaldiligencevaluation
    WHERE isactive = 1
),
dedup_ApplicationDisbursalDetail AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY loanapplicationid ORDER BY lastmodifiedon DESC) rn
    FROM dmihfclos.tblLoanApplicationDisbursalDetail
    WHERE isactive = 1
),
dedup_vetting AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY loanapplicationid ORDER BY lastmodifiedon DESC) rn
    FROM dmihfclos.tblloantechnicalreportvetting
    WHERE isactive = 1
),
technical_report AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY loanapplicationid ORDER BY lastmodifiedon DESC) rn
    FROM dmihfclos.tblLoanTechnicalReport
    WHERE isactive = 1
),
dedup_legal AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY loanapplicationid ORDER BY lastmodifiedon DESC) rn
    FROM dmihfclos.tblloanlegalreport
    WHERE isactive = 1
),

dedup_firing AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY loanapplicationid ORDER BY lastmodifiedon DESC) rn
    FROM dmihfclos.tblloantechnicalfiring
    WHERE isactive = 1
),

dedup_surrounding AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY loanapplicationid ORDER BY lastmodifiedon DESC) rn
    FROM dmihfclos.tblloantechnicalpropertysurrounding
    WHERE isactive = 1
),

base AS (
SELECT
    pd.applicantpropertydetailid AS property_detail_id,
    pd.loanapplicationid AS loan_application_id,
    ps.parentpropertyid AS parent_property_id,

    pd.unittypetypedetailid AS unit_type,
    pd.propertytypedetailid AS property_type,
    rv.classificationtypedetailid   AS property_classification,
    rv.urbanruraltypedetailid   AS urban_rural,
    pd.ownershiptypedetailid AS ownership_type,
    pd.natureofpropertytransactiontypedetailid  AS nature_of_transaction,
    rv.transitiontypedetailid   AS transaction_type,
    pd.endusetypedetailid  AS end_use,
    pd.propertydocumenttypedetailid  AS property_document_type,
    pd.alreadyownedpropertytypedetailid AS already_owned_property,

    CASE WHEN pd.isfirstproperty = '1' THEN TRUE ELSE FALSE END AS is_first_property,

    TRIM(pd.propertyaddress) AS property_address,
    mc.cityname AS property_city,
    md.districtname AS property_district,
    ms.statename  AS property_state,

    CASE
        WHEN TRIM(CAST(pd.pincode AS VARCHAR(50))) ~ '^-?[0-9]+$'
        THEN CAST(TRIM(CAST(pd.pincode AS VARCHAR(50))) AS INT)
    END AS property_pincode,

    CASE
        WHEN pd.isnegativearea = '1' THEN TRUE
        WHEN pd.isnegativearea = '0' THEN FALSE
    END AS is_negative_area,


    --- added  the alternative table for imd tables
    CASE
        WHEN TRIM(CAST(ps.latitude AS VARCHAR(100))) ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN CAST(TRIM(CAST(ps.latitude AS VARCHAR(100))) AS DECIMAL(20,8))
    END AS latitude,

    CASE
        WHEN TRIM(CAST(ps.longitude AS VARCHAR(100))) ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN CAST(TRIM(CAST(ps.longitude AS VARCHAR(100))) AS DECIMAL(20,8))
    END AS longitude,

    ps.pacolonyprojectname AS tech_project_name,
    ps.pakhasrasurveyno  AS tech_khasra_survey_no,

    CASE
        WHEN ps.isitselfoccupiedpropertybyapplicanttypedetailid IS NULL THEN NULL
        WHEN ps.isitselfoccupiedpropertybyapplicanttypedetailid = '1' THEN TRUE
        ELSE FALSE
    END AS is_self_occupied,

    CASE
        WHEN dv.iscompliantwithndmaguidelines = '1' THEN TRUE
        WHEN dv.iscompliantwithndmaguidelines = '0' THEN FALSE
    END AS is_ndma_compliant,

    CASE
        WHEN TRIM(CAST(pd.propertyareasquareft AS VARCHAR(50))) ~ '^-?[0-9]+$'
        THEN CAST(TRIM(CAST(pd.propertyareasquareft AS VARCHAR(50))) AS DECIMAL(20,0))
    END AS property_area_sqft,

    CASE
        WHEN TRIM(CAST(pd.agreementvalue AS VARCHAR(100))) ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN CAST(TRIM(CAST(pd.agreementvalue AS VARCHAR(100))) AS DECIMAL(20,2))
    END AS agreement_value,

    CASE
        WHEN TRIM(CAST(pd.mvofproperty AS VARCHAR(100))) ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN CAST(TRIM(CAST(pd.mvofproperty AS VARCHAR(100))) AS DECIMAL(20,2))
    END AS market_value,

    CASE
        WHEN TRIM(CAST(dv.valuationtotalfairmarketvalue AS VARCHAR(100))) ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN CAST(TRIM(CAST(dv.valuationtotalfairmarketvalue AS VARCHAR(100))) AS DECIMAL(20,2))
    END AS fair_market_value,
    CASE WHEN TRIM(CAST(dv.valuationlandarea AS VARCHAR(100))) ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN CAST(TRIM(CAST(dv.valuationlandarea AS VARCHAR(100))) AS DECIMAL(20, 2))
         ELSE NULL END  AS land_area_sqft,
    CASE WHEN TRIM(CAST(dv.valuationlandarearatepersquarefeet AS VARCHAR(100))) ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN CAST(TRIM(CAST(dv.valuationlandarearatepersquarefeet AS VARCHAR(100))) AS DECIMAL(20, 2))
         ELSE NULL END  AS land_rate_per_sqft,
    CASE WHEN TRIM(CAST(dv.valuationbuiltupAreaexisting AS VARCHAR(100))) ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN CAST(TRIM(CAST(dv.valuationbuiltupAreaexisting AS VARCHAR(100))) AS DECIMAL(20, 2))
         ELSE NULL END AS buildup_existing_sqft,
    CASE WHEN TRIM(CAST(dv.valuationbuiltupAreaproposed AS VARCHAR(100))) ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN CAST(TRIM(CAST(dv.valuationbuiltupAreaproposed AS VARCHAR(100))) AS DECIMAL(20, 2))
         ELSE NULL END   AS buildup_proposed_sqft,
    CASE WHEN TRIM(CAST(dv.valuationsuperbuiltuparea AS VARCHAR(100))) ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN CAST(TRIM(CAST(dv.valuationsuperbuiltuparea AS VARCHAR(100))) AS DECIMAL(20, 2))
         ELSE NULL END AS super_buildup_sqft,
    CASE WHEN TRIM(REPLACE(CAST(dv.valuationbuilding AS VARCHAR(50)), '%', '')) ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN CAST(TRIM(REPLACE(CAST(dv.valuationbuilding AS VARCHAR(50)), '%', '')) AS DECIMAL(20, 2))
         ELSE NULL END AS building_pct,

             --- no field found
    dv.valuationlandarearatepersquarefeet  AS buildup_existing_rate_per_sqft,
    dv.valuationbuiltupareaproposedratepersquarefeet  AS buildup_proposed_rate_per_sqft,
    dv.valuationsuperbuiltuparearatepersquarefeet  as super_buildup_rate_per_sqft,
    dv.valuationcarpetarea  as carpet_area,
    dv.valuationcarpetarearatepersquarefeet  AS carper_area_rate,


    CASE WHEN TRIM(REPLACE(CAST(dv.valuationunitbeingfunded AS VARCHAR(50)), '%', '')) ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN CAST(TRIM(REPLACE(CAST(dv.valuationunitbeingfunded AS VARCHAR(50)), '%', '')) AS DECIMAL(20, 2))
         ELSE NULL END  AS unit_funded_pct,
    lr.legaldetailstatustypedetailid AS legal_report_status,
    tf.agencyid   as legal_agency_name,
    lr.advocatename  as legal_advocate_name,
    tf.reportreceiveddate      as report_date,
    TRIM(
        COALESCE(NULLIF(TRIM(lr.houseno), ''), '') ||
        CASE WHEN TRIM(lr.floorno)   <> '' THEN ', ' || TRIM(lr.floorno)   ELSE '' END ||
        CASE WHEN TRIM(lr.wingno)    <> '' THEN ', ' || TRIM(lr.wingno)    ELSE '' END ||
        CASE WHEN TRIM(lr.buildingno)<> '' THEN ', ' || TRIM(lr.buildingno)ELSE '' END ||
        CASE WHEN TRIM(lr.plotno)    <> '' THEN ', ' || TRIM(lr.plotno)    ELSE '' END ||
        CASE WHEN TRIM(lr.surveyno)  <> '' THEN ', ' || TRIM(lr.surveyno)  ELSE '' END ||
        CASE WHEN TRIM(lr.streetno)  <> '' THEN ', ' || TRIM(lr.streetno)  ELSE '' END ||
        CASE WHEN TRIM(lr.stageno)   <> '' THEN ', ' || TRIM(lr.stageno)   ELSE '' END ||
        CASE WHEN TRIM(lr.landmark)  <> '' THEN ', ' || TRIM(lr.landmark)  ELSE '' END ||
        CASE WHEN TRIM(lr.village)   <> '' THEN ', ' || TRIM(lr.village)   ELSE '' END ||
        CASE WHEN TRIM(lr.mouza)     <> '' THEN ', ' || TRIM(lr.mouza)     ELSE '' END
    ) AS  legal_address,
    lr.cityid  as legal_city,
    lr.districtid  AS legal_district,
    lr.stateid  AS legal_state,
    lr.pincode  AS legal_pincode,



    CASE
        WHEN lr.vendorclearandmarketabletitletypedetailid IS NULL THEN NULL
        WHEN lr.vendorclearandmarketabletitletypedetailid = '1' THEN TRUE
        ELSE FALSE
    END AS title_clear,

    tf.reportfiringtypedetailid AS tech_firing_type,
    ma.agencyname AS tech_agency_name,

    ladd.landplotrate AS  agreement_land_rate_per_sqft,
    ladd.existingconstructionbuiltuprate  AS agreement_buildup_existing_rate_per_sqft,
    ladd.proposedconstructionbuiltuprate AS agreement_buildup_proposed_rate_per_sqft,
    ladd.constructionsuperbuiltuprate AS agreement_super_buildup_rate_per_sqft,


    CAST(tf.firingdate AS DATE)         AS tech_firing_date,
    CAST(tf.reportreceiveddate AS DATE) AS tech_report_received_date,
    CAST(tr.propertyvisitdate AS DATE)  AS tech_visit_date,  -- added  alternative column by excluding imd table field

    CAST(pd.createdon AS TIMESTAMP)      AS record_created_at,
    CAST(pd.lastmodifiedon AS TIMESTAMP) AS record_modified_at,
    CAST(GETDATE() AS TIMESTAMP)         AS silver_loaded_at,
    TO_CHAR(GETDATE(), 'YYYYMMDDHH24MISS') AS silver_batch_id

FROM dmihfclos.tblapplicationpropertydetail pd

LEFT JOIN dedup_valuation dv       ON dv.loanapplicationid = pd.loanapplicationid AND dv.rn = 1
LEFT JOIN dedup_ApplicationDisbursalDetail  ladd on ladd.loanapplicationid = pd.loanapplicationid and ladd.rn = 1
LEFT JOIN dedup_vetting rv         ON rv.loanapplicationid = pd.loanapplicationid AND rv.rn = 1
LEFT JOIN dedup_legal lr           ON lr.loanapplicationid = pd.loanapplicationid AND lr.rn = 1
LEFT JOIN dedup_firing tf          ON tf.loanapplicationid = pd.loanapplicationid AND tf.rn = 1
LEFT JOIN dedup_surrounding ps     ON ps.loanapplicationid = pd.loanapplicationid AND ps.rn = 1
LEFT JOIN technical_report tr      ON lr.loanapplicationid = pd.loanapplicationid AND lr.rn = 1

LEFT JOIN dmihfclos.mstcity mc     ON mc.cityid = pd.cityid AND mc.isactive = 1
LEFT JOIN dmihfclos.mstdistrict md ON md.districtid = pd.districtid AND md.isactive = 1
LEFT JOIN dmihfclos.mststate ms    ON ms.stateid = pd.stateid AND ms.isactive = 1
LEFT JOIN dmihfclos.mstagency ma   ON ma.agencyid = tf.agencyid AND ma.isactive = 1

)

SELECT * FROM base;