-- Silver Layer: slv_asset

DROP TABLE IF EXISTS silver.slv_asset;


CREATE TABLE silver.slv_asset
DISTKEY
    (loan_application_id)
SORTKEY
    (property_detail_id, loan_application_id) AS


    WITH dedup_valuation AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY loanapplicationid ORDER BY lastmodifiedon DESC) AS rn
        FROM dmihfclos.tblloantechnicaldiligencevaluation
        WHERE isactive = 1
    ),
    dedup_disbursal AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY loanapplicationid ORDER BY lastmodifiedon DESC) AS rn
        FROM dmihfclos.tblloanapplicationdisbursaldetail
        WHERE isactive = 1
    ),
    dedup_vetting AS (
        SELECT
            *,
            ROW_NUMBER() OVER ( PARTITION BY loanapplicationid ORDER BY lastmodifiedon DESC) AS rn
        FROM dmihfclos.tblloantechnicalreportvetting
    WHERE isactive = 1
    ),
    dedup_technical AS (
        SELECT
            *,
            ROW_NUMBER() OVER ( PARTITION BY loanapplicationid ORDER BY lastmodifiedon DESC) AS rn
        FROM dmihfclos.tblloantechnicalreport
    WHERE isactive = 1
    ),
    dedup_legal AS (
        SELECT
            *,
            ROW_NUMBER() OVER ( PARTITION BY loanapplicationid ORDER BY lastmodifiedon DESC) AS rn
        FROM dmihfclos.tblloanlegalreport
    WHERE isactive = 1
    ),
    dedup_firing AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY loanapplicationid  ORDER BY  lastmodifiedon DESC) AS rn
        FROM dmihfclos.tblloantechnicalfiring
    WHERE isactive = 1
    ),
    dedup_revised_val AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY loanapplicationid ORDER BY lastmodifiedon DESC) AS rn
        FROM dmihfclos.tblloanrevpropvaluation
    WHERE  isactive = 1
    ),
    dedup_surrounding AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY loanapplicationid ORDER BY lastmodifiedon DESC) AS rn
        FROM dmihfclos.tblloantechnicalpropertysurrounding
        WHERE isactive = 1
    )
SELECT
    --  Identifiers
    pd.applicantpropertydetailid AS property_detail_id,
    pd.loanapplicationid AS loan_application_id,
    ps.parentpropertyid AS parent_property_id,
    --  Property Type
    unittype.typeDetailDescription AS unit_type,
    pd.unittypetypedetailid AS unit_type_typedetailid,
    property.typeDetailDescription AS property_type,
    pd.propertytypedetailid AS property_type_typedetailid,
    classification.typeDetailDescription AS property_classification,
    rv.classificationtypedetailid AS property_classification_typedetailid,
    urbanrural.typeDetailDescription AS urban_rural,
    rv.urbanruraltypedetailid AS urban_rural_typedetailid,
    ownership.typeDetailDescription AS ownership_type,
    pd.ownershiptypedetailid AS ownership_type_typedetailid,
    natureofproperty.typeDetailDescription AS nature_of_transaction,
    pd.natureofpropertytransactiontypedetailid AS nature_of_transaction_typedetailid,
    transition.typeDetailDescription AS transaction_type,
    rv.transitiontypedetailid AS transaction_type_typedetailid,
    enduse.typeDetailDescription AS end_use,
    pd.endusetypedetailid AS end_use_typedetailid,
    propertydocument.typeDetailDescription AS property_document_type,
    pd.propertydocumenttypedetailid AS property_document_type_typedetailid,
    alreadyowned.typeDetailDescription AS already_owned_property,
    pd.alreadyownedpropertytypedetailid AS already_owned_property_typedetailid,
    CASE WHEN pd.isfirstproperty = '1' THEN TRUE ELSE FALSE END AS is_first_property,
    --  Location ─
    coalesce(case when ps.technicaladdress is not null and ps.technicaladdress != 0 then ps.technicaladdress else  (coalesce(pd.propertyaddress , '')  || ' ' ||  (coalesce(pd.landmark, '') )) end )AS property_address,

  coalesce(case when ps.technicaladdress is not null and ps.technicaladdress != 0 then tbl_city.cityname else  mc.cityname  end ) AS property_city,
    coalesce(case when ps.technicaladdress is not null and ps.technicaladdress != 0 then ps.pacitytehsiltalukatownid else  mc.cityid  end ) AS property_city_id,

  coalesce(case when ps.technicaladdress is not null and ps.technicaladdress != 0 then tbl_dist.districtname else  md.districtname  end ) AS property_district,
    coalesce(case when ps.technicaladdress is not null and ps.technicaladdress != 0 then ps.padistrictid else  mc.districtid  end ) AS property_distict_id,


  coalesce(case when ps.technicaladdress is not null and ps.technicaladdress != 0 then tbl_state.statename else  ms.statename   end ) AS property_state,
    coalesce(case when ps.technicaladdress is not null and ps.technicaladdress != 0 then ps.pastateid else  ms.stateid  end ) AS property_state_id,

    coalesce(
        case
            when ps.technicaladdress is not null and ps.technicaladdress != 0
            then
                CASE
                    WHEN TRIM(ps.papincode) ~ '^[0-9]+$'
                    THEN CAST(TRIM(ps.papincode) AS INT)
                END
            else
                (CASE
                    WHEN TRIM(CAST(pd.pincode AS VARCHAR(50))) ~ '^[0-9]+$'
                    THEN CAST(TRIM(CAST(pd.pincode AS VARCHAR(50))) AS INT)
                END)
        end
    ) AS property_pincode,
    pd.isnegativearea = '1' AS is_negative_area,
    CASE
        WHEN REGEXP_REPLACE(ps.latitude, '[^0-9.\-]', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN
            CAST(REGEXP_REPLACE(ps.latitude, '[^0-9.\-]', '') AS DECIMAL(20, 8))
            * (
                CASE
                    WHEN ps.latitude ILIKE '%S%' THEN -1
                    ELSE 1
                END
            )
    END AS latitude,
    CASE
        WHEN REGEXP_REPLACE(ps.longitude, '[^0-9.\-]', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN
            CAST(REGEXP_REPLACE(ps.longitude, '[^0-9.\-]', '') AS DECIMAL(20, 8))
            * (
                CASE
                    WHEN ps.longitude ILIKE '%W%' THEN -1
                    ELSE 1
                END
            )
    END AS longitude,
    --  Technical Address
    ps.pacolonyprojectname AS tech_project_name,
    ps.pakhasrasurveyno AS tech_khasra_survey_no,
    CASE
        WHEN isitself.typeDetailDescription IS NULL THEN NULL
        ELSE isitself.typeDetailDescription = '1'
    END AS is_self_occupied,
    dv.iscompliantwithndmaguidelines = '1' AS is_ndma_compliant,
    --  Valuation
    CASE
        WHEN TRIM(CAST(pd.propertyareasquareft AS VARCHAR(50))) ~ '^-?[0-9]+$' THEN CAST(
        TRIM(CAST(pd.propertyareasquareft AS VARCHAR(50))) AS DECIMAL(20, 0)
    ) END AS property_area_sqft,
    CASE
        WHEN REGEXP_REPLACE(ladd.documentvalue, '[^0-9.\-]', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST(
        REGEXP_REPLACE(ladd.documentvalue, '[^0-9.\-]', '') AS DECIMAL(20, 2)
    ) END AS agreement_value,
    CASE
        WHEN REGEXP_REPLACE(pd.mvofproperty, '[^0-9.\-]', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST(
        REGEXP_REPLACE(pd.mvofproperty, '[^0-9.\-]', '') AS DECIMAL(20, 2)
    ) END AS market_value,
    CASE
        WHEN REGEXP_REPLACE(dv.valuationtotalfairmarketvalue, '[^0-9.\-]', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST(
        REGEXP_REPLACE(dv.valuationtotalfairmarketvalue, '[^0-9.\-]', '') AS DECIMAL(20, 2)
    ) END AS fair_market_value,
    CASE
        WHEN TRIM(CAST(dv.valuationlandarea AS VARCHAR(100))) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST(
        TRIM(CAST(dv.valuationlandarea AS VARCHAR(100))) AS DECIMAL(20, 2)
    ) END AS land_area_sqft,
    CASE
        WHEN TRIM(
            CAST(
                dv.valuationlandarearatepersquarefeet AS VARCHAR(100)
            )
        ) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST(
            TRIM(
                CAST(
                    dv.valuationlandarearatepersquarefeet AS VARCHAR(100)
                )
            ) AS DECIMAL(20, 2)
    ) END AS land_rate_per_sqft,
    CASE
        WHEN TRIM(
            CAST(dv.valuationbuiltupAreaexisting AS VARCHAR(100))
        ) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST(
            TRIM(
                CAST(dv.valuationbuiltupAreaexisting AS VARCHAR(100))
            ) AS DECIMAL(20, 2)
    ) END AS buildup_existing_sqft,
    CASE
        WHEN TRIM(
            CAST(dv.valuationbuiltupAreaproposed AS VARCHAR(100))
        ) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST(
            TRIM(
                CAST(dv.valuationbuiltupAreaproposed AS VARCHAR(100))
            ) AS DECIMAL(20, 2)
    ) END AS buildup_proposed_sqft,
    CASE
        WHEN TRIM(CAST(dv.valuationsuperbuiltuparea AS VARCHAR(100))) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST(
        TRIM(CAST(dv.valuationsuperbuiltuparea AS VARCHAR(100))) AS DECIMAL(20, 2)
    ) END AS super_buildup_sqft,
    CASE
        WHEN TRIM(
            REPLACE(CAST(dv.valuationbuilding AS VARCHAR(50)), '%', '')
        ) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST(
            TRIM(
                REPLACE(CAST(dv.valuationbuilding AS VARCHAR(50)), '%', '')
            ) AS DECIMAL(20, 2)
    ) END AS building_pct,
    dv.valuationlandarearatepersquarefeet AS buildup_existing_rate_per_sqft,
    dv.valuationbuiltupareaproposedratepersquarefeet AS buildup_proposed_rate_per_sqft,
    dv.valuationsuperbuiltuparearatepersquarefeet AS super_buildup_rate_per_sqft,
    dv.valuationcarpetarea AS carpet_area,
    dv.valuationcarpetarearatepersquarefeet AS carper_area_rate,
    CASE
        WHEN TRIM(
            REPLACE(
                CAST(dv.valuationunitbeingfunded AS VARCHAR(50)),
                '%',
                ''
            )
        ) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST(
            TRIM(
                REPLACE(
                    CAST(dv.valuationunitbeingfunded AS VARCHAR(50)),
                    '%',
                    ''
                )
            ) AS DECIMAL(20, 2)
    ) END AS unit_funded_pct,
    --  ATS Rates
    ladd.landplotrate AS agreement_land_rate_per_sqft,
    ladd.existingconstructionbuiltuprate AS agreement_buildup_existing_rate_per_sqft,
    ladd.proposedconstructionbuiltuprate AS agreement_buildup_proposed_rate_per_sqft,
    ladd.constructionsuperbuiltuprate AS agreement_super_buildup_rate_per_sqft,
    --  Legal
    legaldetail.typeDetailDescription AS legal_report_status,
    lr.legaldetailstatustypedetailid AS legal_report_status_typedetailid,
    tf.agencyid AS legal_agency_name,
    lr.advocatename AS legal_advocate_name,
    tf.reportreceiveddate AS report_date,
    TRIM(
        COALESCE(NULLIF(TRIM(lr.houseno), ''), '') || CASE
        WHEN NULLIF(TRIM(lr.floorno), '') IS NOT NULL THEN ', ' || TRIM(lr.floorno)
        ELSE '' END || CASE
        WHEN NULLIF(TRIM(lr.wingno), '') IS NOT NULL THEN ', ' || TRIM(lr.wingno)
        ELSE '' END || CASE
        WHEN NULLIF(TRIM(lr.buildingno), '') IS NOT NULL THEN ', ' || TRIM(lr.buildingno)
        ELSE '' END || CASE
        WHEN NULLIF(TRIM(lr.plotno), '') IS NOT NULL THEN ', ' || TRIM(lr.plotno)
        ELSE '' END || CASE
        WHEN NULLIF(TRIM(lr.surveyno), '') IS NOT NULL THEN ', ' || TRIM(lr.surveyno)
        ELSE '' END || CASE
        WHEN NULLIF(TRIM(lr.streetno), '') IS NOT NULL THEN ', ' || TRIM(lr.streetno)
        ELSE '' END || CASE
        WHEN NULLIF(TRIM(lr.stageno), '') IS NOT NULL THEN ', ' || TRIM(lr.stageno)
        ELSE '' END || CASE
        WHEN NULLIF(TRIM(lr.landmark), '') IS NOT NULL THEN ', ' || TRIM(lr.landmark)
        ELSE '' END || CASE
        WHEN NULLIF(TRIM(lr.village), '') IS NOT NULL THEN ', ' || TRIM(lr.village)
        ELSE '' END || CASE
        WHEN NULLIF(TRIM(lr.mouza), '') IS NOT NULL THEN ', ' || TRIM(lr.mouza)
        ELSE '' END
    ) AS legal_address,
--       add description to the id columns
    lr.cityid AS legal_city,
    lr.districtid AS legal_district,
    lr.stateid AS legal_state,
    lr.pincode AS legal_pincode,
    CASE
    WHEN vendor.typeDetailDescription IS NULL THEN NULL
    ELSE vendor.typeDetailDescription = '1' END AS title_clear,
    --  Technical Firing ─
    reportfiring.typeDetailDescription AS tech_firing_type,
    tf.reportfiringtypedetailid AS tech_firing_type_typedetailid,
    ma.agencyname AS tech_agency_name,
    CAST(tf.firingdate AS DATE) AS tech_firing_date,
    CAST(tf.reportreceiveddate AS DATE) AS tech_report_received_date,
    CAST(tr.propertyvisitdate AS DATE) AS tech_visit_date,
    --  Revised Valuation
    rp.revisedmarketvalue AS revised_market_value,
    COALESCE(rp.valuationdate, rp.createdon) AS revised_valuation_date,
    --  Audit
    CAST(pd.createdon AS TIMESTAMP) AS record_created_at,
    CAST(pd.lastmodifiedon AS TIMESTAMP) AS record_modified_at,
    CAST(GETDATE() AS TIMESTAMP) AS silver_loaded_at,
    TO_CHAR(GETDATE(), 'YYYYMMDDHH24MISS') AS silver_batch_id
FROM
    dmihfclos.tblapplicationpropertydetail pd
    LEFT JOIN dedup_surrounding ps ON ps.loanapplicationid = pd.loanapplicationid
    AND ps.rn = 1
    LEFT JOIN dedup_valuation dv ON dv.loanapplicationid = pd.loanapplicationid
    AND dv.rn = 1
    LEFT JOIN dedup_disbursal ladd ON ladd.loanapplicationid = pd.loanapplicationid
    AND ladd.rn = 1
    LEFT JOIN dedup_vetting rv ON rv.loanapplicationid = pd.loanapplicationid
    AND rv.rn = 1
    LEFT JOIN dedup_legal lr ON lr.loanapplicationid = pd.loanapplicationid
    AND lr.rn = 1
    LEFT JOIN dedup_firing tf ON tf.loanapplicationid = pd.loanapplicationid
    AND tf.rn = 1
    LEFT JOIN dedup_technical tr ON tr.loanapplicationid = pd.loanapplicationid
    AND tr.rn = 1
    LEFT JOIN dedup_revised_val rp ON rp.loanapplicationid = pd.loanapplicationid
    AND rp.rn = 1
    LEFT JOIN dmihfclos.mstcity mc ON mc.cityid = pd.cityid
    AND mc.isactive = 1
    LEFT JOIN dmihfclos.mstdistrict md ON md.districtid = pd.districtid
    AND md.isactive = 1
    LEFT JOIN dmihfclos.mststate ms ON ms.stateid = pd.stateid
    AND ms.isactive = 1
    LEFT JOIN dmihfclos.mstagency ma ON ma.agencyid = tf.agencyid
    AND ma.isactive = 1

    left join  dmihfclos.mstcity tbl_city      ON  tbl_city.cityid  = ps.pacitytehsiltalukatownid  and tbl_city.isactive =1
    LEFT JOIN dmihfclos.mstdistrict tbl_dist    ON tbl_dist.districtid = ps.padistrictid  and tbl_dist.isactive =1
    left join  dmihfclos.mststate tbl_state        ON  tbl_state.stateid  = ps.pastateid  and tbl_state.isactive =1

        LEFT JOIN dmihfclos.tblTypeDetail unittype    ON  unittype.typeDetailID = pd.unittypetypedetailid
        LEFT JOIN dmihfclos.tblTypeDetail property    ON  property.typeDetailID = pd.propertytypedetailid
        LEFT JOIN dmihfclos.tblTypeDetail classification    ON  classification.typeDetailID = rv.classificationtypedetailid
        LEFT JOIN dmihfclos.tblTypeDetail urbanrural    ON  urbanrural.typeDetailID = rv.urbanruraltypedetailid
        LEFT JOIN dmihfclos.tblTypeDetail ownership    ON  ownership.typeDetailID = pd.ownershiptypedetailid
        LEFT JOIN dmihfclos.tblTypeDetail natureofproperty    ON  natureofproperty.typeDetailID = pd.natureofpropertytransactiontypedetailid
        LEFT JOIN dmihfclos.tblTypeDetail transition    ON  transition.typeDetailID = rv.transitiontypedetailid
        LEFT JOIN dmihfclos.tblTypeDetail enduse    ON  enduse.typeDetailID = pd.endusetypedetailid
        LEFT JOIN dmihfclos.tblTypeDetail propertydocument    ON  propertydocument.typeDetailID = pd.propertydocumenttypedetailid
        LEFT JOIN dmihfclos.tblTypeDetail alreadyowned    ON  alreadyowned.typeDetailID = pd.alreadyownedpropertytypedetailid
        LEFT JOIN dmihfclos.tblTypeDetail isitself    ON  isitself.typeDetailID = ps.isitselfoccupiedpropertybyapplicanttypedetailid
        LEFT JOIN dmihfclos.tblTypeDetail legaldetail    ON  legaldetail.typeDetailID = lr.legaldetailstatustypedetailid
        LEFT JOIN dmihfclos.tblTypeDetail reportfiring    ON  reportfiring.typeDetailID =  tf.reportfiringtypedetailid
        LEFT JOIN dmihfclos.tblTypeDetail vendor    ON  vendor.typeDetailID = lr.vendorclearandmarketabletitletypedetailid

    where  pd.isactive =1