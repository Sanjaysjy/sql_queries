-- slv_customer
-- DROP TABLE IF EXISTS silver.slv_customer;

--
-- CREATE TABLE silver.slv_customer
-- DISTSTYLE KEY
-- DISTKEY(applicant_id)
-- SORTKEY(loan_application_id)
-- AS

WITH base_appl AS (
    SELECT
        applicantID,
        entityID,
        loanApplicationID,
        parentApplicantID,
        firstName,
        middleName,
        lastName,
        fatherFirstName,
        fatherMiddleName,
        fatherLastName,
        motherFirstName,
        motherMiddleName,
        motherLastName,
        spouseFirstName,
        spouseMiddleName,
        spouseLastName,
        dateOfBirth,
        age,
        genderTypeDetailID,
        maritialStatusTypeDetailID,
        nationalityTypeDetailID,
        religionTypeDetailID,
        casteTypeDetailID,
        isMinority,
        isMainApplicant,
        relationshipTypeDetailId,
        isDisabled,
        isExServicemen,
        isManualScavenger,
        isHomeMakerTypeDetailID,
        qualificationTypeDetailID,
        detailQualificationTypeDetailID,
        livingStandardTypeDetailId,
        isNegativeProfile,
        isCustomerHavePoliticalLink,
        isRegularRTRPostDefault,
        createdOn,
        lastModifiedOn,
        noOfPropertyOwned,
        incomeConsidered ,                 ---added       /* additional fields */
        nameAsPerAadhar,                   ---added     /* additional fields */
        riskTypeDetailID,                 ---added    /* additional fields */
        (
            SELECT ar.riskTypeDetailID
            FROM dmihfclos.tblApplicantRisk ar
            WHERE ar.applicantID = a.applicantID
            AND ar.isActive = 1
            ORDER BY ar.reEvaluatedOn DESC
            LIMIT 1
        ) AS current_riskTypeDetailID ,                 ---added     /* additional fields : left join on table tblApplicantRisk  based on applicantID where isActive = 1 having max reEvaluatedOn */
        ROW_NUMBER() OVER (PARTITION BY applicantID ORDER BY lastModifiedOn DESC) AS rn
    FROM dmihfclos.tblApplicant a
),
current_risk  AS (
    SELECT
        ba.applicantID,
        ba.loanApplicationID,
        COALESCE(ba.current_riskTypeDetailID, ba.riskTypeDetailID) AS current_riskTypeDetailID,
        ROW_NUMBER() OVER (PARTITION BY ba.applicantID ORDER BY ba.lastModifiedOn DESC) AS rn
    FROM base_appl ba
        )

,addr_cur AS (
    SELECT
        address1,    ---added /*  added address */
        cityID,      --- added /*  added city */
        districtID,   ---added /*  added destrict */
        stayingInYears, --- present /* added  field */
        entityID,
        cityName,
        zip,
        landmark,
        blocktaluka ,
        addressTypeDetailID,
        residenceTypeTypeDetailID,
        ROW_NUMBER() OVER (PARTITION BY entityID ORDER BY lastModifiedOn DESC) AS rn
    FROM dmihfclos.tblAddress
    WHERE isActive = 1
),

occ_det AS (
    SELECT
        applicantID,
        loanApplicationID,
        profileSegmentTypeDetailID,
        subProfileTypeDetailID,
        companyName,
        retirementage ,
        industryTypeDetailID,
        subIndustryTypeDetailID,
        constitutionTypeDetailID,
        nhbQOccupationTypeDetailID,
        occupationSubTypeDetailID,
        employmentTypeDetailID,
        sectorTypeDetailID,
        designation,
        gstRegistration,
        dateOfIncorporation,
        dateOfJoining ,                   ---added    /* added  field */
        totalExperience ,                 ---added     /* added  field */
        businessVinatgeInYear,            ---added     /* added  field */
        businessSetupTypeDetailID,         ---added      /* added  field */
        isPensioner,                      ---added    /* added  field */
        isSalarySlipAvailable,            ---added    /* added  field */
        isMultipleEarner,                 ---added     /* added  field */
        ROW_NUMBER() OVER (PARTITION BY applicantID, loanApplicationID ORDER BY lastModifiedOn DESC) AS rn
    FROM dmihfclos.tblApplicantOccupationalDetail
    WHERE isActive = 1
),

inc_det AS (
    SELECT
        applicantID,
        loanApplicationID,
        applicantGrossIncome,
        assessedMonthlyIncome,
        ROW_NUMBER() OVER (PARTITION BY applicantID, loanApplicationID ORDER BY lastModifiedOn DESC) AS rn
    FROM dmihfclos.tblApplicantIncome
    WHERE isActive = 1  /* to discuss */
),

kyc_pan AS (
    SELECT
        applicantID,
        loanApplicationID,
        identificationNumber,
        stats.typeDetailDescription  as pan_kyc_verified_status,
        ROW_NUMBER() OVER (PARTITION BY ba.applicantID, ba.loanApplicationID ORDER BY ba.lastModifiedOn DESC) AS rn
    FROM dmihfclos.tblApplicantKyc ba
    LEFT JOIN dmihfclos.tblTypeDetail stats
    ON   stats.typedetailid = ba.verifiedstatustypedetailid
    and ba.isActive = 1
      where ba.identificationTypeDetailID =  56
),

kyc_aadh AS (
    SELECT
        applicantID,
        loanApplicationID,
        refUID,
        uidLastDigit ,   /* added  field */
        stats.typeDetailDescription  as aadh_kyc_verified_status,
        ROW_NUMBER() OVER (PARTITION BY ba.applicantID, ba.loanApplicationID ORDER BY ba.lastModifiedOn DESC) AS rn
    FROM dmihfclos.tblApplicantKyc ba
    LEFT JOIN dmihfclos.tblTypeDetail stats
    ON   stats.typedetailid = ba.verifiedstatustypedetailid
    and ba.isActive = 1
      where ba.identificationTypeDetailID =  230
),

kyc_voter AS (
    SELECT
        applicantID,
        loanApplicationID,
        identificationNumber,
        stats.typeDetailDescription  as voter_kyc_verified_status,
        ROW_NUMBER() OVER (PARTITION BY ba.applicantID, ba.loanApplicationID ORDER BY ba.lastModifiedOn DESC) AS rn
    FROM dmihfclos.tblApplicantKyc ba
    LEFT JOIN dmihfclos.tblTypeDetail stats
    ON   stats.typedetailid = ba.verifiedstatustypedetailid
    and ba.isActive = 1
      where ba.identificationTypeDetailID =  233
),

kyc_pass AS (
    SELECT
        applicantID,
        loanApplicationID,
        identificationNumber,
        stats.typeDetailDescription  as pass_kyc_verified_status,
        ROW_NUMBER() OVER (PARTITION BY ba.applicantID, ba.loanApplicationID ORDER BY ba.lastModifiedOn DESC) AS rn
    FROM dmihfclos.tblApplicantKyc ba
    LEFT JOIN dmihfclos.tblTypeDetail stats
    ON   stats.typedetailid = ba.verifiedstatustypedetailid
    and ba.isActive = 1
      where ba.identificationTypeDetailID =  231
),

kyc_dl AS (
    SELECT
        applicantID,
        loanApplicationID,
        identificationNumber,
        stats.typeDetailDescription  as dl_kyc_verified_status,
        ROW_NUMBER() OVER (PARTITION BY ba.applicantID, ba.loanApplicationID ORDER BY ba.lastModifiedOn DESC) AS rn
    FROM dmihfclos.tblApplicantKyc ba
    LEFT JOIN dmihfclos.tblTypeDetail stats
    ON   stats.typedetailid = ba.verifiedstatustypedetailid
    and ba.isActive = 1
      where ba.identificationTypeDetailID =  232
),

kyc_stat AS (
    SELECT
        applicantID,
        loanApplicationID,
        verifiedStatusTypeDetailID,
        ROW_NUMBER() OVER (PARTITION BY applicantID, loanApplicationID ORDER BY lastModifiedOn DESC) AS rn
    FROM dmihfclos.tblApplicantKyc
    WHERE isActive = 1
      AND isUseForPerformKyc = 1   /* to discuss */
),

bur_cibil AS (
    SELECT
        entityID,
        loanApplicationID,
        score,
        ROW_NUMBER() OVER (PARTITION BY entityID, loanApplicationID ORDER BY reportDate DESC) AS rn
    FROM dmihfclos.tblEntityBureau
    WHERE isActive = 1
      AND bureauTypeDetailId = 410
),

ascore_rec AS (
    SELECT
        applicantID,
        loanApplicationID,
        underwritingScore,
        creditBucket ,  /* added  field */
        ROW_NUMBER() OVER (PARTITION BY applicantID, loanApplicationID ORDER BY createdOn DESC) AS rn
    FROM dmihfclos.tblAscoreModel
    WHERE isActive = 1
),

oblig_agg AS (
    SELECT
        applicantID,
        loanApplicationID,
        SUM(emiAmount) AS total_existing_emi,
        COUNT(existingObligationID) AS existing_obligation_count,
        SUM(loanAmount) AS total_existing_loan_amt
        -- repaymentbanktypedetailid
    FROM dmihfclos.tblApplicantExistingObligation
    WHERE isActive = 1
    GROUP BY applicantID, loanApplicationID  /* to discuss  */
)

SELECT
    ba.applicantID AS applicant_id,
    ba.entityID AS entity_id,
    ba.loanApplicationID AS loan_application_id,
    ba.parentApplicantID AS parent_applicant_id,
    NULLIF(TRIM(
        TRIM(COALESCE(NULLIF(TRIM(CAST(ba.firstName AS VARCHAR(150))), ''), ''))
        || CASE WHEN NULLIF(TRIM(CAST(ba.middleName AS VARCHAR(150))), '') IS NOT NULL THEN ' ' || TRIM(CAST(ba.middleName AS VARCHAR(150))) ELSE '' END
        || CASE WHEN NULLIF(TRIM(CAST(ba.lastName   AS VARCHAR(150))), '') IS NOT NULL THEN ' ' || TRIM(CAST(ba.lastName   AS VARCHAR(150))) ELSE '' END
    ), '') AS full_name,
    CAST(ba.dateOfBirth AS DATE) AS date_of_birth,
    ba.age AS age,
    td_gender.typeDetailDescription AS gender,
    td_marital.typeDetailDescription AS marital_status,
    td_nation.typeDetailDescription AS nationality,
    td_religion.typeDetailDescription AS religion,
    td_caste.typeDetailDescription AS caste_category,
    CASE WHEN ba.isMinority = 1 THEN TRUE WHEN ba.isMinority = 0 THEN FALSE ELSE NULL END AS is_minority,
    CASE WHEN ba.isMainApplicant = 1 THEN TRUE   ELSE FALSE END AS is_main_applicant,               ---added    /*  CASE WHEN ba.isMainApplicant = 1 THEN TRUE ELSE FALSE END AS is_main_applicant  */
    CASE WHEN ba.isMainApplicant = 1 THEN 'Self' ELSE td_relation.typeDetailDescription END AS relationship_to_main,            --- addded  /*  CASE WHEN ba.isMainApplicant = 1 THEN 'Self' ELSE td_relation.typeDetailDescription END AS relationship_to_main */
    NULLIF(TRIM(
        TRIM(COALESCE(NULLIF(TRIM(CAST(ba.fatherFirstName  AS VARCHAR(200))), ''), ''))
        || CASE WHEN NULLIF(TRIM(CAST(ba.fatherMiddleName AS VARCHAR(200))), '') IS NOT NULL THEN ' ' || TRIM(CAST(ba.fatherMiddleName AS VARCHAR(200))) ELSE '' END
        || CASE WHEN NULLIF(TRIM(CAST(ba.fatherLastName   AS VARCHAR(200))), '') IS NOT NULL THEN ' ' || TRIM(CAST(ba.fatherLastName   AS VARCHAR(200))) ELSE '' END
    ), '') AS father_name,
    NULLIF(TRIM(
        TRIM(COALESCE(NULLIF(TRIM(CAST(ba.motherFirstName  AS VARCHAR(200))), ''), ''))
        || CASE WHEN NULLIF(TRIM(CAST(ba.motherMiddleName AS VARCHAR(200))), '') IS NOT NULL THEN ' ' || TRIM(CAST(ba.motherMiddleName AS VARCHAR(200))) ELSE '' END
        || CASE WHEN NULLIF(TRIM(CAST(ba.motherLastName   AS VARCHAR(200))), '') IS NOT NULL THEN ' ' || TRIM(CAST(ba.motherLastName   AS VARCHAR(200))) ELSE '' END
    ), '') AS mother_name,              --- added     /*  Append Mother Name */
    NULLIF(TRIM(
        TRIM(COALESCE(NULLIF(TRIM(CAST(ba.spouseFirstName  AS VARCHAR(200))), ''), ''))
        || CASE WHEN NULLIF(TRIM(CAST(ba.spouseMiddleName AS VARCHAR(200))), '') IS NOT NULL THEN ' ' || TRIM(CAST(ba.spouseMiddleName AS VARCHAR(200))) ELSE '' END
        || CASE WHEN NULLIF(TRIM(CAST(ba.spouseLastName   AS VARCHAR(200))), '') IS NOT NULL THEN ' ' || TRIM(CAST(ba.spouseLastName   AS VARCHAR(200))) ELSE '' END
    ), '') AS spouse_name,
    CASE WHEN ba.isDisabled = 1 THEN TRUE WHEN ba.isDisabled = 0 THEN FALSE ELSE NULL END AS is_disabled,
    CASE WHEN ba.isExServicemen = 1 THEN TRUE WHEN ba.isExServicemen = 0 THEN FALSE ELSE NULL END AS is_exservicemen,
    CASE WHEN ba.isManualScavenger = 1 THEN TRUE WHEN ba.isManualScavenger = 0 THEN FALSE ELSE NULL END AS is_manualscavenger,
    CASE WHEN ba.isHomeMakerTypeDetailID IS NOT NULL THEN TRUE ELSE FALSE END AS is_homemaker,
    td_risk.typeDetailDescription  AS current_risk,   --- added
    ba.incomeConsidered AS income_considered,
    ba.nameAsPerAadhar AS name_as_per_aadhar,
    ba.riskTypeDetailID AS risk_TypeDetailID,
    td_qual.typeDetailDescription AS qualification,
    td_dqual.typeDetailDescription AS detailed_qualification,
    CASE WHEN ba.livingStandardTypeDetailId IS NOT NULL THEN TRUE ELSE FALSE END AS lsi,
    CASE WHEN ba.isNegativeProfile = 1 THEN TRUE WHEN ba.isNegativeProfile = 0 THEN FALSE ELSE NULL END AS negative_caution_profile,
    CASE WHEN ba.isCustomerHavePoliticalLink = 1 THEN TRUE WHEN ba.isCustomerHavePoliticalLink = 0 THEN FALSE ELSE NULL END AS is_politacally_linked,
    CASE WHEN ba.isRegularRTRPostDefault = 1 THEN 'Yes' WHEN ba.isRegularRTRPostDefault = 0 THEN 'No' ELSE NULL END AS regular_rtr,
    ba.noOfPropertyOwned  AS no_of_propertyowned,
    csm.custSegment  AS customer_segment,
    kyc_pan.identificationNumber AS pan_number,
    kyc_pan.pan_kyc_verified_status,
    kyc_aadh.refUID AS aadhaar_token,
    kyc_aadh.aadh_kyc_verified_status,
    kyc_aadh.uidLastDigit  as uid_last_digit,
    kyc_voter.identificationNumber AS voter_id,
    kyc_voter.voter_kyc_verified_status,
    kyc_pass.identificationNumber AS passport_number,
    kyc_pass.pass_kyc_verified_status,
    kyc_dl.identificationNumber AS driving_license_number,
    kyc_dl.dl_kyc_verified_status,
    td_kyc_stat.typeDetailDescription AS kyc_status,
    ac.cityName AS current_city,
    aab.currentStateID AS current_state,
    ac.zip AS current_pincode,
    td_res_type.typeDetailDescription AS residence_type,
    td_addr_type.typeDetailDescription AS address_type,
    ac.address1  as primary_address,    ---added
    ac.cityID  as city_id,      --- added
    ac.districtID as district_id,    -- added
    ac.landmark AS address_landmark,  /*Address fields*/
    ac.blockTaluka AS block_taluka,
    ac.stayingInYears AS staying_in_years,
    td_prof_seg.typeDetailDescription AS profile_segment,
    td_sub_prof.typeDetailDescription AS sub_profile,
    td_nhb_occ.typeDetailDescription AS occupation,
    td_occ_sub.typeDetailDescription AS distinct_occupation,
    td_const.typeDetailDescription AS constitution,
    UPPER(TRIM(occ.companyName)) AS company_name,
    td_industry.typeDetailDescription AS industry,
    td_sub_ind.typeDetailDescription AS sub_industry,
    inc.applicantGrossIncome AS gross_income,
    inc.assessedMonthlyIncome AS assessed_income,
    CAST(occ.dateOfIncorporation AS DATE) AS dincorporation_joining_date,
    occ.retirementAge AS  retirement_age,
    aab.officeAddress AS  business_office_address,
    td_emp_type.typeDetailDescription AS employment_type,
    td_sector.typeDetailDescription AS sector_constitution,
    ud.udhyamregnumber AS udyam_reg_no,         ---added     /* Table  to be referred tblEmploymentUdhyamDetails */
    ud.udhyamissuedate AS udyam_reg_date,       ---added      /* Table  to be referred tblEmploymentUdhyamDetails */
    occ.designation AS designation,
    occ.gstRegistration AS gst_vat,
    occ.dateOfJoining as date_of_joining,
    occ.totalExperience as total_experience,
    occ.businessVinatgeInYear as business_vinatge_in_year,
    occ.businessSetupTypeDetailID as business_setup_typeDetailID,
    occ.isPensioner as is_pensioner,
    occ.isSalarySlipAvailable as is_salary_slip_available,
    occ.isMultipleEarner as is_multiple_earner,
    CAST(bur.score AS INT) AS cibil_score,
    asc_r.underwritingScore AS a_score,
    asc_r.creditBucket  as credit_bucket,
    aab.experianScore AS experian_score,
    -- highmark_result_score
    -- oblig.total_existing_emi AS total_existing_emi,      /*Multiple */
    -- oblig.existing_obligation_count AS existing_obligation_count,  /*Multiple */
    -- oblig.total_existing_loan_amt AS total_existing_loan_amt,  /*Multiple */
    lbt.btLoanAmount AS  bt_loan_amount,
    -- bt_loan_financer    /*  to discuss  Multiple */
    -- cash_deposit_sync_business -- source table not present

    -- abd.bankNameTypeDetailID AS bank_name,        /*Multiple */
    -- abd.accountTypeDetailID AS account_type,        /*Multiple */
    -- abd.salaryCreditTypeDetailID AS salary_credited,     /*Multiple */
    -- oblig.repaymentBankTypeDetailID AS dmi_repayment_bank,
    -- abd.isAbnormalTransaction AS abnormal_transaction_nontracable,    /*Multiple */

--     NOT MENTIONED ABOUT  6 months  sum or count of salary credits
    --salaryCreditCountLastSixMonth AS salary_credited_last_6months  FROM THIS TABLE tblCartCamAnalysisData          --pk or relation with the base table ?
--     td_risk.typeDetailDescription AS risk_category,
    CAST(ba.createdOn AS TIMESTAMP) AS record_created_at,
    CAST(ba.lastModifiedOn AS TIMESTAMP) AS record_modified_at,
    CURRENT_TIMESTAMP AS silver_loaded_at,
    TO_CHAR(GETDATE(),'YYYYMMDD_HH24MISS') AS silver_batch_id
FROM base_appl ba
LEFT JOIN addr_cur ac
    ON  ac.entityID = ba.entityID
    AND ac.rn = 1
LEFT JOIN occ_det occ
    ON  occ.applicantID       = ba.applicantID
    AND occ.loanApplicationID = ba.loanApplicationID
    AND occ.rn = 1
LEFT JOIN inc_det inc
    ON  inc.applicantID       = ba.applicantID
    AND inc.loanApplicationID = ba.loanApplicationID
    AND inc.rn = 1
left join dmihfclos.tblCustomerSegmentationModel csm
    ON  csm.applicantID       = ba.applicantID
    AND csm.loanApplicationID = ba.loanApplicationID
    AND csm.isactive =1
left join dmihfclos.tblLoanApplicationAdditionalBureau aab
    ON  aab.loanApplicationID = ba.loanApplicationID
    AND aab.isactive =1
left join dmihfclos.tblLoanBTSummary lbt
    ON  lbt.loanApplicationID = ba.loanApplicationID
    AND lbt.isactive =1
left join dmihfclos.tblApplicantBankDetails abd
    ON  abd.loanApplicationID = ba.loanApplicationID
    AND abd.isactive =1
LEFT JOIN dmihfclos.tblEmploymentUdhyamDetails ud
    ON  ud.applicantID = ba.applicantID
    AND ud.isactive =1

LEFT JOIN current_risk cr
    ON  cr.applicantID       = ba.applicantID
    AND cr.loanApplicationID = ba.loanApplicationID
    AND cr.rn = 1

LEFT JOIN kyc_pan
    ON  kyc_pan.applicantID       = ba.applicantID
    AND kyc_pan.loanApplicationID = ba.loanApplicationID
    AND kyc_pan.rn = 1
LEFT JOIN kyc_aadh
    ON  kyc_aadh.applicantID       = ba.applicantID
    AND kyc_aadh.loanApplicationID = ba.loanApplicationID
    AND kyc_aadh.rn = 1
LEFT JOIN kyc_voter
    ON  kyc_voter.applicantID       = ba.applicantID
    AND kyc_voter.loanApplicationID = ba.loanApplicationID
    AND kyc_voter.rn = 1
LEFT JOIN kyc_pass
    ON  kyc_pass.applicantID       = ba.applicantID
    AND kyc_pass.loanApplicationID = ba.loanApplicationID
    AND kyc_pass.rn = 1
LEFT JOIN kyc_dl
    ON  kyc_dl.applicantID       = ba.applicantID
    AND kyc_dl.loanApplicationID = ba.loanApplicationID
    AND kyc_dl.rn = 1
LEFT JOIN kyc_stat
    ON  kyc_stat.applicantID       = ba.applicantID
    AND kyc_stat.loanApplicationID = ba.loanApplicationID
    AND kyc_stat.rn = 1
LEFT JOIN bur_cibil bur
    ON  bur.entityID          = ba.entityID
    AND bur.loanApplicationID = ba.loanApplicationID
    AND bur.rn = 1
LEFT JOIN ascore_rec asc_r
    ON  asc_r.applicantID       = ba.applicantID
    AND asc_r.loanApplicationID = ba.loanApplicationID
    AND asc_r.rn = 1
LEFT JOIN oblig_agg oblig
    ON  oblig.applicantID       = ba.applicantID
    AND oblig.loanApplicationID = ba.loanApplicationID
LEFT JOIN dmihfclos.tblTypeDetail td_gender
    ON  td_gender.typeDetailID = ba.genderTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_marital
    ON  td_marital.typeDetailID = ba.maritialStatusTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_nation
    ON  td_nation.typeDetailID = ba.nationalityTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_religion
    ON  td_religion.typeDetailID = ba.religionTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_caste
    ON  td_caste.typeDetailID = ba.casteTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_relation
    ON  td_relation.typeDetailID = ba.relationshipTypeDetailId
LEFT JOIN dmihfclos.tblTypeDetail td_qual
    ON  td_qual.typeDetailID = ba.qualificationTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_dqual
    ON  td_dqual.typeDetailID = ba.detailQualificationTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_addr_type
    ON  td_addr_type.typeDetailID = ac.addressTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_res_type
    ON  td_res_type.typeDetailID = ac.residenceTypeTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_kyc_stat
    ON  td_kyc_stat.typeDetailID = kyc_stat.verifiedStatusTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_prof_seg
    ON  td_prof_seg.typeDetailID = occ.profileSegmentTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_sub_prof
    ON  td_sub_prof.typeDetailID = occ.subProfileTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_nhb_occ
    ON  td_nhb_occ.typeDetailID = occ.nhbQOccupationTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_occ_sub
    ON  td_occ_sub.typeDetailID = occ.occupationSubTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_const
    ON  td_const.typeDetailID = occ.constitutionTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_industry
    ON  td_industry.typeDetailID = occ.industryTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_sub_ind
    ON  td_sub_ind.typeDetailID = occ.subIndustryTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_emp_type
    ON  td_emp_type.typeDetailID = occ.employmentTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_sector
    ON  td_sector.typeDetailID = occ.sectorTypeDetailID
LEFT JOIN dmihfclos.tblTypeDetail td_risk
    ON  td_risk.typeDetailID = cr.current_riskTypeDetailID

WHERE ba.rn = 1  and isactive=1 ;