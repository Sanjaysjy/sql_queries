-- -- slv_loan_details
-- CREATE TABLE silver.slv_loan_details
-- DISTSTYLE KEY
-- DISTKEY (loan_application_id)
-- SORTKEY (loan_application_id)
-- AS

WITH LoanApplicationDetails AS (
    SELECT
        la.loanApplicationID AS loan_application_id,
        UPPER(TRIM(la.applicationNumber)) AS application_number,
        la.salesforceID AS salesforce_id,
        la.leadID AS lead_id,
        ds.fleNo AS file_number,
        la.loanProductID AS product_id,

        p.productName AS product_name,
        lp.loanPurposeName AS loan_purpose,
        ts.typeDetailDisplayText AS loan_scheme,
        lt.typeDetailDisplayText AS interest_type,
             --                      as category,
        CASE WHEN la.isEmployeeLoan = '1' THEN TRUE ELSE FALSE END AS is_employee_loan,
        CAST(la.date AS DATE) AS application_date,
        CAST(lad.loginDate  as DATE) AS login_date,

        --         COALESCE(lsh.applicationAcceptedDate, la.createdOn) AS login_acceptance_date,  -- logic not found

        la.firstSanctionDate AS first_sanction_date,
        la.lastSanctionDate AS last_sanction_date,
        ds.firstDisbursedDate AS first_disbursal_date,
        ds.lastDisbursedDate AS last_disbursal_date,
        ds.emiStartDate AS emi_start_date,
        ds.emiEndDate AS emi_end_date,
        la.loanAmountRequest AS requested_amount,
        la.firstSanctionAmount AS first_sanction_amount,
        la.revisedSanctionAmount AS revised_sanction_amount,
        ds.totalDisbursedAmount AS total_disbursed_amount,
        ds.totalBookedAmount AS total_booked_amount,
        lad.totalInsuranceAmount AS total_insurance_amount,
        ds.documentValue AS document_value,
        la.tenure AS requested_tenure_months,
        la.sanctionedTenure AS sanctioned_tenure_months,
        lad.currentTenor AS current_tenure_months,
        lad.balanceTenor AS balance_tenure_months,
        la.sanctionedEMI AS sanctioned_emi,
        lad.currentEMI AS current_emi,
        ds.emiCycle AS emi_cycle_day,
        TRIM(la.finalStatus) AS final_status,

        ---need  confirmation
    --         CAST(NULL AS INTEGER) AS moratorium_months
        TRIM(ds.fundingSouce) AS funding_source,
        lad.inorganicType  AS inorganic_type,

        --  ROI
        lad.currentROI AS current_roi_pct

    FROM dmihfclos.tblLoanApplication la

    LEFT JOIN dmihfclos.mstLoanPurpose lp
        ON la.loanPurposeID = lp.loanPurposeID
        AND lp.isActive = 1

    LEFT JOIN dmihfclos.mstProduct p
        ON la.loanProductID = p.productID
        AND p.isActive = 1

    LEFT JOIN dmihfclos.tblLoanApplicationDisbursalDetail ds
        ON la.loanApplicationID = ds.loanApplicationID
        AND ds.isActive = 1

    LEFT JOIN dmihfclos.tblLoanApplicationAdditionalDetail lad
        ON la.loanApplicationID = lad.loanApplicationID
        AND lad.isActive = 1

    LEFT JOIN dmihfclos.tblLoanApplicationStatusHistory lsh
        ON la.loanApplicationID = lsh.loanApplicationID
        AND lsh.isActive = 1

    LEFT JOIN dmihfclos.tblTypeDetail ts
        ON la.loanSchemeTypeDetailID = ts.typeDetailID
        AND ts.isActive = 1

    LEFT JOIN dmihfclos.tblTypeDetail lt
        ON la.interestTypeTypeDetailID = lt.typeDetailID
        AND lt.isActive = 1
),

-- ROI
roi_spread  AS (
    SELECT
        loanApplicationID AS loan_application_id,
        baseRateInPercentage AS base_rate_pct,
        plrSpreadInPercentage AS plr_spread_pct,
        finalROI AS final_roi_pct,
        waiverRoiInPercentage AS waiver_roi_pct
    FROM (
        SELECT
            rs.*,
            ROW_NUMBER() OVER (
                PARTITION BY rs.loanApplicationID
                ORDER BY rs.lastModifiedOn DESC
            ) AS rn
        FROM dmihfclos.tblLoanApplicationRoiSpread rs
        WHERE rs.isActive = 1
    ) t
    WHERE rn = 1
),

RuleEngineResponse AS (
    SELECT
        loanApplicationID AS loan_application_id,
        loanOfferAmount AS eligible_loan_amount,
        -- loanOfferAmount - COALESCE(premium,0)  AS eligible_amount_wo_ins,  --->>>  premium column not there
        proposedEmi AS proposed_emi,
        proposedTenor AS proposed_tenor,
        incomeConsidered AS income_considered,
        obligationConsidered AS obligation_considered,
        propertyValue AS property_value_considered,
        finalFOIR AS final_foir_pct,
        finalLTV AS final_ltv_pct,
        combinedLTV AS combined_ltv_pct,
        insr AS insr_pct,
        foirNorm AS foir_norm,
        ltvNorm AS ltv_norm
    FROM (
        SELECT
            rer.*,
            ROW_NUMBER() OVER (
                PARTITION BY rer.loanApplicationID
                ORDER BY rer.ruleEngineResponseID DESC
            ) rn
        FROM dmihfclos.tblLoanApplicationRuleEngineResponse rer
        WHERE rer.outputType = 'sanction'
    ) t
    WHERE rn = 1
),

LoanApplicationWaiver AS (
    SELECT
        la.loanApplicationID AS loan_application_id,
        COALESCE(w.foirWaiver, 0.0) AS foir_waiver,
        COALESCE(w.ltvWaiver, 0.0) AS ltv_waiver,
        COALESCE(w.waiverTenure, 0.0) AS tenure_waiver,
        COALESCE(w.lcrWaiver, 0.0) AS lcr_waiver,
        COALESCE(w.insrWaiver, 0.0) AS insr_waiver
    FROM dmihfclos.tblLoanApplication la
    LEFT JOIN dmihfclos.tblLoanApplicationWaiver w
        ON la.loanApplicationID = w.loanApplicationID
    WHERE la.isActive = 1
),

LoanApplicationSummary AS (
    SELECT
        lad.*,
        lw.foir_waiver,
        lw.ltv_waiver,
        lw.tenure_waiver,
        lw.lcr_waiver,
        lw.insr_waiver
    FROM LoanApplicationDetails lad
    LEFT JOIN LoanApplicationWaiver lw
        ON lad.loan_application_id = lw.loan_application_id
),

--  DECISION METRICS
disbursal_details AS (
    SELECT
        loanApplicationID AS loan_application_id,
        CAST(ltv AS DECIMAL(10,2)) AS dltv,
        CAST(ltvWithInsurance AS DECIMAL(10,2)) AS dltv_with_insurance,
        CAST(lcr AS DECIMAL(10,2)) AS dlcr,
        CAST(lcrWithInsurance AS DECIMAL(10,2)) AS dlcr_with_insurance,
        CAST(combinedDltv AS DECIMAL(10,2)) AS combined_dltv,
        CAST(combinedDlcr AS DECIMAL(10,2)) AS combined_dlcr
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY loanApplicationID
                ORDER BY lastModifiedOn DESC
            ) rn
        FROM dmihfclos.tblLoanApplicationDisbursalDetail
        WHERE isActive = 1
    ) t
    WHERE rn = 1
),

SourcingChannelDetails AS (
    SELECT
        la.loanApplicationID AS loan_application_id,
        la.channelPartnerDsaID AS channel_partner_dsa_id,
        dr.companyName AS channel_partner_name,
        la.salesOfficerEmpID AS sales_officer_emp_id,
        la.creditOfficerEmpID AS credit_officer_emp_id,
        lad.branchID AS branch_id,
        lad.branchName AS branch_name,
        lad.regionName AS region_name,
        lad.zoneName AS zone_name
    FROM dmihfclos.tblLoanApplication la
    LEFT JOIN dmihfclos.tblLoanApplicationAdditionalDetail lad
        ON la.loanApplicationID = lad.loanApplicationID
        AND lad.isActive = 1
    LEFT JOIN dmihfclos.tblDsaRenewal dr
        ON la.channelPartnerDsaID = dr.dsaID
        AND dr.isActive = 1
),

LinkedLoans AS (
    SELECT
        loanApplicationID AS loan_application_id,
        CASE WHEN COUNT(*) > 0 THEN TRUE ELSE FALSE END AS is_link_loan,
        COUNT(linkedLoanApplicationID) AS linked_loan_count,
        MAX(isSameProperty) AS is_same_property_link,
        LISTAGG(linkedLoanApplicationID, ',')
        WITHIN GROUP (ORDER BY linkedLoanApplicationID) AS linked_loans
    FROM dmihfclos.tblLoanApplicationLinking
    WHERE isActive = 1
    GROUP BY loanApplicationID
),

tvr_Details AS (
    SELECT
        tvr.loanApplicationID AS loan_application_id,
        CASE
            WHEN MAX(tvr.tvrChecklistID) IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS tvr_bribe_flag
    FROM dmihfclos.tblLoanApplicationTvrChecklist tvr
    WHERE isActive = 1
    GROUP BY tvr.loanApplicationID
),

audit_details AS (
    SELECT
        la.loanApplicationID AS loan_application_id,
        la.createdOn AS record_created_at,
        la.lastModifiedOn AS record_modified_at,
        CURRENT_TIMESTAMP AS silver_loaded_at,
        TO_CHAR(GETDATE(),'YYYYMMDD_HH24MISS') AS silver_batch_id
    FROM dmihfclos.tblLoanApplication la
    WHERE la.isActive = 1
),

LoanApplicationAdditionalDetail AS (
    SELECT
        lad.loanApplicationID AS loan_application_id,
        lad.branchID AS branch_id,
        lad.branchName AS branch_name,
        lad.zoneName AS zone_name,
        lad.regionName AS region_name,
        lad.salesOfficerName AS sales_officer_name_code,
        lad.salesOfficerDesignation AS sales_officer_designation
    FROM dmihfclos.tblLoanApplicationAdditionalDetail lad
    WHERE lad.isActive = 1
)
--     ,final as (
SELECT

-- LoanApplicationSummary (ls)
ls.loan_application_id,
ls.application_number,
ls.salesforce_id,
ls.lead_id,
ls.file_number,
ls.product_id,
ls.product_name,
ls.loan_purpose,
ls.loan_scheme,
ls.interest_type,
ls.is_employee_loan,
ls.application_date,
ls.login_date,
ls.first_sanction_date,
ls.last_sanction_date,
ls.first_disbursal_date,
ls.last_disbursal_date,
ls.emi_start_date,
ls.emi_end_date,
ls.requested_amount,
ls.first_sanction_amount,
ls.revised_sanction_amount,
ls.total_disbursed_amount,
ls.total_booked_amount,
ls.total_insurance_amount,
ls.document_value,
ls.requested_tenure_months,
ls.sanctioned_tenure_months,
ls.current_tenure_months,
ls.balance_tenure_months,
ls.sanctioned_emi,
ls.current_emi,
ls.emi_cycle_day,
ls.final_status,
ls.funding_source,
ls.inorganic_type,
ls.current_roi_pct,
ls.foir_waiver,
ls.ltv_waiver,
ls.tenure_waiver,
ls.lcr_waiver,
ls.insr_waiver,

-- ROI
rs.base_rate_pct,
rs.plr_spread_pct,
rs.final_roi_pct,
rs.waiver_roi_pct,

-- ELIGIBILITY
rer.eligible_loan_amount,
rer.proposed_emi,
rer.proposed_tenor,
rer.income_considered,
rer.obligation_considered,
rer.property_value_considered,
rer.final_foir_pct,
rer.final_ltv_pct,
rer.combined_ltv_pct,
rer.insr_pct,
rer.foir_norm,
rer.ltv_norm,

-- DECISION METRICS
dd.dltv,
dd.dltv_with_insurance,
dd.dlcr,
dd.dlcr_with_insurance,
dd.combined_dltv,
dd.combined_dlcr,

-- SOURCING
sc.channel_partner_dsa_id,
sc.channel_partner_name,
sc.sales_officer_emp_id,
sc.credit_officer_emp_id,
sc.branch_id,
sc.branch_name,
sc.region_name,
sc.zone_name,

-- Linked loans
ll.is_link_loan,
ll.linked_loan_count,
ll.is_same_property_link,
ll.linked_loans,

-- TVR
tvr.tvr_bribe_flag,

-- AUDIT
ad.record_created_at,
ad.record_modified_at,
ad.silver_loaded_at,
ad.silver_batch_id,

-- EMPLOYEE
-- CAST(NULL AS BIGINT) AS sales_officer_emp_id,
-- CAST(NULL AS BIGINT) AS credit_officer_emp_id,
-- CAST(NULL AS VARCHAR) AS sales_officer_emp_Name_Code,
-- CAST(NULL AS VARCHAR) AS credit_officer_emp_Name_Code,

-- Additional Detail
lad.sales_officer_name_code,
lad.sales_officer_designation

FROM LoanApplicationSummary ls

LEFT JOIN LoanApplicationAdditionalDetail lad
    ON ls.loan_application_id = lad.loan_application_id

LEFT JOIN roi_spread rs
    ON ls.loan_application_id = rs.loan_application_id

LEFT JOIN RuleEngineResponse rer
    ON ls.loan_application_id = rer.loan_application_id

LEFT JOIN disbursal_details dd
    ON ls.loan_application_id = dd.loan_application_id

LEFT JOIN SourcingChannelDetails sc
    ON ls.loan_application_id = sc.loan_application_id

LEFT JOIN LinkedLoans ll
    ON ls.loan_application_id = ll.loan_application_id

LEFT JOIN tvr_Details tvr
    ON ls.loan_application_id = tvr.loan_application_id

LEFT JOIN audit_details ad
    ON ls.loan_application_id = ad.loan_application_id
-- )select count(*) from final