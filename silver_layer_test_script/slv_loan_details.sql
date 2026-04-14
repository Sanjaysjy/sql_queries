WITH LoanApplicationDetails AS (
    SELECT
    la.isactive,
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
        CASE WHEN la.isEmployeeLoan = '1' THEN TRUE ELSE FALSE END AS is_employee_loan,
        CAST(la.date AS DATE) AS application_date,
        CAST(lad.loginDate AS DATE) AS login_date,
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
        TRIM(ds.fundingSouce) AS funding_source,
        lad.inorganicType AS inorganic_type,
        lad.currentROI AS current_roi_pct
FROM dmihfclos.tblLoanApplication la
left JOIN dmihfclos.mstLoanPurpose lp
    ON la.loanPurposeID = lp.loanPurposeID
    AND lp.isActive = 1
left JOIN dmihfclos.mstProduct p
    ON la.loanProductID = p.productID
    AND p.isActive = 1
left JOIN dmihfclos.tblLoanApplicationDisbursalDetail ds
    ON la.loanApplicationID = ds.loanApplicationID
    AND ds.isActive = 1
left JOIN dmihfclos.tblLoanApplicationAdditionalDetail lad
    ON la.loanApplicationID = lad.loanApplicationID
    AND lad.isActive = 1
left JOIN dmihfclos.tblLoanApplicationStatusHistory lsh
    ON la.loanApplicationID = lsh.loanApplicationID
    AND lsh.isActive = 1
left JOIN dmihfclos.tblTypeDetail ts
    ON la.loanSchemeTypeDetailID = ts.typeDetailID
    AND ts.isActive = 1
left JOIN dmihfclos.tblTypeDetail lt
    ON la.interestTypeTypeDetailID = lt.typeDetailID
    AND lt.isActive = 1
WHERE la.isActive = 1
),
roi_spread AS (
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
            ) rn
        FROM dmihfclos.tblLoanApplicationRoiSpread rs
        WHERE rs.isActive = 1
    ) t
    WHERE rn = 1
),
RuleEngineResponse AS (
    SELECT
        loanApplicationID AS loan_application_id,
        loanOfferAmount AS eligible_loan_amount,
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
            AND rer.isActive = 1
    ) t
    WHERE rn = 1
),
-- LoanDPD AS (
--     SELECT
--         loanApplicationID,
--         MAX(dpd) AS current_due_day
-- FROM dmihfclos.tblLoanMonthly
-- WHERE isActive = 1
--     GROUP BY loanApplicationID
-- ),

audit_details AS (
    SELECT
        loanApplicationID AS loan_application_id,
        createdOn AS record_created_at,
        lastModifiedOn AS record_modified_at,
        CURRENT_TIMESTAMP AS silver_loaded_at,
        TO_CHAR(GETDATE(),'YYYYMMDD_HH24MISS') AS silver_batch_id
    FROM dmihfclos.tblLoanApplication
    WHERE isActive = 1
)
-- , final as (
SELECT
    ls.*,
    -- dpd.current_due_day,
    rs.base_rate_pct,
    rs.plr_spread_pct,
    rs.final_roi_pct,
    rs.waiver_roi_pct,
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
    ad.record_created_at,
    ad.record_modified_at,
    ad.silver_loaded_at,
    ad.silver_batch_id

FROM LoanApplicationDetails ls
LEFT JOIN roi_spread rs
    ON ls.loan_application_id = rs.loan_application_id
LEFT JOIN RuleEngineResponse rer
    ON ls.loan_application_id = rer.loan_application_id
-- LEFT JOIN LoanDPD dpd
--     ON ls.loan_application_id = dpd.loanApplicationID
LEFT JOIN audit_details ad
    ON ls.loan_application_id = ad.loan_application_id
-- )    select count(loan_application_id) from final

    -- select loan_application_id, count(*) from final group by loan_application_id  having  count(loan_application_id) >1