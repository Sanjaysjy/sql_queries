  -- slv_contract_perf_monthly
drop table if exists silver.slv_contract_perf_monthly;


CREATE TABLE silver.slv_contract_perf_monthly
DISTKEY(loan_application_id)
SORTKEY(loan_application_id)
AS

  SELECT
    lm.loanMonthlyID AS loan_monthly_id,
    lm.loanApplicationID AS loan_application_id,
    DATE(MAX(lm.transactionDate) OVER (PARTITION BY lm.loanMonthlyID)) AS snapshot_date,
    lm.pos,
    lm.sellPos AS sell_pos,
    lm.dueInterest AS due_interest,
    lm.duePrinciple AS due_principal,
    lm.previousDue AS previous_due,
    lm.outstandinginsurance AS outstanding_insurance,
    lm.accuredinterestnotdue    AS accrued_interest_notdue,
    lm.maxDeliquencyDay AS max_dpd,
    lm.isNPA AS is_npa,
    lm.npaStartDate AS npa_start_date,
    lm.assetClassification AS asset_classification,
    CASE
    WHEN lm.isRestructured IN ('1') THEN TRUE
    WHEN lm.isRestructured IN ('0','NULL') THEN FALSE
    END AS is_restructured, -- 1 = ture but null = false for now have to ask

--     td. AS dpd_bucket,   --- need's comfirmation
--     --need clarification on dpd days for staging
--     CASE
--         WHEN lm.maxDeliquencyDay <= 30 THEN 'Stage 1'
--         WHEN lm.maxDeliquencyDay BETWEEN 31 AND 90 THEN 'Stage 2'
--         WHEN lm.maxDeliquencyDay > 90 THEN 'Stage 3'
--         ELSE NULL
--     END AS ecl_stage,
--
-- -- missing column for this or logic to use on
--     lm.probability_of_default
--     lm.loss_given_default
-- confirmation on which column to use
    lm.statustypedetailid AS  loanstatus_typedetail_id,
    tb.typeDetailDisplayText  as loan_status,


    lm.provisionsValue AS provisions_value,
    lm.riskWeight AS risk_weight,
    lm.SourceFundingName AS source_funding_name,
    lm.liabilityCode AS liability_code,

    /*   have to make the changes */
--     lm.     AS partner_name,
--     lm.     AS engagement_date,   -- engagement id -->>  tblporfoli engg details -- > take the start date  -- and partner id -- mstporfoliapartner -- to get the parner name
--   engagement type and assignmenttype  concat both -- for -- >  engagement_Type column
    lm.roi AS roi,

    lm.assetclassificationdate  as asset_classification_date,
    lm.currentltv  as current_ltv,
    lm.fixedstartdate  as fixed_start_date,
    lm.collectionstatus  as collection_status,
    lm.presentationstatus  as presentation_status,
    lm.collectionbkt  as collection_bkt,
    lm.fixedduration  as fixed_duration,


    lm.balanceTenor AS balance_tenur,
    lm.createdOn   AS  record_created_at,
    lm.lastModifiedOn AS record_modified_at,
    CURRENT_TIMESTAMP AS silver_loaded_at,
    TO_CHAR(GETDATE(),'YYYYMMDD_HH24MISS') AS silver_batch_id
FROM
    dmihfclos.tblLoanMonthly lm
LEFT JOIN dmihfclos.tblTypeDetail curr_status
    ON la.statustypedetailid = curr_status.typeDetailID
    AND curr_status.isActive = 1
  LEFT JOIN
    dmihfclos.tblTypeDetail td ON lm.maxDeliquencyDay = td.typeDetailID
        and  td.isactive=1  ;