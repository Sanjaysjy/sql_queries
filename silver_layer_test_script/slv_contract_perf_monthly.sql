WITH MonthlyLoanPerformance AS (
  SELECT
    lm.loanMonthlyID AS loan_monthly_id,
    lm.loanApplicationID AS loan_application_id,
    DATE(MAX(lm.transactionDate) OVER (PARTITION BY lm.loanMonthlyID)) AS snapshot_date,
    lm.pos,
    lm.sellPos AS sell_pos,
    lm.dueInterest AS due_interest,
    lm.duePrinciple AS due_principal,
    lm.previousDue AS previous_due,
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
--     lm. AS  loan_status, 
--

    lm.provisionsValue AS provisions_value,
    lm.riskWeight AS risk_weight,
    lm.SourceFundingName AS source_funding_name,
    lm.liabilityCode AS liability_code,
    lm.roi AS roi,
    lm.balanceTenor AS balance_tenur,
    lm.createdOn   AS  record_created_at,
    lm.lastModifiedOn AS record_modified_at,
    CURRENT_TIMESTAMP AS silver_loaded_at,
    TO_CHAR(GETDATE(),'YYYYMMDD_HH24MISS') AS silver_batch_id
  FROM
    dmihfclos.tblLoanMonthly lm
  LEFT JOIN
    dmihfclos.tblTypeDetail td ON lm.maxDeliquencyDay = td.typeDetailID
)
SELECT
  loan_monthly_id,
  loan_application_id,
  snapshot_date,
  pos,
  sell_pos,
  due_interest,
  due_principal,
  previous_due,
  max_dpd,
--   dpd_bucket,
  is_npa,
  npa_start_date,
  asset_classification,
  is_restructured,
--   ecl_stage,
--   probability_of_default,
--   loss_given_default,
  provisions_value,
  risk_weight,
  source_funding_name,
  liability_code,
  roi,
  balance_tenur,
--   loan_status,
  record_created_at,
  record_modified_at,
  silver_loaded_at,
  silver_batch_id
FROM
  MonthlyLoanPerformance;
