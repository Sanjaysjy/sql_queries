WITH
-- bounce_flags AS (
--     SELECT
--         loan_application_id,
--         due_date,
--         CASE WHEN is_cleared = FALSE OR bounce_reason IS NOT NULL THEN 1 ELSE 0 END AS is_bounced,
--         ROW_NUMBER() OVER (PARTITION BY loan_application_id ORDER BY due_date DESC)  AS rn
--     FROM silver.slv_repayment_behavior
-- ),
-- first_clear_rn AS (
--     SELECT
--         loan_application_id,
--         MIN(rn) AS first_clear_pos
--     FROM bounce_flags
--     WHERE is_bounced = 0
--     GROUP BY loan_application_id
-- ),
-- cte_consecutive_bounces AS (
--     SELECT
--         bf.loan_application_id,
--         COUNT(*) AS consecutive_bounces
--     FROM bounce_flags bf
--     LEFT JOIN first_clear_rn fc ON bf.loan_application_id = fc.loan_application_id
--     WHERE bf.is_bounced = 1
--       AND (fc.first_clear_pos IS NULL OR bf.rn < fc.first_clear_pos)
--     GROUP BY bf.loan_application_id
-- ),
dpd_ranked AS (
    SELECT
        loan_application_id,
        max_dpd,
        ROW_NUMBER() OVER (PARTITION BY loan_application_id ORDER BY snapshot_date DESC) AS rn
    FROM silver.slv_contract_perf_monthly
),
dpd_pivot AS (
    SELECT
        loan_application_id,
        MAX(CASE WHEN rn = 1 THEN max_dpd END) AS dpd_m1,
        MAX(CASE WHEN rn = 2 THEN max_dpd END) AS dpd_m2,
        MAX(CASE WHEN rn = 3 THEN max_dpd END) AS dpd_m3
    FROM dpd_ranked
    WHERE rn <= 3
    GROUP BY loan_application_id
),
cte_dpd_trend AS (
    SELECT
        loan_application_id,
        CASE
            WHEN dpd_m1 > dpd_m2 AND dpd_m2 > dpd_m3 THEN 'Worsening'
            WHEN dpd_m1 < dpd_m2 AND dpd_m2 < dpd_m3 THEN 'Improving'
            ELSE 'Stable'
        END AS dpd_trend_direction
    FROM dpd_pivot
),
customer_ranked AS (
    SELECT
        loan_application_id,
        entity_id,
        ROW_NUMBER() OVER (
            PARTITION BY loan_application_id
            ORDER BY CASE WHEN is_main_applicant = TRUE THEN 0 ELSE 1 END, record_modified_at DESC
        ) AS rn
    FROM silver.slv_customer
),
customer_dedup AS (
    SELECT
        loan_application_id,
        entity_id
    FROM customer_ranked
    WHERE rn = 1
),
entity_dpd AS (
    SELECT
        cd1.loan_application_id,
        MAX(cpm.max_dpd) AS max_entity_dpd
    FROM customer_dedup cd1
    JOIN customer_dedup cd2 ON cd1.entity_id = cd2.entity_id
    JOIN silver.slv_contract_perf_monthly cpm ON cd2.loan_application_id = cpm.loan_application_id
    GROUP BY cd1.loan_application_id
),
cte_multi_loan_stress AS (
    SELECT
        loan_application_id,
        CASE WHEN max_entity_dpd > 0 THEN TRUE ELSE FALSE END AS multi_loan_stress
    FROM entity_dpd
),
cte_asset AS (
    SELECT
        loan_application_id,
        CASE
            WHEN MAX(CASE WHEN is_negative_area = TRUE THEN 1 ELSE 0 END) = 1
            THEN TRUE ELSE FALSE
        END AS negative_area_flag
    FROM silver.slv_asset
    GROUP BY loan_application_id
),
cte_base AS (
    SELECT
        ld.loan_application_id,
        CURRENT_DATE                                                                        AS signal_date,
        -- COALESCE(cb.consecutive_bounces, 0) AS consecutive_bounces,
        -- emi_increase_stress: column current_emi not present in silver.slv_loan_details DDL
        -- ltv_breach_flag: columns currentLTV and ltvNorm not present in silver.slv_contract_perf_monthly DDL
        dt.dpd_trend_direction,
        COALESCE(ast.negative_area_flag, FALSE)                                             AS negative_area_flag,
        COALESCE(mls.multi_loan_stress, FALSE)                                              AS multi_loan_stress,
        CASE WHEN ld.final_foir_pct > 65 THEN TRUE ELSE FALSE END                          AS high_foir_flag
    FROM silver.slv_loan_details ld
    -- LEFT JOIN cte_consecutive_bounces cb  ON ld.loan_application_id = cb.loan_application_id
    LEFT JOIN cte_dpd_trend           dt  ON ld.loan_application_id = dt.loan_application_id
    LEFT JOIN cte_multi_loan_stress   mls ON ld.loan_application_id = mls.loan_application_id
    LEFT JOIN cte_asset               ast ON ld.loan_application_id = ast.loan_application_id
),
cte_scored AS (
    SELECT
        loan_application_id,
        signal_date,
        -- consecutive_bounces,
        -- emi_increase_stress: column current_emi not present in silver.slv_loan_details DDL
        -- ltv_breach_flag: columns currentLTV and ltvNorm not present in silver.slv_contract_perf_monthly DDL
        dpd_trend_direction,
        negative_area_flag,
        multi_loan_stress,
        high_foir_flag,
        (
            -- CASE
            --     WHEN consecutive_bounces >= 3 THEN 15
            --     WHEN consecutive_bounces = 2  THEN 10
            --     WHEN consecutive_bounces = 1  THEN 5
            --     ELSE 0
            -- END
            CASE
                WHEN dpd_trend_direction = 'Worsening' THEN 20
                WHEN dpd_trend_direction = 'Stable'    THEN 10
                ELSE 0
            END
            + CASE WHEN high_foir_flag    = TRUE THEN 10 ELSE 0 END
            + CASE WHEN multi_loan_stress = TRUE THEN 10 ELSE 0 END
        ) AS ews_risk_score
    FROM cte_base
)
SELECT
    ROW_NUMBER() OVER (ORDER BY loan_application_id, signal_date)                           AS ews_id,
    loan_application_id,
    signal_date,
    -- consecutive_bounces,
    dpd_trend_direction,
    -- emi_increase_stress: column current_emi not present in silver.slv_loan_details DDL
    -- ltv_breach_flag: columns currentLTV and ltvNorm not present in silver.slv_contract_perf_monthly DDL
    negative_area_flag,
    multi_loan_stress,
    high_foir_flag,
    ews_risk_score,
    CASE
        WHEN ews_risk_score BETWEEN 0  AND 25  THEN 'Green'
        WHEN ews_risk_score BETWEEN 26 AND 50  THEN 'Amber'
        WHEN ews_risk_score BETWEEN 51 AND 75  THEN 'Red'
        WHEN ews_risk_score BETWEEN 76 AND 100 THEN 'Critical'
    END                                                                                     AS ews_risk_grade,
    CASE
        WHEN ews_risk_score BETWEEN 76 AND 100 AND negative_area_flag = TRUE
            THEN 'Immediate field visit required. Initiate legal notice and NPA provisioning review. Flag for collateral re-valuation.'
        WHEN ews_risk_score BETWEEN 76 AND 100
            THEN 'Immediate field visit required. Initiate legal notice and NPA provisioning review.'
        WHEN ews_risk_score BETWEEN 51 AND 75  AND negative_area_flag = TRUE
            THEN 'Telephonic follow-up. Evaluate OTS or restructuring options. Flag for collateral re-valuation.'
        WHEN ews_risk_score BETWEEN 51 AND 75
            THEN 'Telephonic follow-up. Evaluate OTS or restructuring options.'
        WHEN ews_risk_score BETWEEN 26 AND 50  AND negative_area_flag = TRUE
            THEN 'Monitor closely. Send payment reminder communication. Flag for collateral re-valuation.'
        WHEN ews_risk_score BETWEEN 26 AND 50
            THEN 'Monitor closely. Send payment reminder communication.'
        WHEN negative_area_flag = TRUE
            THEN 'No action required. Standard monitoring applies. Flag for collateral re-valuation.'
        ELSE 'No action required. Standard monitoring applies.'
    END                                                                                     AS recommended_action,
    SYSDATE                                                                                 AS gold_loaded_at,
    TO_CHAR(SYSDATE, 'YYYYMMDD_HH24MISS')                                                  AS gold_batch_id,
    CASE
        WHEN CURRENT_DATE = LAST_DAY(CURRENT_DATE) THEN CURRENT_DATE
        ELSE LAST_DAY(CURRENT_DATE)
    END                                                                                     AS reporting_period
FROM cte_scored;



/*   Report as of today  and month End.(history)
1.Non-starter and (SMA-1 or above)   : tblLoanApplication statusTypeDetailID =189,tblLoanApplicationPaySchedule ,paymentReceivedDate is null and  clearenceFlag is null or 0 or false, tblLoanDueStatus deliquencyBktDetailID    in 2570,2571,2572
2.Active Loan Account and SMA-2  :   tblLoanApplication statusTypeDetailID =189,tblLoanDueStatus deliquencyBktDetailID =2571
3.ACH not registered & (SMA-1 or above)   : tblLoanApplication statusTypeDetailID =189, tblLoanApplicationACH where registered is null or false and registeredStatusTypeDetailId is null or not equal to 1991  ,tblLoanDueStatus deliquencyBktDetailID  in 2570,2571,2572
4.Construction not started even after 18 months and SMA-1 : tblLoanApplication statusTypeDetailID =189,  only first tranch is disbursed 18 month before the current date and  loan is not fully disbursed yet and loanPurposeCode in ('PCBUILDER', 'RENOVATION','PLOTCONSTRUCTION' ) ,tblLoanDueStatus deliquencyBktDetailID =2570
5. Title document & delay of more than 3 months : tblLoanApplication statusTypeDetailID = 189,tblLoanLegalDocuments where documentId in (1,3,16,70,81,118,130,209) and documentCategoryDetailId in (3256)  and  documentTypeDetailId in(1957) and documentStatusTypeDetailId  IS NULL OR documentStatusTypeDetailId not in (1948,1949,1951) and first disbursement date  is more than 3 months
6.More than 3 bounce in current financial year & SMA-1  :  tblLoanApplication statusTypeDetailID =189, tblLoanApplicationChargeDetails where chargesForTypeDetailID =1561 and typeDetailChargeID =1 and chargeDate,tblLoanDueStatus deliquencyBktDetailID =2570

*/