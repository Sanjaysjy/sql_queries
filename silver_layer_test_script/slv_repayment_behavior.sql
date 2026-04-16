--  slv_repayment_behavior

CREATE TABLE silver.slv_repayment_behavior
DISTKEY(loan_application_id)
SORTKEY(pay_schedule_id)
AS


WITH ps_dedup AS (
    SELECT
        payscheduleid,
        loanapplicationid,
        head,
        duedate,
        dueamount,
        principal,
        interst,
        closingbalance,
        applicableroi,
        paymentreceiveddate,
        delinquentdays,
        clearenceflag,
        prepayment,
        bouncingchargewithtax,
        penalchargewithtax,
        createddate,
        lastmodifieddate,
        ROW_NUMBER() OVER ( PARTITION BY payscheduleid ORDER BY lastmodifieddate DESC) AS rn
    FROM dmihfclos.tblloanapplicationpayschedule
    WHERE isactive = 1
)
,
manual_receipt AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER ( PARTITION BY loanapplicationid ORDER BY createdon DESC ) AS rn
        FROM dmihfclos.tblloanapplicationManualReceipt
        WHERE isactive = 1
    ) t
    WHERE rn = 1
    -- select
    --     loanapplicationid,
    --     unadjustmentamount,
    --     collectedthroughtypedetailid
    -- from dmihfclos.tblloanapplicationManualReceipt
    -- where isactive=1

)
,pd_dedup AS (
    SELECT
        loanapplicationid,
        pdcachdate,
        statustypedetailid,
        bouncereasontypedetailid,
        ispresented,
        ROW_NUMBER() OVER (
            PARTITION BY loanapplicationid, pdcachdate
            ORDER BY lastmodifiedon DESC
        ) AS rn
    FROM dmihfclos.tblloanapplicationpresentationdetail
    WHERE isactive = 1
)
SELECT
    ps.payscheduleid::BIGINT AS pay_schedule_id,
    ps.loanapplicationid::BIGINT  AS loan_application_id,
    UPPER(TRIM(ps.head))  AS instalment_head,
    ps.duedate::DATE AS due_date,
    ps.dueamount::DECIMAL(18,2) AS due_amount,
    ps.principal::DECIMAL(18,2) AS principal_component,
    ps.interst::DECIMAL(18,2) AS interest_component,
    ps.closingbalance::DECIMAL(18,2) AS closing_balance,
    ps.applicableroi::DECIMAL(18,2) AS applicable_roi,
    ps.paymentreceiveddate::DATE AS payment_received_date,
    ps.delinquentdays::BIGINT AS delinquent_days,
    CASE WHEN ps.clearenceflag = 1 THEN TRUE ELSE FALSE END AS is_cleared,
    mr.unadjustmentamount  as unadjusted_amount,
    ps.prepayment::DECIMAL(18,2) AS prepayment_amount,
    ps.bouncingchargewithtax::DECIMAL(18,2) AS bouncing_charge,
    ps.penalchargewithtax::DECIMAL(18,2) AS penal_charge,


    --17. [bouncing_adjusted]   matching source field not present
    --18.  [penal_adjusted]:    matching source field not present

    pd.ispresented AS presentation_status,
    pd.bouncereasontypedetailid AS bounce_reason,

    mr.collectedthroughtypedetailid as presentation_mode,

    -- [batch_no]    matching source field not present
    -- [reconciliation_date]    matching source field not present


    CASE
        WHEN ps.delinquentdays = 0 THEN '0'
        WHEN ps.delinquentdays BETWEEN 1  AND 30     THEN 'SMA-0'
        WHEN ps.delinquentdays BETWEEN 31 AND 60     THEN 'SMA-1'
        WHEN ps.delinquentdays BETWEEN 61 AND 90     THEN 'SMA-2'
        WHEN ps.delinquentdays BETWEEN 90 AND 365    THEN 'NPA (Sub-standard)'
        WHEN ps.delinquentdays BETWEEN 366 AND 730   THEN  'Doubtful 1 (D1)'
        WHEN ps.delinquentdays BETWEEN 731 AND 1095   THEN  'Doubtful 2 (D2)'
        WHEN ps.delinquentdays > 1096  THEN  'Doubtful 3 (D3)'
        ELSE NULL
    END AS dpd_bucket,
    CASE
        WHEN ps.duedate < CURRENT_DATE
         AND (ps.clearenceflag = 0 OR ps.clearenceflag IS NULL)  THEN TRUE
        ELSE FALSE
    END AS is_overdue,


    ps.createddate::TIMESTAMP AS record_created_at,
    ps.lastmodifieddate::TIMESTAMP AS record_modified_at,
    CURRENT_TIMESTAMP AS silver_loaded_at,
    TO_CHAR(GETDATE(),'YYYYMMDD_HH24MISS') AS silver_batch_id
FROM ps_dedup ps
LEFT JOIN pd_dedup pd
    ON  ps.loanapplicationid = pd.loanapplicationid
    AND ps.duedate = pd.pdcachdate
    AND pd.rn = 1
LEFT JOIN manual_receipt mr
    ON  ps.loanapplicationid = mr.loanApplicationID
WHERE ps.rn = 1

