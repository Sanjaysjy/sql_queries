-- slv_collections_daily

CREATE TABLE silver.slv_collections_daily
DISTKEY(loan_due_status_id)
SORTKEY(loan_application_id, loan_due_status_id)
AS
WITH cte_receipt_agg AS (
    SELECT
        loanapplicationid,
        dateofcollection,
        COUNT(draftreceiptid) AS receipt_count,
        COALESCE(SUM(CAST(collectedamount AS DECIMAL(18, 2))), 0.00) AS receipt_amount,
        COALESCE(SUM(CAST(emidueamount   AS DECIMAL(18, 2))), 0.00)  AS emi_due_sum
    FROM dmihfclos.tbldraftmanualreceipt
    WHERE isactive = 1
    GROUP BY loanapplicationid, dateofcollection
),
 cte_presentation_agg AS (
    SELECT
        loanapplicationid,
        presentationdate,
        COUNT(presentationdetailid) AS presentation_count,
        COUNT(CASE WHEN bouncereasontypedetailid IS NOT NULL THEN 1 END) AS bounce_count
    FROM dmihfclos.tblloanapplicationpresentationdetail
    WHERE isactive = 1
    GROUP BY loanapplicationid,  presentationdate
) ,
cte_followup_latest AS (
    SELECT
        loanapplicationid,
        followuptypedetailid
    FROM (
        SELECT
            loanapplicationid,
            followuptypedetailid,
            ROW_NUMBER() OVER ( PARTITION BY loanapplicationid  ORDER BY followupdate DESC) AS rn
        FROM dmihfclos.tblcollectionfollowup
        WHERE isactive      = 1  AND islastrecord  = 1
    ) ranked_followup
    WHERE rn = 1
)
SELECT
    CAST(lds.loanduestatusid   AS BIGINT) AS loan_due_status_id,
    CAST(lds.loanapplicationid AS BIGINT) AS loan_application_id,
    CAST(lds.transactiondate   AS DATE) AS transaction_date,
    CAST(lds.pos AS DECIMAL(18, 2)) AS pos,

    -- verification on --> does emi_due_sum column has these two fields values combined (principal + interest) ??
    CAST(
        COALESCE(CAST(lds.unadjustedamount AS DECIMAL(18, 2)), CAST(0 AS DECIMAL(18, 2)))
        +
        COALESCE(CAST(ra.emi_due_sum       AS DECIMAL(18, 2)), CAST(0 AS DECIMAL(18, 2)))
        AS DECIMAL(18, 2)
    ) AS total_demand,

    CAST(lds.maxdeliquencyday  AS INT) AS max_dpd,
    lds.deliquencybktdetailid  AS dpd_bucket,
    CAST(COALESCE(CAST(lds.isnpa AS INT), 0) AS BOOLEAN) AS is_npa,
    COALESCE(CAST(ra.receipt_count AS INT), CAST(0 AS INT)) AS daily_receipt_count,
    COALESCE(CAST(ra.receipt_amount AS DECIMAL(18, 2)), CAST(0 AS DECIMAL(18, 2))) AS daily_receipt_amount,
    COALESCE(CAST(pa.presentation_count  AS INT), CAST(0 AS INT)) AS daily_presentations,
    COALESCE(CAST(pa.bounce_count        AS INT), CAST(0 AS INT)) AS daily_bounces,
    cf.followuptypedetailid AS followup_type,
    TRIM(lds.sourcefundingname) AS source_funding_name,
    CAST(lds.createdon AS TIMESTAMP) AS record_created_at,
    CAST(lds.lastmodifiedon AS TIMESTAMP) AS record_modified_a,
    CURRENT_TIMESTAMP AS silver_loaded_at,
    TO_CHAR(GETDATE(),'YYYYMMDD_HH24MISS') AS silver_batch_id
FROM dmihfclos.tblloanduestatus lds
LEFT JOIN cte_receipt_agg ra
    ON  ra.loanapplicationid = lds.loanapplicationid
    AND ra.dateofcollection  = lds.transactiondate
LEFT JOIN cte_presentation_agg pa
    ON  pa.loanapplicationid = lds.loanapplicationid
    AND pa.presentationdate  = lds.transactiondate
LEFT JOIN cte_followup_latest cf
    ON  cf.loanapplicationid = lds.loanapplicationid
WHERE lds.isactive = 1