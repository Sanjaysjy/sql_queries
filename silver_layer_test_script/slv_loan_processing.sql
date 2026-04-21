-- slv_loan_processing
CREATE TABLE silver.slv_loan_processing
DISTKEY (loan_application_id)
SORTKEY (loan_application_id)
AS

WITH cte_disbursal_agg AS (
    SELECT
        loanapplicationid,
        CAST(COUNT(*) AS INTEGER) AS total_tranches,
        MAX(disburseddate) AS last_tranche_date,
        MIN(createdon) AS record_created_at,
        MAX(lastmodifiedon) AS record_modified_at
    FROM dmihfclos.tblloanapplicationdisbursmentfavouring
    where isactive= 1
    GROUP BY loanapplicationid
),
cte_disbursal_latest AS (
    SELECT
        loanapplicationid,
        disbursedamount,
        favouringcategorytypedetailid,
        transactiontypedetailid
    FROM (
        SELECT
            loanapplicationid,
            disbursedamount,
            favouringcategorytypedetailid,
            transactiontypedetailid,
            ROW_NUMBER() OVER ( PARTITION BY loanapplicationid ORDER BY disburseddate DESC, favouringid DESC) AS rn
        FROM dmihfclos.tblloanapplicationdisbursmentfavouring
        where isactive=1
    ) ranked_disbursal
    WHERE rn = 1
),
cte_pd_deduped AS (
    SELECT
        loanapplicationid,
        pddonebyentityid,
        pdstatustypedetailid,
        dateperformed
    FROM (
        SELECT
            loanapplicationid,
            pddonebyentityid,
            pdstatustypedetailid,
            dateperformed,
            ROW_NUMBER() OVER ( PARTITION BY loanapplicationid ORDER BY lastmodifiedon DESC ) AS rn
        FROM dmihfclos.tblloanapplicationpdreport
        where isactive =1
    ) ranked_pd
    WHERE rn = 1
),
cte_status_history_deduped AS (
    SELECT
        loanapplicationid,
        applicationaccepteddate
    FROM (
        SELECT
            loanapplicationid,
            applicationaccepteddate,
            ROW_NUMBER() OVER ( PARTITION BY loanapplicationid ORDER BY lastmodifiedon DESC) AS rn
        FROM dmihfclos.tblloanapplicationstatushistory
        WHERE isapplicationaccepted = 1  and isactive=1
    ) ranked_status
    WHERE rn = 1
)
SELECT
    CAST(da.loanapplicationid AS BIGINT) AS loan_application_id,
    da.total_tranches AS total_tranches,
    CAST(da.last_tranche_date AS DATE) AS last_tranche_date,
    CAST(dl.disbursedamount AS DECIMAL(14,2)) AS last_tranche_amount,

    CAST(dl.favouringcategorytypedetailid AS VARCHAR(200))  AS cheque_favouring_category,
    CAST(dl.transactiontypedetailid AS VARCHAR(200))  AS payment_type,

    CAST(pd.pddonebyentityid AS VARCHAR(200)) AS pd_done_by,
    CAST(pd.dateperformed AS DATE) AS pd_done_date,
    CAST(pd.pdstatustypedetailid AS VARCHAR(100)) AS pd_status,
    CAST(sh.applicationaccepteddate AS DATE) AS file_acceptance_date,

--   11.  welcome_kit_status -->> needs confirmation on this logic derived
--     CASE
--         WHEN isFullyDisbursed = 1 THEN 'Updated'
--         ELSE 'Pending'
--     END AS welcome_kit_status
--  12 .    ldd.cersaiID as cersai_number

--   13.  no related field  found for this
--     cersai_date

    CAST(da.record_created_at AS TIMESTAMP) AS record_created_at,
    CAST(da.record_modified_at AS TIMESTAMP) AS record_modified_at,
    CURRENT_TIMESTAMP AS silver_loaded_at,
    TO_CHAR(GETDATE(),'YYYYMMDD_HH24MISS') AS silver_batch_id
FROM cte_disbursal_agg da
INNER JOIN cte_disbursal_latest dl
    ON dl.loanapplicationid = da.loanapplicationid
LEFT JOIN cte_pd_deduped pd
    ON pd.loanapplicationid = da.loanapplicationid
LEFT JOIN cte_status_history_deduped sh
    ON sh.loanapplicationid = da.loanapplicationid
LEFT JOIN dmihfclos.tblLoanApplicationDisbursalDetail ldd
    ON  ldd.loanapplicationid = da.loanapplicationid