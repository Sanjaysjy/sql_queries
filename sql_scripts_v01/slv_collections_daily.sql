-- slv_collections_daily
CREATE TABLE silver.slv_collections_daily
DISTKEY(loan_due_status_id)
SORTKEY(loan_application_id, loan_due_status_id)
AS

WITH cte_receipt_agg AS (
    SELECT
        loanapplicationid,

        last_paid_mode,
        coalesce -> pd.statusdate, mr.statusupdateddate -->>  last_paid_date

        dateofcollection,       --change
        COUNT(draftreceiptid) AS receipt_count,         --change
        COALESCE(SUM(CAST(collectedamount AS DECIMAL(18, 2))), 0.00) AS receipt_amount  + pdcachamount, -- change emi due amount and count
--        COALESCE(SUM(CAST(emidueamount   AS DECIMAL(18, 2))), 0.00)  AS emi_due_sum  --  change
    FROM dmihfclos.tblloanapplicationmanualreceipt  mr
    left join tblloanapplicationpresentationdetail  pd
    WHERE mr.isactive = 1 and pd.isactive =1
--     and m_reciept.status_typedetail_id = 219 for manual reciept   and presentation_table.status_type detail_id =2066
    GROUP BY loanapplicationid, mr.statusupdateddate, pd.statusdate  --
), cte_paid_agg AS (
    SELECT
        loanapplicationid,

        last_paid_mode,
        coalesce -> pd.statusdate, mr.statusupdateddate -->>  last_paid_date

    FROM dmihfclos.tblloanapplicationmanualreceipt  mr
    left join tblloanapplicationpresentationdetail  pd
    WHERE mr.isactive = 1 and pd.isactive =1
    GROUP BY loanapplicationid, mr.statusupdateddate, pd.statusdate  --


   ),
 cte_presentation_agg AS (
    SELECT
        cd.loanapplicationid,
        count(if cd.typedetailchargeid = 1, cd.loanapplicationchargedetailid , null ) as bounce_count,
        sum(if cd.typedetailchargeid = 1,  cd.totalcharge,  0 ) as total_bounce_amount     ,

        count(if cd.typedetailchargeid = 2, cd.loanapplicationchargedetailid , null ) as penal_charge_count,
        sum(if cd.typedetailchargeid = 2,  cd.totalcharge,  0 ) as total_penal_charge_amount

        sum(if cd.typedetailchargeid = 1  and cd.knockoffdate = current_date , cd.totalcharge,   0 ) as total_bounce_amount_adjust     ,
        sum(if cd.typedetailchargeid = 2 and cd.knockoffdate = current_date ,  cd.totalcharge,  0 ) as total_penal_charge_amount_adjust     ,

--        COUNT(presentationdetailid) AS presentation_count,  -- need clarification
    FROM silver.slv_loan_details ld
    inner join dmihfclos.tblloanapplicationchargedetail cd
         on  cd.loanapplicationid= ld.loan_application_id
    WHERE cd.isactive = 1  and cd.chargedate is not null and cd.chargedate = CURRENT_DATE -- for bounce
    GROUP BY cd.loanapplicationid
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
), cte_due_amount as (
SELECT
    la.loanapplicationid,
    count(la.duedate)  as cnt_emi_due,
    sum(la.dueamount)   as total_emi_due_amount,
    sum(la.principle)  as principal_over_due,
    sum(la.interst) as interest-over_due
FROM dmihfclos.tblloanapplicationpayschedule la
inner join dmihfclos.tblLoanApplication  tl  on tl.loanapplicationid  =  la.loanapplicationID and tl.isactive =1 and tl.statustypedetailid =189 and la.dueamount > 0
WHERE duedate < CURRENT_DATE
  AND la.isactive = 1
  group by loanapplicationid
  ), cte_npa_date as (
  select *

FROM dmihfclos.tblloanduestatus lds
where lds.isactive=1

)

SELECT
    CAST(lds.loanduestatusid   AS BIGINT) AS loan_due_status_id,
    CAST(lds.loanapplicationid AS BIGINT) AS loan_application_id,
    CAST(lds.transactiondate   AS DATE) AS transaction_date,  -- confirmation from dmi
    CAST(lds.pos AS DECIMAL(18, 2)) AS pos,

    -- verification on --> does emi_due_sum column has these two fields values combined (principal + interest) ??
    CAST(
        COALESCE(CAST(lds.unadjustedamount AS DECIMAL(18, 2)), CAST(0 AS DECIMAL(18, 2)))
        +
        COALESCE(CAST(ra.total_emi_due_amount       AS DECIMAL(18, 2)), CAST(0 AS DECIMAL(18, 2)))    ---
        AS DECIMAL(18, 2)
    ) AS total_demand,

    CAST(lds.maxdeliquencyday  AS INT) AS max_dpd,
    lds.deliquencybktdetailid  AS dpd_bucket,
    case when   lds.linkednpaloanid is not null  and  trim(lds.linkednpaloanid) <> '' OR (lds.isnpa is not null and lds.isnpa =1) THEN TRUE  ELSE  FALSE AS is_npa,
    coalesce(lds.npastartdate , lds1.npastartdate )    AS npa_start_date,
    COALESCE(CAST(ra.receipt_count AS INT), CAST(0 AS INT)) AS daily_receipt_count,
    COALESCE(CAST(ra.receipt_amount AS DECIMAL(18, 2)), CAST(0 AS DECIMAL(18, 2))) AS daily_receipt_amount,
--    COALESCE(CAST(pa.presentation_count  AS INT), CAST(0 AS INT)) AS daily_presentations,
    COALESCE(CAST(pa.bounce_count        AS INT), CAST(0 AS INT)) AS daily_bounces,
    cf.followuptypedetailid AS followup_type,
--      lookup for this --typeID description
--    TRIM(lds.sourcefundingname) AS source_funding_name,  --- need to check
    CAST(lds.createdon AS TIMESTAMP) AS record_created_at ,
    CAST(lds.lastmodifiedon AS TIMESTAMP) AS record_modified_a,
    CURRENT_TIMESTAMP AS silver_loaded_at,
    TO_CHAR(GETDATE(),'YYYYMMDD_HH24MISS') AS silver_batch_id
--    is_employee_loan  silver. loan details
--- income_considered  -- customer tbl silver 


FROM cte_npa_date lds
--dmihfclos.tblloanduestatus lds
LEFT JOIN cte_receipt_agg ra
    ON  ra.loanapplicationid = lds.loanapplicationid
    AND ra.dateofcollection  = lds.transactiondate
LEFT JOIN cte_presentation_agg pa
    ON  pa.loanapplicationid = lds.loanapplicationid
    AND pa.presentationdate  = lds.transactiondate
LEFT JOIN cte_followup_latest cf
    ON  cf.loanapplicationid = lds.loanapplicationid
left join cte_due_amount da
    ON da.loanapplicationid  = lds.loanapplicationid
left join cte_npa_date lds1
    ON lds.linkednpaloanid  = lds.loanapplicationid
WHERE lds.isactive = 1


--- early vintage
-- ach status