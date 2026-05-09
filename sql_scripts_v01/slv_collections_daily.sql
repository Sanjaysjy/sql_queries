-- slv_collections_daily
Drop table if exists silver.slv_collections_daily;

CREATE TABLE silver.slv_collections_daily
DISTKEY(loan_due_status_id)
SORTKEY(loan_application_id, loan_due_status_id)
AS

WITH mr_agg AS (
    SELECT
        loanapplicationid,
        MAX(collectedthroughtypedetailid) AS last_paid_mode_id,
        MAX(statusupdateddate) AS last_paid_date,
        MAX(collectedamount) AS last_collected_amount,
        COUNT(DISTINCT draftreceiptid) AS receipt_count,
        COALESCE( SUM(CAST(COALESCE(collectedamount, 0) AS DECIMAL(18, 2))), 0.00) AS mr_total_receipt_amount

        FROM dmihfclos.tblloanapplicationmanualreceipt
        WHERE isactive = 1
          AND statustypedetailid = 219
        GROUP BY loanapplicationid
),

pd_agg AS (
    SELECT
        loanapplicationid,
        MAX(statusdate) AS pd_last_paid_date,
        MAX(pdcachamount) AS pd_last_collected_amount,
        COALESCE(  SUM(CAST(COALESCE(pdcachamount, 0) AS DECIMAL(18, 2))), 0.00) AS pd_total_receipt_amount

        FROM dmihfclos.tblloanapplicationpresentationdetail
        WHERE isactive = 1
          AND statustypedetailid = 2066
        GROUP BY loanapplicationid
),

cte_receipt_agg AS (
    SELECT
      mr.loanapplicationid,
      mr.last_paid_mode_id,   -- need confirmation is there no collection mode type in pd ??
      td_mode.typeDetailDescription AS last_paid_mode,
      GREATEST(mr.last_paid_date, pd.pd_last_paid_date) AS last_paid_date,
      COALESCE(pd.pd_last_collected_amount, mr.last_collected_amount) AS last_collected_amount,
      mr.receipt_count,
      COALESCE(mr.mr_total_receipt_amount, 0.00) + COALESCE(pd.pd_total_receipt_amount, 0.00) AS total_receipt_amount
    FROM
      mr_agg mr
    LEFT JOIN pd_agg pd
        ON mr.loanapplicationid = pd.loanapplicationid
    LEFT JOIN dmihfclos.tblTypeDetail td_mode
        ON  td_mode.typeDetailID = mr.last_paid_mode_id



--    SELECT
--        mr.loanapplicationid,
--
--        COALESCE( mr.collectedthroughtypedetailid) AS last_paid_mode_id,  -- need confirmation is there no collection mode type in pd ??
--        COALESCE( mr.collectedthroughtypedetailid) AS last_paid_mode,
--
--        COALESCE(pd.statusdate, mr.statusupdateddate) AS last_paid_date,
--        COALESCE(pd.pdcachamount, mr.collectedamount) AS last_collected_amount,
--
--            COUNT(mr.draftreceiptid) AS receipt_count,
--        COALESCE(SUM(CAST(mr.collectedamount AS DECIMAL(18,2))+ CAST(pd.pdcachamount AS DECIMAL(18,2))),0.00) AS total_receipt_amount
--
----        COALESCE(SUM(CAST(emidueamount   AS DECIMAL(18, 2))), 0.00)  AS emi_due_sum  --  change
--    FROM dmihfclos.tblloanapplicationmanualreceipt  mr
--    left join dmihfclos.tblloanapplicationpresentationdetail  pd
--         on  mr.loanapplicationid= pd.loanapplicationid
--    WHERE mr.isactive = 1 and pd.isactive =1
--     and mr.statustypedetailid = 219    and pd.statustypedetailid =2066
--    GROUP BY mr.loanapplicationid, mr.statusupdateddate, pd.statusdate
),
--
--cte_paid_agg AS (
--    SELECT
--        loanapplicationid,
--
--        last_paid_mode,
--        coalesce -> pd.statusdate, mr.statusupdateddate -->>  last_paid_date
--
--    FROM dmihfclos.tblloanapplicationmanualreceipt  mr
--    left join tblloanapplicationpresentationdetail  pd
--    WHERE mr.isactive = 1 and pd.isactive =1
--    GROUP BY loanapplicationid, mr.statusupdateddate, pd.statusdate  --
--   ),

cte_presentation_agg AS (
    SELECT
        cd.loanapplicationid,
        COUNT(CASE WHEN cd.typedetailchargeid = 1 THEN cd.loanapplicationchargedetailid END) AS bounce_count,
        SUM(CASE  WHEN cd.typedetailchargeid = 1 THEN cd.totalcharge ELSE 0 END) AS total_bounce_amount  ,

        COUNT(CASE WHEN cd.typedetailchargeid = 2 THEN cd.loanapplicationchargedetailid END) AS penal_charge_count,
        SUM(CASE WHEN cd.typedetailchargeid = 2 THEN cd.totalcharge ELSE 0 END) AS total_penal_charge_amount,

        SUM(CASE WHEN cd.typedetailchargeid = 1 AND cd.knockoffdate = CURRENT_DATE THEN cd.totalcharge ELSE 0 END) AS total_bounce_amount_adjust,
        SUM(CASE WHEN cd.typedetailchargeid = 2 AND cd.knockoffdate = CURRENT_DATE THEN cd.totalcharge ELSE 0 END) AS total_penal_charge_amount_adjust

--        COUNT(presentationdetailid) AS presentation_count,  -- need clarification
    FROM silver.slv_loan_details ld
    inner join dmihfclos.tblloanapplicationchargedetails cd
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
    sum(la.principal)  as principal_over_due,
    sum(la.interst) as interest_over_due
FROM dmihfclos.tblloanapplicationpayschedule la
inner join dmihfclos.tblLoanApplication  tl  on tl.loanapplicationid  =  la.loanapplicationID and tl.isactive =1 and tl.statustypedetailid =189 and la.dueamount > 0
WHERE la.duedate < CURRENT_DATE
  AND la.isactive = 1
  group by la.loanapplicationid
  ),

cte_last_sanction as (

SELECT uw.loanapplicationid, e.employeename || '_'  || e.employeecode  as last_sanction_by
FROM dmihfclos.tblloanapplicationunderwriting  uw
inner join dmihfclos.tbluser u on u.userid = uw.createdby
inner join dmihfclos.tblemployee e on e.entityid = u.entityid

WHERE uw.underwritingstatustypedetailid = 237
QUALIFY ROW_NUMBER() OVER ( PARTITION BY uw.loanapplicationid ORDER BY uw.lastmodifiedon DESC) = 1

),
cte_valuevation_dsv   as(

    SELECT
        tdv.loanapplicationid,
        valuationdistresssalevalue   as    valuation_distress_sale_value
    FROM dmihfclos.tblloantechnicaldiligencevaluation tdv
    left join dmihfclos.tblloanduestatus lds
        ON tdv.loanapplicationid  = lds.loanapplicationid

),

cte_npa_date as (
  select
      *
FROM dmihfclos.tblloanduestatus lds
where lds.isactive=1

)

SELECT
    CAST(lds.loanduestatusid   AS BIGINT) AS loan_due_status_id,
    CAST(lds.loanapplicationid AS BIGINT) AS loan_application_id,
    CAST(lds.transactiondate   AS DATE) AS transaction_date,  -- confirmation from dmi
    CAST(lds.pos AS DECIMAL(18, 2)) AS pos,

    CAST(
        COALESCE(CAST(lds.unadjustedamount AS DECIMAL(18, 2)), CAST(0 AS DECIMAL(18, 2)))
        +
        COALESCE(CAST(da.total_emi_due_amount       AS DECIMAL(18, 2)), CAST(0 AS DECIMAL(18, 2)))    ---
        AS DECIMAL(18, 2)
    ) AS total_demand,
    da.cnt_emi_due,
    da.total_emi_due_amount,
    da.principal_over_due,
    da.interest_over_due,

    CAST(lds.maxdeliquencyday  AS INT) AS max_dpd,
    lds.deliquencybktdetailid  AS dpd_bucket,
    CASE
        WHEN (lds.linkednpaloanid IS NOT NULL AND TRIM(lds.linkednpaloanid) <> '') OR (lds.isnpa IS NOT NULL AND lds.isnpa = 1)
        THEN TRUE ELSE FALSE
    END AS is_npa,
    COALESCE(lds.npastartdate , lds1.npastartdate )    AS npa_start_date,
    COALESCE(CAST(ra.receipt_count AS INT), CAST(0 AS INT)) AS daily_receipt_count,
    -- COALESCE(CAST(ra.receipt_amount AS DECIMAL(18, 2)), CAST(0 AS DECIMAL(18, 2))) AS daily_receipt_amount,
    ra.last_paid_mode_id,
    ra.last_paid_mode,
    ra.last_paid_date,
    ra.last_collected_amount,
    ra.total_receipt_amount,


    pa.total_bounce_amount,
    pa.penal_charge_count,
    pa.total_penal_charge_amount,
    pa.total_bounce_amount_adjust,
    pa.total_penal_charge_amount_adjust,

    vdv.valuation_distress_sale_value,
--    COALESCE(CAST(pa.presentation_count  AS INT), CAST(0 AS INT)) AS daily_presentations,
    COALESCE(CAST(pa.bounce_count        AS INT), CAST(0 AS INT)) AS daily_bounces,
    cf.followuptypedetailid AS followup_type_id,
    tb_follow.typeDetailDescription  AS followup_type,

    sld.is_employee_loan  as is_employee_loan,
    sc.income_considered  as income_considered,
--    TRIM(lds.sourcefundingname) AS source_funding_name,  --- need to check

    ls.last_sanction_by,
    CAST(lds.createdon AS TIMESTAMP) AS record_created_at ,
    CAST(lds.lastmodifiedon AS TIMESTAMP) AS record_modified_a,
    CURRENT_TIMESTAMP AS silver_loaded_at,
    TO_CHAR(GETDATE(),'YYYYMMDD_HH24MISS') AS silver_batch_id



FROM cte_npa_date lds
LEFT JOIN cte_receipt_agg ra
    ON  ra.loanapplicationid = lds.loanapplicationid
LEFT JOIN cte_presentation_agg pa
    ON  pa.loanapplicationid = lds.loanapplicationid
LEFT JOIN cte_valuevation_dsv vdv
    ON  vdv.loanapplicationid = lds.loanapplicationid
LEFT JOIN cte_followup_latest cf
    ON  cf.loanapplicationid = lds.loanapplicationid
left join cte_due_amount da
    ON da.loanapplicationid  = lds.loanapplicationid
left join cte_npa_date lds1
    ON lds.linkednpaloanid  = lds.loanapplicationid
left join cte_last_sanction ls
    ON ls.loanapplicationid  = lds.loanapplicationid
left join silver.slv_loan_details sld
    ON lds.loanapplicationid  = sld.loan_application_id
left join silver.slv_customer sc
    ON lds.loanapplicationid  = sc.loan_application_id

LEFT JOIN dmihfclos.tblTypeDetail tb_follow
    ON  tb_follow.typeDetailID = cf.followuptypedetailid

WHERE lds.isactive = 1

--- table confirmation needed
--- early vintage
-- ach status