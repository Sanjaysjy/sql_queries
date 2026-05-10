-- silver.slv_early_warning_risk

-- historical retain of 18 months
-- Run the CREATE block only once during initial setup.

CREATE TABLE IF NOT EXISTS silver.slv_early_warning
(
    ews_id                        BIGINT,
    loan_application_id           VARCHAR(50),
    signal_date                   DATE,
    snapshot_type                 VARCHAR(20),    -- ADDED: 'MONTH_END' or 'CURRENT_DAY'
    snapshot_month                DATE,           -- ADDED: LAST_DAY of the signal_date month
    dpd_trend_direction           VARCHAR(20),
    negative_area_flag            BOOLEAN,
    multi_loan_stress             BOOLEAN,
    high_foir_flag                BOOLEAN,
    non_starter_sma1_flag         BOOLEAN,
    active_sma2_flag              BOOLEAN,
    ach_not_registered_sma1_flag  BOOLEAN,
    construction_delay_sma1_flag  BOOLEAN,
    title_doc_delay_flag          BOOLEAN,
    bounce_fy_sma1_flag           BOOLEAN,
    ews_risk_score                INT,
    ews_risk_grade                VARCHAR(20),
    recommended_action            VARCHAR(500),
    ews_indicators                VARCHAR(1000),
    previous_month_ews_score      INT,           -- ADDED
    score_variance                INT,           -- ADDED
    score_trend                   VARCHAR(20),   -- ADDED
    gold_loaded_at                TIMESTAMP,
    gold_batch_id                 VARCHAR(20),
    reporting_period              DATE
)
DISTKEY(loan_application_id)
SORTKEY(loan_application_id, signal_date);


INSERT INTO silver.slv_early_warning

--
--total  dpd by current day and  total dpd by  previous mont if
--       both are =  0 then current   or stable
--       both are = previous month total   THEN  ->  Improved
--        m2  <  m1    then  Worsning
--        m1 >0   and  m2 =0  -> worsning
--        m2 >0  and   m1 =0  -> improved




WITH  dpd_ranked AS (
    SELECT
        loan_application_id,
        max_dpd,
        ROW_NUMBER() OVER (
            PARTITION BY loan_application_id
            ORDER BY snapshot_date DESC
        ) AS rn
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
            WHEN dpd_m1 = 0 AND dpd_m2 = 0 AND dpd_m3 = 0
                THEN 'Stable'

            WHEN dpd_m1 = 0 AND (dpd_m2 > 0 OR dpd_m3 > 0)
                THEN 'Improving'

            WHEN dpd_m1 > 0 AND dpd_m2 = 0 AND dpd_m3 = 0
                THEN 'Worsening'

            WHEN dpd_m1 > dpd_m2 AND dpd_m2 > dpd_m3
                THEN 'Worsening'

            WHEN dpd_m1 < dpd_m2 AND dpd_m2 < dpd_m3
                THEN 'Improving'

            WHEN dpd_m1 = dpd_m2 AND dpd_m2 = dpd_m3 AND dpd_m1 >= 91
                THEN 'Stable'
            WHEN dpd_m1 = dpd_m2 AND dpd_m2 = dpd_m3 AND dpd_m1 < 91
                THEN 'Improving'
            ELSE 'Stable'

        END AS dpd_trend_direction
    FROM dpd_pivot
),
customer_ranked AS (
    SELECT
        loan_application_id,
        entity_id,
        parent_applicant_id ,
        ROW_NUMBER() OVER ( PARTITION BY loan_application_id
                ORDER BY CASE WHEN is_main_applicant = 'true'  THEN 0 ELSE 1  END,record_modified_at DESC) AS rn
    FROM silver.slv_customer
),
customer_dedup AS (
    SELECT loan_application_id, entity_id, parent_applicant_id
    FROM customer_ranked
    WHERE rn = 1
),


entity_dpd AS (
    SELECT
        count(cd1.loan_application_id)  AS cnt_loan_application_id,
        cd1.loan_application_id,
        ta.parent_applicant_id,
        MAX(cpm.max_dpd) AS max_entity_dpd
    FROM customer_dedup cd1
    left join silver.slv_customer ta
        on ta.loan_application_id = cd1.loan_application_id
    JOIN silver.slv_contract_perf_monthly cpm
        ON cd1.loan_application_id = cpm.loan_application_id
    GROUP BY ta.parent_applicant_id, cd1.loan_application_id
),
cte_multi_loan_stress AS (
    SELECT
        loan_application_id,
        CASE WHEN max_entity_dpd > 0 THEN TRUE
         AND cnt_loan_application_id > 0 ELSE FALSE END AS multi_loan_stress
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

-- Non-Starter: first 2 dues unpaid + no clearance + SMA-1 or above
cte_non_starter_first AS (
        SELECT
            la.loanapplicationid,
            la.duedate,
            la.paymentreceiveddate,
            la.clearenceflag,
            ROW_NUMBER() OVER (
                PARTITION BY la.loanapplicationid
                ORDER BY la.duedate  ASC
            ) AS rn
        FROM dmihfclos.tblloanapplicationpayschedule la
        INNER JOIN dmihfclos.tblloanapplication tl
            ON  tl.loanapplicationid    = la.loanapplicationid
            AND tl.isactive             = 1
            AND tl.statustypedetailid   = 189       -- active loan
            AND la.dueamount            > 0
        WHERE la.duedate  < CURRENT_DATE
          AND la.isactive = 1
    ),

cte_non_starter_loans_second AS (
        SELECT
            loanapplicationid
        FROM cte_non_starter_first
        WHERE rn <= 2                                           -- first 2 instalments only
          AND paymentreceiveddate IS NULL                       -- no payment received
          AND (clearenceflag IS NULL OR clearenceflag = 0)     -- no clearance
        GROUP BY loanapplicationid
        HAVING COUNT(duedate) = 2                              -- BOTH first 2 dues must be unpaid
    ),

-- [NEW — Signal 1]--   corrected - need to verify
cte_non_starter AS (

    SELECT
        ns.loanapplicationid  as loan_application_id,
        TRUE AS non_starter_sma1_flag
    FROM cte_non_starter_loans_second ns
    INNER JOIN dmihfclos.tblloanmonthly lds              -- SMA / delinquency bucket check
        ON  lds.loanapplicationid       = ns.loanapplicationid
        AND lds.isactive                = 1
        AND lds.deliquencybktdetailid   IN (2570, 2571, 2572)  -- SMA-1, SMA-2, NPA
    GROUP BY ns.loanapplicationid
),
-- [NEW — Signal 2]-- done
cte_active_sma2 AS (
    SELECT
        la.loan_application_id,
        TRUE AS active_sma2_flag
    FROM silver.slv_loan_details    la
    JOIN dmihfclos.tblloanmonthly lds
        ON la.loan_application_id = lds.loanapplicationid   and lds.isactive =1
    WHERE la.loanstatus_typedetail_id= 189          -- active loan
      AND lds.deliquencybktdetailid = 2571    -- SMA-2
    GROUP BY la.loan_application_id
),

-- [NEW — Signal 3]
cte_ach_not_registered AS (
    SELECT
        la.loan_application_id,
        TRUE AS ach_not_registered_sma1_flag
    FROM silver.slv_loan_details    la
    JOIN dmihfclos.tblloanmonthly  lds
        ON la.loan_application_id = lds.loanapplicationid  and lds.isactive=1
    left JOIN  dmihfclos.tblLoanApplicationACH    ach
        ON la.loan_application_id = ach.loanapplicationid

    WHERE la.loanstatus_typedetail_id= 189
        AND (
           ach.registered IS NULL
        OR ach.registered = 0
        )      AND (ach.registeredstatustypedetailid IS NULL
           OR ach.registeredstatustypedetailid <> 1991)  and ach.isactive =1
      AND lds.deliquencybktdetailid IN (2570, 2571, 2572)  -- SMA-1, SMA-2, NPA
    GROUP BY la.loan_application_id
),

-- [NEW — Signal 4] -- done
cte_construction_delay AS (
    SELECT
        la.loan_application_id,
        TRUE AS construction_delay_sma1_flag
    FROM silver.slv_loan_details    la
    JOIN dmihfclos.tblloanmonthly lds
        ON la.loan_application_id = lds.loanapplicationid  and lds.isactive =1
    WHERE la.loanstatus_typedetail_id= 189
       AND la.loan_purpose_id IN (16, 1, 5  )    --('PCBUILDER', 'RENOVATION', 'PLOTCONSTRUCTION')

      AND la.first_disbursal_date <= ADD_MONTHS(CURRENT_DATE, -18)
       AND ( la.is_Actually_Fully_Disbursed = 0 or  la.is_Actually_Fully_Disbursed is null )
    --   AND total_disbursed_amount = first_disbursed_amount
      AND lds.deliquencybktdetailid = 2570    -- SMA-1
    GROUP BY la.loan_application_id
),

-- [NEW — Signal 5]  --done
cte_title_doc_delay AS (
    SELECT
        la.loan_application_id,
        TRUE AS title_doc_delay_flag
    FROM silver.slv_loan_details       la
    JOIN dmihfclos.tblLoanLegalDocuments    lld
        ON la.loan_application_id = lld.loanapplicationid
    WHERE la.loanstatus_typedetail_id= 189
      AND lld.documentid IN (1, 3, 16, 70, 81, 118, 130, 209)
      AND lld.documentcategorydetailid = 3256
      AND lld.documenttypedetailid     = 1957
      AND (lld.documentstatustypedetailid IS NULL
           OR lld.documentstatustypedetailid NOT IN (1948, 1949, 1951))

      AND la.first_disbursal_date <= ADD_MONTHS(CURRENT_DATE, -3)
    GROUP BY la.loan_application_id
),

-- [NEW — Signal 6] -- done
cte_bounce_fy AS (
    SELECT
        la.loan_application_id,
        TRUE AS bounce_fy_sma1_flag
    FROM silver.slv_loan_details          la
    JOIN dmihfclos.tblLoanApplicationChargeDetails   lcd
        ON la.loan_application_id = lcd.loanapplicationid
    JOIN dmihfclos.tblloanduestatus       lds
        ON la.loan_application_id = lds.loanapplicationid
    WHERE la.loanstatus_typedetail_id      = 189
      AND lcd.chargesfortypedetailid = 1561   -- bounce charge
      AND lcd.typedetailchargeid      = 1

      AND lcd.chargedate >= CASE
              WHEN EXTRACT(MONTH FROM CURRENT_DATE) >= 4
              THEN DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '3 months'
              ELSE DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '9 months'
          END
      AND lds.deliquencybktdetailid = 2570    -- SMA-1
    GROUP BY la.loan_application_id
    HAVING COUNT(lcd.chargedate) > 3
),

cte_base AS (
    SELECT
        ld.loan_application_id,
        CURRENT_DATE AS signal_date,
        -- ADDED
        CASE
            WHEN CURRENT_DATE = LAST_DAY(CURRENT_DATE)
                 THEN 'MONTH_END'
            ELSE 'CURRENT_DAY'
        END AS snapshot_type,
        -- ADDED
        LAST_DAY(CURRENT_DATE) AS snapshot_month,

        -- consecutive_bounces: source column unavailable
        -- COALESCE(cb.consecutive_bounces, 0)  AS consecutive_bounces,

        --  DPD trend (Worsening / Stable / Improving)
        dt.dpd_trend_direction,

        --  emi_increase_stress: current_emi not in DDL
        --  ltv_breach_flag: currentLTV/ltvNorm not in DDL

        -- Collateral & customer signals
        COALESCE(ast.negative_area_flag, FALSE) AS negative_area_flag,
        COALESCE(mls.multi_loan_stress, FALSE)  AS multi_loan_stress,

        CASE WHEN ld.final_foir_pct > 65
             THEN TRUE ELSE FALSE END AS high_foir_flag,

        -- [NEW — Signal 1] Non-starter & SMA-1+
        COALESCE(ns.non_starter_sma1_flag, FALSE) AS non_starter_sma1_flag,

        -- [NEW — Signal 2] Active loan & SMA-2
        COALESCE(as2.active_sma2_flag, FALSE)    AS active_sma2_flag,

        -- [NEW — Signal 3] ACH not registered & SMA-1+
        COALESCE(ach.ach_not_registered_sma1_flag,FALSE) AS ach_not_registered_sma1_flag,

        -- [NEW — Signal 4] Construction delay 18 months & SMA-1
        COALESCE(cd.construction_delay_sma1_flag, FALSE) AS construction_delay_sma1_flag,

        -- [NEW — Signal 5] Title document delay > 3 months
        COALESCE(tdd.title_doc_delay_flag, FALSE) AS title_doc_delay_flag,

        -- [NEW — Signal 6] 3+ bounces in current FY & SMA-1
        COALESCE(bfy.bounce_fy_sma1_flag, FALSE)    AS bounce_fy_sma1_flag

    FROM silver.slv_loan_details ld
    -- LEFT JOIN cte_consecutive_bounces  cb  ON ld.loan_application_id = cb.loan_application_id
    LEFT JOIN cte_dpd_trend dt  ON ld.loan_application_id = dt.loan_application_id
    LEFT JOIN cte_multi_loan_stress mls ON ld.loan_application_id = mls.loan_application_id
    LEFT JOIN cte_asset ast ON ld.loan_application_id = ast.loan_application_id

    LEFT JOIN cte_non_starter ns  ON ld.loan_application_id = ns.loan_application_id
    LEFT JOIN cte_active_sma2 as2 ON ld.loan_application_id = as2.loan_application_id
    LEFT JOIN cte_ach_not_registered     ach ON ld.loan_application_id = ach.loan_application_id
    LEFT JOIN cte_construction_delay     cd  ON ld.loan_application_id = cd.loan_application_id
    LEFT JOIN cte_title_doc_delay tdd ON ld.loan_application_id = tdd.loan_application_id
    LEFT JOIN cte_bounce_fy  bfy ON ld.loan_application_id = bfy.loan_application_id
),

cte_scored AS (
    SELECT
        loan_application_id,
        signal_date,

        snapshot_type,    -- ADDED

        snapshot_month,   -- ADDED

        -- Signal columns
        -- consecutive_bounces,
        dpd_trend_direction,
        -- emi_increase_stress: column current_emi not present in silver.slv_loan_details DDL
        -- ltv_breach_flag: columns currentLTV and ltvNorm not present in silver.slv_contract_perf_monthly DDL
        negative_area_flag,
        multi_loan_stress,
        high_foir_flag,

        non_starter_sma1_flag,
        active_sma2_flag,
        ach_not_registered_sma1_flag,
        construction_delay_sma1_flag,
        title_doc_delay_flag,
        bounce_fy_sma1_flag,

        -- Composite EWS risk score (capped at 100)
        LEAST(
            (
                -- Bounce score (source unavailable)
                -- CASE
                --     WHEN consecutive_bounces >= 3 THEN 15
                --     WHEN consecutive_bounces = 2  THEN 10
                --     WHEN consecutive_bounces = 1  THEN  5
                --     ELSE 0
                -- END

                -- DPD trend score
                CASE
                    WHEN dpd_trend_direction = 'Worsening' THEN 20
                    WHEN dpd_trend_direction = 'Stable'    THEN 10
                    ELSE 0
                END
                + CASE WHEN high_foir_flag = TRUE THEN 10 ELSE 0 END
                + CASE WHEN multi_loan_stress = TRUE THEN 10 ELSE 0 END
                -- [NEW — Signal 1] Non-starter & SMA-1+
                + CASE WHEN non_starter_sma1_flag = TRUE THEN 15 ELSE 0 END
                -- [NEW — Signal 2] Active loan & SMA-2
                + CASE WHEN active_sma2_flag = TRUE THEN 15 ELSE 0 END
                -- [NEW — Signal 3] ACH not registered & SMA-1+
                + CASE WHEN ach_not_registered_sma1_flag  = TRUE THEN 10 ELSE 0 END
                -- [NEW — Signal 4] Construction delay 18 months & SMA-1
                + CASE WHEN construction_delay_sma1_flag  = TRUE THEN 10 ELSE 0 END
                -- [NEW — Signal 5] Title document delay > 3 months
                + CASE WHEN title_doc_delay_flag = TRUE THEN  5 ELSE 0 END
                -- [NEW — Signal 6] 3+ bounces in current FY & SMA-1
                + CASE WHEN bounce_fy_sma1_flag = TRUE THEN 15 ELSE 0 END
            ),
            100
        ) AS ews_risk_score,

        -- Derived indicator summary string (ews_indicators column per mapping)
        TRIM(
            CASE WHEN non_starter_sma1_flag = TRUE THEN 'NON_STARTER; & SMA1 OR ABOVE' ELSE '' END
            || CASE WHEN active_sma2_flag = TRUE THEN 'ACTIVE & ACTIVE_SMA2;' ELSE '' END
            || CASE WHEN ach_not_registered_sma1_flag= TRUE THEN 'ACH_NOT_REGISTERED; 7 SMA1 AND ABOVE '  ELSE '' END
            || CASE WHEN construction_delay_sma1_flag= TRUE THEN 'CONSTRUCTION_DELAY_18M; & SMA1'   ELSE '' END
            || CASE WHEN title_doc_delay_flag = TRUE THEN 'TITLE_DOC_DELAY_3M; $ DELAY FOR MORE THAN = 3 MONTHS' ELSE '' END
            || CASE WHEN bounce_fy_sma1_flag = TRUE THEN 'BOUNCE_FY_3PLUS;  &  SMA1 ' ELSE '' END
            || CASE WHEN high_foir_flag = TRUE THEN 'HIGH_FOIR;' ELSE '' END
            || CASE WHEN multi_loan_stress = TRUE THEN 'MULTI_LOAN_STRESS;' ELSE '' END
            || CASE WHEN dpd_trend_direction = 'Worsening' THEN 'DPD_WORSENING;' ELSE '' END
        ) AS ews_indicators

    FROM cte_base
),

cte_prev_score AS (
    SELECT
        loan_application_id,
        ews_risk_score AS previous_month_ews_score
    FROM (
        SELECT
            loan_application_id,
            ews_risk_score,
            ROW_NUMBER() OVER (
                PARTITION BY loan_application_id
                ORDER BY signal_date DESC
            ) AS rn
        FROM silver.slv_early_warning
        WHERE snapshot_type = 'MONTH_END'
    ) t
    WHERE rn = 1
)

SELECT
    ROW_NUMBER() OVER (ORDER BY src.loan_application_id) AS ews_id,
    src.loan_application_id,
    src.signal_date,

    src.snapshot_type,    -- ADDED

    src.snapshot_month,   -- ADDED

    -- Repayment signals
    -- consecutive_bounces, --- more than 3 bounce    --  source column unavailable in repayment slv table add and pull here -- create a cte for this  count(((  -- tbl oan appl pay schedule-.. pay schedule id ,, mapp it with --> tbl loan appli charge details ,,, pay schedu id column ,, with a condition --> typedetails charge _id =1  and charge for typew detail id =1561  and is active =1
    src.dpd_trend_direction, -- Worsening/Stable/Improving

    -- emi_increase_stress,    -- current_emi not in slv_loan_details DDL
    -- ltv_breach_flag,        --  currentLTV/ltvNorm not in source DDL  --> pos / coal(latest value , market value)  decimal  .2    -->> case for this on condition

    -- Collateral signals
    src.negative_area_flag, -- Property in negative area

    -- Customer signals
    src.multi_loan_stress, -- Multiple loans with DPD
    src.high_foir_flag, -- FOIR > 65%

    -- New signals (v2)
    src.non_starter_sma1_flag, -- Signal 1
    src.active_sma2_flag, -- Signal 2
    src.ach_not_registered_sma1_flag, -- Signal 3
    src.construction_delay_sma1_flag, -- Signal 4
    src.title_doc_delay_flag, -- Signal 5
    src.bounce_fy_sma1_flag, -- Signal 6

    -- Composite
    src.ews_risk_score, -- 0–100 weighted
    CASE
        WHEN src.ews_risk_score BETWEEN  0 AND 25  THEN 'Green'
        WHEN src.ews_risk_score BETWEEN 26 AND 50  THEN 'Amber'
        WHEN src.ews_risk_score BETWEEN 51 AND 75  THEN 'Red'
        WHEN src.ews_risk_score BETWEEN 76 AND 100 THEN 'Critical'
    END AS ews_risk_grade, -- Green/Amber/Red/Critical

    CASE
        -- Critical tier
        WHEN src.ews_risk_score BETWEEN 76 AND 100 AND src.negative_area_flag = TRUE
            THEN 'Immediate field visit required. Initiate legal notice and NPA provisioning review. Flag for collateral re-valuation.'
        WHEN src.ews_risk_score BETWEEN 76 AND 100
            THEN 'Immediate field visit required. Initiate legal notice and NPA provisioning review.'
        -- Red tier
        WHEN src.ews_risk_score BETWEEN 51 AND 75 AND src.negative_area_flag = TRUE
            THEN 'Telephonic follow-up. Evaluate OTS or restructuring options. Flag for collateral re-valuation.'
        WHEN src.ews_risk_score BETWEEN 51 AND 75
            THEN 'Telephonic follow-up. Evaluate OTS or restructuring options.'
        -- Amber tier
        WHEN src.ews_risk_score BETWEEN 26 AND 50 AND src.negative_area_flag = TRUE
            THEN 'Monitor closely. Send payment reminder communication. Flag for collateral re-valuation.'
        WHEN src.ews_risk_score BETWEEN 26 AND 50
            THEN 'Monitor closely. Send payment reminder communication.'
        -- Green tier
        WHEN src.negative_area_flag = TRUE
            THEN 'No action required. Standard monitoring applies. Flag for collateral re-valuation.'
        ELSE 'No action required. Standard monitoring applies.'
    END AS recommended_action,
    src.ews_indicators,

    ps.previous_month_ews_score,
    src.ews_risk_score - ps.previous_month_ews_score  AS score_variance,
    CASE
        WHEN (src.ews_risk_score - ps.previous_month_ews_score) > 0  THEN 'WORSENING'
        WHEN (src.ews_risk_score - ps.previous_month_ews_score) < 0  THEN 'IMPROVING'
        WHEN ps.previous_month_ews_score IS NULL                      THEN 'NEW'
        ELSE 'STABLE'
    END AS score_trend,

    -- Audit columns
    SYSDATE AS gold_loaded_at,
    TO_CHAR(SYSDATE, 'YYYYMMDD_HH24MISS') AS gold_batch_id,
    CASE
        WHEN CURRENT_DATE = LAST_DAY(CURRENT_DATE) THEN CURRENT_DATE
        ELSE LAST_DAY(CURRENT_DATE)
    END AS reporting_period

FROM cte_scored src
LEFT JOIN cte_prev_score ps
    ON src.loan_application_id = ps.loan_application_id
-- removes dedup of rows in table
WHERE NOT EXISTS (
    SELECT 1
    FROM silver.slv_early_warning tgt
    WHERE tgt.loan_application_id = src.loan_application_id
      AND tgt.signal_date         = src.signal_date
      AND tgt.snapshot_type       = src.snapshot_type
);

--  to remove the months other date rows except current date
--DELETE FROM silver.slv_early_warning
--WHERE snapshot_type = 'CURRENT_DAY'
--  AND signal_date < (
--      SELECT MAX(signal_date)
--      FROM silver.slv_early_warning
--      WHERE snapshot_type = 'CURRENT_DAY'
--  );

-- to remove row  before 18 months olds
DELETE FROM silver.slv_early_warning
WHERE signal_date < ADD_MONTHS(CURRENT_DATE, -18);


/*   Report as of today  and month End.(history)
1.Non-starter and (SMA-1 or above)   : tblLoanApplication statusTypeDetailID =189,tblLoanApplicationPaySchedule ,paymentReceivedDate is null and  clearenceFlag is null or 0 or false, tblLoanDueStatus deliquencyBktDetailID    in 2570,2571,2572
2.Active Loan Account and SMA-2  :   tblLoanApplication statusTypeDetailID =189,tblLoanDueStatus deliquencyBktDetailID =2571
3.ACH not registered & (SMA-1 or above)   : tblLoanApplication statusTypeDetailID =189, tblLoanApplicationACH where registered is null or false and registeredStatusTypeDetailId is null or not equal to 1991  ,tblLoanDueStatus deliquencyBktDetailID  in 2570,2571,2572
4.Construction not started even after 18 months and SMA-1 : tblLoanApplication statusTypeDetailID =189,  only first tranch is disbursed 18 month before the current date and  loan is not fully disbursed yet and loanPurposeCode in ('PCBUILDER', 'RENOVATION','PLOTCONSTRUCTION' ) ,tblLoanDueStatus deliquencyBktDetailID =2570
5. Title document & delay of more than 3 months : tblLoanApplication statusTypeDetailID = 189,tblLoanLegalDocuments where documentId in (1,3,16,70,81,118,130,209) and documentCategoryDetailId in (3256)  and  documentTypeDetailId in(1957) and documentStatusTypeDetailId  IS NULL OR documentStatusTypeDetailId not in (1948,1949,1951) and first disbursement date  is more than 3 months
6.More than 3 bounce in current financial year & SMA-1  :  tblLoanApplication statusTypeDetailID =189, tblLoanApplicationChargeDetails where chargesForTypeDetailID =1561 and typeDetailChargeID =1 and chargeDate,tblLoanDueStatus deliquencyBktDetailID =2570

*/