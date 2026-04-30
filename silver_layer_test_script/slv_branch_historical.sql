-- silver.slv_branch_historical

DROP TABLE IF EXISTS silver.slv_branch_historical;

-- one time run to create the historical table newly
CREATE TABLE silver.slv_branch_historical AS
SELECT
    CURRENT_DATE AS snapshot_date,
    t.*
FROM silver.slv_branch t
WHERE 1=0;


INSERT INTO silver.slv_branch_historical (
    snapshot_date,
    branch_id,
    branch_name,
    branch_city,
    state_name,
    branch_address,
    latitude,
    longitude,
    gstin,
    region_id,
    region_name,
    zone_id,
    zone_name,
    branch_incharge_emp_id,
    branch_incharge_name,
    branch_incharge_name_and_code,
    new_employee_id,
    nav_code,
    cersai_code,
    total_loans_originated,
    total_logins,
    total_loans_booked,
    total_loans_disbursed,
    total_loans_disbursal_cancelled,
    active_loan_count,
    total_pos,
    total_pos_after_sell,
    sma0_count,
    sma1_count,
    sma2_count,
    npa_sub_standard_count,
    doubtful1_count,
    doubtful2_count,
    doubtful3_count,
    npa_count,
    gross_npa_pct,
    active_employee_count,
    active_dsa_count,
    no_of_fos,
    record_created_at,
    record_modified_at,
    silver_loaded_at,
    silver_batch_id
)
WITH RECURSIVE dates (run_date) AS (
    SELECT DATE_TRUNC('month', DATEADD(month, -24, CURRENT_DATE))
    UNION ALL
    SELECT DATEADD(month, 1, run_date)
    FROM dates d
    WHERE run_date < DATE_TRUNC('month', CURRENT_DATE)
),

    deduped_branch AS (
        SELECT *
        FROM (
            SELECT
                b.*,
                ROW_NUMBER() OVER ( PARTITION BY b.branchid ORDER BY b.lastmodifiedon DESC) AS rn
            FROM dmihfclos.mstbranch b
            where  isactive=1
        ) t
        WHERE rn = 1
    ),
    -- Loan Aggregation
    loan_agg AS (
        SELECT
        d.run_date,
        ld.branch_id,
        COUNT( ld.loan_application_id) AS total_loans_originated,
        SUM(CASE  WHEN CAST(ld.login_date AS DATE) = d.run_date THEN 1 ELSE 0  END) AS total_logins,
        SUM(CASE WHEN CAST(ld.last_sanction_date AS DATE) = d.run_date  THEN 1 ELSE 0 END) AS total_loans_sanctioned,
        SUM(CASE WHEN CAST(ld.last_booked_date AS DATE) = d.run_date   THEN 1 ELSE 0 END) AS total_loans_booked,
        SUM(CASE WHEN CAST(ld.last_disbursal_date AS DATE) = d.run_date THEN 1 ELSE 0 END) AS total_loans_disbursed,
        SUM(CASE  WHEN loanstatustypedetailid = 1619 and  CAST(lash.createdon AS DATE) = d.run_date  THEN 1 ELSE 0  END) AS total_loans_disbursal_cancelled
    FROM dates d
    join silver.slv_loan_details ld on 1=1

    LEFT JOIN dmihfclos.tblloanapplicationstatushistory lash
        ON ld.loan_application_id = lash.loanapplicationid
        AND lash.isactive = 1
    GROUP BY ld.branch_id, d.run_date
        ),

    --    SELECT
    --         ld.branch_id,
    --         COUNT(ld.loan_application_id) AS total_loans_originated,
    --         sum(case when cast(TO_CHAR(login_date,'YYYYMM') as int) = cast(TO_CHAR(SYSDATE,'YYYYMM') as int) then 1 else 0 end ) as total_logins,
    --         sum(case when cast(TO_CHAR(last_sanction_date,'YYYYMM') as int)= cast(TO_CHAR(SYSDATE,'YYYYMM') as int)then 1 else 0 end) as total_loans_sanctioned,
    --         sum(case when  cast(TO_CHAR(last_booked_date,'YYYYMM') as int)= cast(TO_CHAR(SYSDATE,'YYYYMM') as int) then 1 else 0 end)  as total_loans_booked,
    --          sum(case when cast(TO_CHAR(last_disbursal_date,'YYYYMM') as int)= cast(TO_CHAR(SYSDATE,'YYYYMM') as int)then 1 else 0 end)  as total_loans_disbursed,
    --        sum(case when  loanstatustypedetailid = 1619  and  cast(TO_CHAR(lash.createdon,'YYYYMM') as int)= cast(TO_CHAR(SYSDATE,'YYYYMM') as int) then 1 else 0 end) as total_loans_disbursal_cancelled
    --     FROM silver.slv_loan_details ld
    --        left join dmihfclos.tblloanapplicationstatushistory lash on ld.loan_application_id = lash.loanapplicationid  and lash.isactive =1
    --     GROUP BY branch_id
    -- ),
    -- reconstructed using daily changing table -- Latest Performance Snapshot
    latest_perf AS (
        SELECT *
        FROM (
            SELECT
                ld.loanapplicationid AS loan_application_id,
                ld.pos  as pos,
                ld.pos * (
                            CASE
                                WHEN ped.percentage = 0 OR ped.percentage IS NULL
                                    THEN COALESCE(peld.percentage, 100)
                                ELSE ped.percentage
                            END / 100
                        ) AS pos_after_sell,            --- added  --    /* column added */
                ld.maxdeliquencyday AS dpd,             --- added  --    /* to discuss the usage*/
                ld.isnpa AS is_npa,
                CASE
                    WHEN ps.delinquentdays = 0 THEN '0'
                    WHEN ps.delinquentdays BETWEEN 1  AND 30     THEN 'SMA-0'
                    WHEN ps.delinquentdays BETWEEN 31 AND 60     THEN 'SMA-1'
                    WHEN ps.delinquentdays BETWEEN 61 AND 90     THEN 'SMA-2'
                    WHEN ps.delinquentdays BETWEEN 90 AND 365    THEN 'NPA (Sub-standard)'
                    WHEN ps.delinquentdays BETWEEN 366 AND 730   THEN  'Doubtful 1 (D1)'
                    WHEN ps.delinquentdays BETWEEN 731 AND 1095   THEN  'Doubtful 2 (D2)'
                    WHEN ps.delinquentdays > 1095  THEN  'Doubtful 3 (D3)'
                    ELSE NULL
                END AS dpd_bucket,
                ROW_NUMBER() OVER (PARTITION BY ld.loanapplicationid  ORDER BY ld.lastmodifiedon DESC) AS rn

            FROM dmihfclos.tblloanduestatus ld

            left join dmihfclos.tblLoanApplicationDisbursalDetail ladd
                ON ld.loanapplicationid = ladd.loanApplicationID

            LEFT JOIN dmihfclos.tblPortfolioPartnerEngagementDetail ped
                ON ladd.partnerengagementID = ped.engagementID
                AND ped.isActive = 1  and ped.engagementtype = 'SELL'    ---- added

            LEFT join dmihfclos.tblLoanApplicationPaySchedule  ps
                ON ld.loanapplicationid = ps.loanApplicationID

            LEFT JOIN dmihfclos.tblPortfolioPartnerEngagementLoanDetails peld
                ON ld.loanapplicationid = peld.loanApplicationID
                AND peld.isActive = 1
            where ld.isActive = 1
            ) t
        WHERE rn = 1
    ),
    perf_with_branch AS (
        SELECT
            ld.loan_application_id,
            ld.branch_id,
            lp.pos,
            lp.pos_after_sell,     -- added -- /* column added */
            lp.dpd_bucket,
            lp.dpd,
            lp.is_npa
        FROM latest_perf lp
        JOIN silver.slv_loan_details ld
            ON lp.loan_application_id = ld.loan_application_id
        WHERE lp.rn = 1
    ),
    -- Performance Aggregation
    perf_agg AS (
        SELECT
            pwb.branch_id,
            COUNT(
                CASE
                    WHEN pwb.pos > 0
    --                  )
                    THEN 1
                END
            ) AS active_loan_count,
            SUM(pwb.pos) AS total_pos,
            SUM(pwb.pos_after_sell) AS total_pos_after_sell,  /* column added */
            COUNT(CASE WHEN pwb.dpd > 0 THEN 1 END) AS dpd_count,
            COUNT(CASE WHEN UPPER(TRIM(pwb.is_npa)) IN ('1','TRUE','Y') THEN 1 END) AS npa_count,
            SUM(CASE WHEN dpd_bucket = 'SMA-0' THEN 1 END) as sma0_count,
            SUM(CASE WHEN dpd_bucket = 'SMA-1' THEN 1 END) as sma1_count,
            SUM(CASE WHEN dpd_bucket = 'SMA-2' THEN 1 END) as sma2_count,
            SUM(CASE WHEN dpd_bucket = 'NPA (Sub-standard)' THEN 1 END) as npa_sub_standard_count,
            SUM(CASE WHEN dpd_bucket = 'Doubtful 1 (D1)' THEN 1 END) as  doubtful1_count,
            SUM(CASE WHEN dpd_bucket = 'Doubtful 2 (D2)' THEN 1 END) as doubtful2_count,
            SUM(CASE WHEN dpd_bucket = 'Doubtful 3 (D3)' THEN 1 END) as doubtful3_count,
            SUM(CASE
                    WHEN UPPER(TRIM(pwb.is_npa)) IN ('1','TRUE','Y')  or lds.linkednpaloanid is not null   --- added -- --tblduestatus.linkednpaloanid -- have to add
                    THEN pwb.pos
                    ELSE 0
                END
            ) AS npa_pos
        FROM perf_with_branch pwb
        left join dmihfclos.tblloanduestatus lds  on pwb.loan_application_id =lds.loanapplicationid
        GROUP BY branch_id   /* to discuss for pos after sell*/
    ),
    -- Employee Aggregation
    employee_agg AS (
        SELECT
            base_branch_id,
            COUNT(employee_id) AS active_employee_count,
            COUNT(CASE
                    WHEN UPPER(designation) = 'FOS'
                    THEN employee_id
                  END
                ) AS no_of_fos
        FROM silver.slv_employee
    WHERE is_active = 1
    AND (
        is_resigned IS NULL
        OR UPPER(TRIM(is_resigned)) IN ('FALSE','0','N')
    )    GROUP BY base_branch_id
    ),
    -- DSA Aggregation
    dsa_agg AS (
        SELECT
            base_location_branch_id,
            COUNT(dsa_id) AS active_dsa_count
        FROM silver.slv_channel_partner
        GROUP BY base_location_branch_id
    )
    SELECT
        d.run_date AS snapshot_date,
        -- Identifiers
        CAST(b.branchid AS INT) AS branch_id,
        TRIM(b.branchname) AS branch_name,
        UPPER(TRIM(b.branchcity)) AS branch_city,
        UPPER(TRIM(b.statename)) AS state_name,
        TRIM(b.branchaddress) AS branch_address,
        CAST(NULLIF(TRIM(b.latitude), '') AS DECIMAL(12,8)) AS latitude,
        CAST(NULLIF(TRIM(b.longitude), '') AS DECIMAL(12,8)) AS longitude,
        UPPER(TRIM(b.gstin)) AS gstin,
        -- Hierarchy
        CAST(b.regionid AS INT) AS region_id,
        TRIM(r.regionname) AS region_name,
        CAST(r.zoneid AS INT) AS zone_id,
        TRIM(z.zonename) AS zone_name,
        CAST(b.branchinchargeemployeeid AS BIGINT) AS branch_incharge_emp_id,
        TRIM(e.employeename) AS branch_incharge_name,
        TRIM(e.employeename) || ' - ' || TRIM(e.employeecode) AS branch_incharge_name_and_code,             --- added      /* concat employee code */
        trim(e.newemployeeid)  AS new_employee_id,
        -- Codes
        TRIM(b.navcode) AS nav_code,
        TRIM(b.cersaicode) AS cersai_code,
    --     CASE
    --         WHEN UPPER(TRIM(b.isactive)) IN ('1','TRUE','Y')
    --         THEN TRUE
    --         ELSE FALSE
    --     END AS is_active,
        -- Analytics
        COALESCE(la.total_loans_originated, 0) AS total_loans_originated,
        COALESCE(la.total_logins, 0) AS total_logins,
        COALESCE(la.total_loans_booked, 0) AS total_loans_booked,
        COALESCE(la.total_loans_disbursed, 0) AS total_loans_disbursed,
        COALESCE(la.total_loans_disbursal_cancelled, 0) AS total_loans_disbursal_cancelled,

        COALESCE(pa.active_loan_count, 0) AS active_loan_count,
        COALESCE(pa.total_pos, 0) AS total_pos,
        coalesce(pa.total_pos_after_sell, 0 ) as total_pos_after_sell,           --- added --
        COALESCE(pa.sma0_count, 0) AS sma0_count,
        COALESCE(pa.sma1_count, 0) AS sma1_count,
        COALESCE(pa.sma2_count, 0) AS sma2_count,
        COALESCE(pa.npa_sub_standard_count, 0) AS npa_sub_standard_count,
        COALESCE(pa.doubtful1_count, 0) AS doubtful1_count,
        COALESCE(pa.doubtful2_count, 0) AS doubtful2_count,
        COALESCE(pa.doubtful3_count, 0) AS doubtful3_count,

        COALESCE(pa.npa_count, 0) AS npa_count,
        CAST(ROUND(COALESCE(pa.npa_pos, 0) / NULLIF(pa.total_pos, 0) * 100, 2) AS DECIMAL(6,2)) AS gross_npa_pct,


        --     CASE
        --         WHEN COALESCE(rd.unsecuredamount,0) = 0
        --             THEN 0
        --         ELSE (COALESCE(rd.securedamount,0) / rd.totalamount) * 100
        --     END AS collection_efficiency_pct,

        -- Employee / DSA
        COALESCE(ea.active_employee_count, 0) AS active_employee_count,
        COALESCE(da.active_dsa_count, 0) AS active_dsa_count,
        COALESCE(ea.no_of_fos, 0) AS no_of_fos,

        -- 28. fos_productivity ( login details not founds in tables)
        --      AS fos_productivity,


        -- Audit
        b.createdon AS record_created_at,
        b.lastmodifiedon AS record_modified_at,
        CURRENT_TIMESTAMP AS silver_loaded_at,
        TO_CHAR(GETDATE(),'YYYYMMDD_HH24MISS') AS silver_batch_id

    FROM   dates d
    JOIN    deduped_branch b  on 1=1
    LEFT JOIN dmihfclos.mstregion r    ON b.regionid = r.regionid   /* inner join */
    LEFT JOIN dmihfclos.mstzone z    ON r.zoneid = z.zoneid     /* inner join */
    LEFT JOIN dmihfclos.tblemployee e    ON b.branchinchargeemployeeid = e.employeeid
    LEFT JOIN loan_agg la    ON b.branchid = la.branch_id   AND la.run_date = d.run_date
    LEFT JOIN perf_agg pa    ON b.branchid = pa.branch_id
    LEFT JOIN employee_agg ea    ON b.branchid = ea.base_branch_id
    -- left join dmihfclos.tblapplicantriskdetail rd
    --     on rd.loanapplicationid = c.loan_application_id
    LEFT JOIN dsa_agg da    ON b.branchid = da.base_location_branch_id;

