-- slv_branch
CREATE TABLE silver.slv_branch
DISTKEY(branch_id)
SORTKEY(branch_id)
AS

WITH deduped_branch AS (
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
        branch_id,
        COUNT(loan_application_id) AS total_loans_originated
    FROM silver.slv_loan_details
    GROUP BY branch_id
),
-- Latest Performance Snapshot
latest_perf AS (
    SELECT
        loan_application_id,
        pos,
        dpd,
        is_npa,
        ROW_NUMBER() OVER (  PARTITION BY loan_application_id  ORDER BY snapshot_date DESC ) AS rn
    FROM silver.slv_contract_perf_monthly
),
perf_with_branch AS (
    SELECT
        ld.branch_id,
        lp.pos,
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
        branch_id,
        COUNT(
            CASE
                WHEN pos > 0
--                  AND DATE_TRUNC('month', snapshot_date) = (
--                      SELECT DATE_TRUNC('month', MAX(snapshot_date))
--                      FROM silver.slv_contract_perf_monthly
--                  )
                THEN 1
            END
        ) AS active_loan_count,
        SUM(pos) AS total_pos,
        COUNT(CASE WHEN dpd > 0 THEN 1 END) AS dpd_count,
        COUNT(CASE
                WHEN UPPER(TRIM(is_npa)) IN ('1','TRUE','Y')
                THEN 1
              END
        ) AS npa_count,
        SUM(CASE
                WHEN UPPER(TRIM(is_npa)) IN ('1','TRUE','Y')
                THEN pos
                ELSE 0
            END
        ) AS npa_pos
    FROM perf_with_branch
    GROUP BY branch_id
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
    GROUP BY base_branch_id
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
    -- Identifiers
    CAST(b.branchid AS INT) AS branch_id,
    TRIM(b.branchname) AS branch_name,
    UPPER(TRIM(b.branchcity)) AS branch_city,
    UPPER(TRIM(b.statename)) AS state_name,
    TRIM(b.branchaddress) AS branch_address,
    CAST(b.latitude AS DECIMAL(12,8)) AS latitude,
    CAST(b.longitude AS DECIMAL(12,8)) AS longitude,
    UPPER(TRIM(b.gstin)) AS gstin,
    -- Hierarchy
    CAST(b.regionid AS INT) AS region_id,
    TRIM(r.regionname) AS region_name,
    CAST(r.zoneid AS INT) AS zone_id,
    TRIM(z.zonename) AS zone_name,
    CAST(b.branchinchargeemployeeid AS BIGINT) AS branch_incharge_emp_id,
    TRIM(e.employeename) AS branch_incharge_name,
    -- Codes
    TRIM(b.navcode) AS nav_code,
    TRIM(b.cersaicode) AS cersai_code,
    CASE
        WHEN UPPER(TRIM(b.isactive)) IN ('1','TRUE','Y')
        THEN TRUE
        ELSE FALSE
    END AS is_active,
    -- Analytics
    COALESCE(la.total_loans_originated, 0) AS total_loans_originated,
    COALESCE(pa.active_loan_count, 0) AS active_loan_count,
    COALESCE(pa.total_pos, 0) AS total_pos,
    COALESCE(pa.dpd_count, 0) AS dpd_count,
    COALESCE(pa.npa_count, 0) AS npa_count,
    CAST(ROUND(COALESCE(pa.npa_pos, 0) / NULLIF(pa.total_pos, 0) * 100, 2) AS DECIMAL(6,2)) AS gross_npa_pct,

    -- 24. no proper join found for this formula o derive

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

FROM deduped_branch b
LEFT JOIN dmihfclos.mstregion r    ON b.regionid = r.regionid
LEFT JOIN dmihfclos.mstzone z    ON r.zoneid = z.zoneid
LEFT JOIN dmihfclos.tblemployee e    ON b.branchinchargeemployeeid = e.employeeid
LEFT JOIN loan_agg la    ON b.branchid = la.branch_id
LEFT JOIN perf_agg pa    ON b.branchid = pa.branch_id
LEFT JOIN employee_agg ea    ON b.branchid = ea.base_branch_id
-- left join dmihfclos.tblapplicantriskdetail rd
--     on rd.loanapplicationid = c.loan_application_id
LEFT JOIN dsa_agg da    ON b.branchid = da.base_location_branch_id;