-- slv_employee
DROP TABLE IF EXISTS silver.slv_employee;
CREATE TABLE silver.slv_employee
DISTKEY(employee_id)
SORTKEY(employee_id)
AS

WITH emp_dedup AS (
    SELECT
        employeeid,
        employeecode,
        employeename,
        basebranchid,
        departmentid,
        designationleveltypedetailid,
        designationtypedetailid,
        parentemployeeid,
        joiningdate,
        isResigned,
        relivingdate,
        isactive,
        createdon,
        entityid,
        lastmodifiedon,
        ROW_NUMBER() OVER (
            PARTITION BY employeeid
            ORDER BY lastmodifiedon DESC NULLS LAST
        ) AS rn
    FROM dmihfclos.tblemployee
    where isactive =1
),
    cte_mstbranch  as(
        select
            branchname,
            entityid
    FROM dmihfclos.mstBranch
    where isactive =1
),
loan_agg AS (
    SELECT
        sales_officer_emp_id,
        COUNT(DISTINCT loan_application_id) AS loans_onboarded_total,
        COUNT(
            CASE
                WHEN date_trunc('month', last_disbursal_date) = date_trunc('month', CURRENT_DATE)
                THEN 1
            END  ) AS loans_onboarded_mtd,
        SUM(COALESCE(total_disbursed_amount, 0)) AS total_disbursed_amount
    FROM silver.slv_loan_details
    GROUP BY sales_officer_emp_id
)
-- Portfolio metrics
,portfolio_metrics AS (
    SELECT
        ld.sales_officer_emp_id AS employee_id,

        COUNT(CASE WHEN c.pos > 0 THEN 1 END) AS active_loan_count,
        SUM(c.pos) AS current_book_pos,

        COUNT(CASE WHEN c.max_dpd > 0 THEN 1 END) AS loans_in_dpd,

        COUNT(CASE 
            WHEN UPPER(TRIM(c.is_npa)) IN ('1','TRUE','Y')
            THEN 1 
        END) AS loans_in_npa,

        SUM(c.due_interest + c.due_principal) AS total_arrears_amount,

        CASE
            WHEN SUM(rd.totalamount) = 0 THEN 0
            ELSE (SUM(rd.securedamount) / SUM(rd.totalamount)) * 100
        END AS collection_efficiency_pct,

        -- DPD %
        CASE
            WHEN COUNT(CASE WHEN c.pos > 0 THEN 1 END) > 0
            THEN (
                COUNT(CASE WHEN c.max_dpd > 0 THEN 1 END)::DECIMAL
                / COUNT(CASE WHEN c.pos > 0 THEN 1 END)
            ) * 100
        END AS dpd_rate_pct,

        -- NPA %
        CAST(
            ROUND(
                SUM(
                    CASE
                        WHEN UPPER(TRIM(c.is_npa)) IN ('1','TRUE','Y')
                        THEN c.pos ELSE 0
                    END
                ) / NULLIF(SUM(c.pos), 0) * 100,
            2)
        AS DECIMAL(6,2)) AS npa_rate_pct

    FROM silver.slv_contract_perf_monthly c

    JOIN silver.slv_loan_details ld
        ON c.loan_application_id = ld.loan_application_id

    LEFT JOIN dmihfclos.tblapplicantriskdetail rd
        ON rd.loanapplicationid = c.loan_application_id
       AND rd.isactive = 1

    GROUP BY ld.sales_officer_emp_id
)
SELECT
    CAST(e.employeeid AS BIGINT)   AS employee_id,
    TRIM(UPPER(e.employeecode))   AS employee_code,
    TRIM(e.employeename)     AS employee_name,
    CAST(e.basebranchid AS INT)  AS base_branch_id,
    mb.branchname      AS base_branch_name,
    e.departmentid     AS department,
    e.designationtypedetailid   AS designation,
    e.designationleveltypedetailid    AS designation_level,
    CAST(e.parentemployeeid AS INT)    AS parent_employee_id,
    CAST(e.joiningdate AS DATE)    AS joining_date,
    CAST(e.relivingdate AS DATE)    AS relieving_date,
    e.isResigned  AS is_Resigned, 
    CASE WHEN e.isactive = 1 THEN TRUE ELSE FALSE END    AS is_active,

    la.loans_onboarded_total,
    la.loans_onboarded_mtd,
    la.total_disbursed_amount,

    pa.active_loan_count,
    pa.current_book_pos,
    pa.loans_in_dpd,
    pa.loans_in_npa,
    pa.total_arrears_amount,
    pa.collection_efficiency_pct,
    pa.dpd_rate_pct,
    pa.npa_rate_pct,

    CAST(e.createdon AS TIMESTAMP)  AS record_created_at,
    CAST(e.lastmodifiedon AS TIMESTAMP) AS record_modified_at,
    CURRENT_TIMESTAMP AS silver_loaded_at,
    TO_CHAR(GETDATE(),'YYYYMMDD_HH24MISS') AS silver_batch_id

FROM emp_dedup e   
LEFT JOIN loan_agg la
    ON la.sales_officer_emp_id = e.employeeid
LEFT JOIN cte_mstbranch mb
    ON mb.entityid = e.entityid
LEFT JOIN portfolio_metrics pa
ON pa.employee_id = e.employeeid
