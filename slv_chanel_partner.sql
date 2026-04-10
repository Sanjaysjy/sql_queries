WITH CombinedData AS (
    SELECT
        dsa.dsaID AS dsa_id,
        dsa.entityID AS entity_id,
--         typeDetail.typeDetailDescription AS channel_type,  --verification needed
        renewal.companyName AS company_name,
        dsa.isActive,
        dsa.isDormant,
        dsa.baseLocation AS base_location_branch_id,
        renewal.approvalStatusTypeDetailID AS approval_status_id,
        renewal.createdOn AS record_created_at,
        renewal.lastModifiedOn AS record_modified_at
    FROM dmihfclos.tblDsa dsa
    LEFT JOIN dmihfclos.tblEntity entity ON dsa.entityID = entity.entityID
    LEFT JOIN dmihfclos.tblTypeDetail typeDetail ON dsa.channelTypeTypeDetailID = typeDetail.typeDetailID
    LEFT JOIN dmihfclos.tblDsaRenewal renewal ON dsa.dsaID = renewal.dsaID
    WHERE dsa.isActive = 1
),
AggregatedCounts AS (
    SELECT
        entityID AS entity_id,
        COUNT(entityCoveredBranchID) AS covered_branch_count
    FROM dmihfclos.tblEntityCoveredBranch
    GROUP BY entityID
)
SELECT
    cd.dsa_id,
    cd.entity_id,
    -- cd.channel_type,   --verification needed 
    cd.company_name,
    cd.isActive,
    cd.isDormant,
    cd.base_location_branch_id,
    approval.typeDetailDescription AS agreement_status,
    CASE
        WHEN approval.typeDetailDescription = 'Initiated' THEN 'FI Positive'
        ELSE 'FI Negative'
    END AS fi_status,

    /* WIP - logic not defined */

--     NULL AS wip_branch_count,
--     NULL AS wip_ho_count,
--     NULL AS wip_fcu_count,




    -- em.empID   AS mapped_employee_id,  --- --verification needed
    COALESCE(br.covered_branch_count, 0) AS covered_branch_count,
    cd.record_created_at,
    cd.record_modified_at,
    CURRENT_TIMESTAMP AS silver_loaded_at,
    TO_CHAR(GETDATE(),'YYYYMMDD_HH24MISS') AS silver_batch_id
FROM CombinedData cd
-- LEFT JOIN  tblDsaEmployeeMapping em ON cd.dsa_id = em.dsaid -- --verification needed
LEFT JOIN AggregatedCounts br ON cd.entity_id = br.entity_id
LEFT JOIN dmihfclos.tblTypeDetail approval ON cd.approval_status_id = approval.typeDetailID
ORDER BY cd.dsa_id;
