-- slv_channel_partner
CREATE TABLE silver.slv_channel_partner
DISTKEY(dsa_id)
SORTKEY(dsa_id)
AS
WITH CombinedData AS (
    SELECT
        dsa.dsaID AS dsa_id,
        dsa.entityID AS entity_id,
        renewal.dsacode  AS dsa_Code,
        typeDetail.typedetaildisplaytext AS channel_type,
        renewal.companyName AS partner_name,
        renewal.contractstartdate  AS contract_start_date,
        renewal.contractenddate   AS contract_end_date,
        renewal.typeoforganisationtypedetailid  AS type_of_organisation_type_detail_id,
        type_ren.typedetaildisplaytext  AS type_of_organisation_type_detail_id_value,


        renewal.iskycdetailverified  AS is_kyc_detail_verified,
        renewal.isreportverified  AS is_report_verified,
        renewal.isfiverified  AS is_fi_verified,
        renewal.iscibilverified  AS is_cibil_verified,
        renewal.isfcuverified  AS is_fcu_verified,
        renewal.isagreementverified  AS is_agreement_verified,
        renewal.ispayoutannexureverified  AS is_payout_annexure_verified,

        request.typeoforganisationtypedetailid as organisation_type_id,
        typeDetail.typedetaildisplaytext  as organisation_type_id_value,
        request.occupationtypedetailid  AS occupation_type_detail_id,

        renewal.iscorporate   as is_corporate,
        renewal.standardpayout AS is_standard_payout,
        dsa.isActive,
        dsa.isDormant,
        dsa.baseLocation AS base_location_branch_id,
        renewal.approvalStatusTypeDetailID AS approval_status_id,
        type_renewal.typedetaildisplaytext  AS approval_status_id_value,
        renewal.createdOn AS record_created_at,
        renewal.lastModifiedOn AS record_modified_at
    FROM dmihfclos.tblDsa dsa
    LEFT JOIN dmihfclos.tblEntity entity ON dsa.entityID = entity.entityID   and  entity..isActive = 1
    LEFT JOIN dmihfclos.tblTypeDetail typeDetail ON dsa.channelTypeTypeDetailID = typeDetail.typeDetailID
    LEFT JOIN dmihfclos.tblTypeDetail type_renewal ON renewal.approvalStatusTypeDetailID = type_renewal.typeDetailID
    LEFT JOIN dmihfclos.tblTypeDetail type_ren ON renewal.typeoforganisationtypedetailid = type_ren.typeDetailID,
    LEFT JOIN dmihfclos.tblTypeDetail type_request ON request.typeoforganisationtypedetailid = type_request.typeDetailID
    LEFT JOIN dmihfclos.tblDsaRenewal renewal ON dsa.dsaID = renewal.dsaID  and renewal..isActive = 1
    LEFT JOIN dmihfclos.tbldsarequest request ON renewal.dsacode = renewal.draftdsaid  and renewal..isActive = 1  --- need confirmation on the join entity
    WHERE dsa.isActive = 1 --and dsa_id = 455
),
AggregatedCounts AS (
    SELECT
        entityID AS entity_id,
        COUNT(entityCoveredBranchID) AS covered_branch_count
    FROM dmihfclos.tblEntityCoveredBranch
    where isActive = 1
    GROUP BY entityID
)
SELECT
    cd.dsa_id,
    cd.entity_id,
    cd.dsa_Code,
    cd.channel_type,
    cd.partner_name,
    cd.is_corporate,
    cd.is_standard_payout,
    cd.isActive,
    cd.isDormant,
    cd.base_location_branch_id,
    cd.contract_start_date,
    cd.contract_end_date,
    cd.type_of_organisation_type_detail_id,
    cd.type_of_organisation_type_detail_id_value,
    cd.is_kyc_detail_verified,
    cd.is_report_verified,
    cd.is_fi_verified,
    cd.is_cibil_verified,
    cd.is_fcu_verified,
    cd.is_agreement_verified,
    cd.is_payout_annexure_verified,
    cd.organisation_type_id,
    cd.organisation_type_id_value,
    cd.occupation_type_detail_id,
    approval.typedetaildisplaytext AS agreement_status,
    CASE
        WHEN approval.typedetailcode = 'Initiated' THEN 'FI Positive'
        ELSE 'FI Negative'
    END AS fi_status,    --- need confirmation from dmi for entity



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
ORDER BY cd.dsa_id