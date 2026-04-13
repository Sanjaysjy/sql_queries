WITH LeadFollowUpAggregations AS (
    SELECT
        leadId,
        MAX(createdOn) AS last_followup_date,
        COUNT(leadFollowUpId) AS total_followups
    FROM dmihfclos.tblLeadFollowUp
    GROUP BY leadId
)
SELECT
    l.leadID AS lead_id,
    TRIM(l.leadNumber) AS lead_number,
    l.salesforceID AS salesforce_id,
    TRIM(l.leadSource) AS lead_source,
    TRIM(l.leadSubSource) AS lead_sub_source,
    UPPER(l.deviceType) AS device_type,
    dt1.typeDetailDescription AS sourcing_channel,
    dt2.typeDetailDescription AS campaign_name,
    l.dsaID AS dsa_id,
    TRIM(l.applicantFirstName) || ' ' || TRIM(l.applicantMiddleName) || ' ' || TRIM(l.applicantLastName) AS applicant_name,
    l.amountRequest AS amount_requested,
    l.loanProductID AS product_id,
    dt3.typeDetailDescription AS current_status,
    l.isConvertedToLoanApplication AS is_converted_to_loan,
    l.branchID AS branch_id,
    DATE(l.createdOn) AS lead_created_date,
    lfa.last_followup_date,
    lfa.total_followups,
    DATE(lfa.last_followup_date) - DATE(l.createdOn) AS days_to_conversion,
    l.lastModifiedBy AS lead_rejected_submitted_by,
    l.createdBy AS lead_created_by,
    l.createdOn AS record_created_at,
    l.lastModifiedOn AS record_modified_at,
    CURRENT_TIMESTAMP AS silver_loaded_at,
    TO_CHAR(GETDATE(),'YYYYMMDD_HH24MISS') AS silver_batch_id
FROM dmihfclos.tblLead l
Left JOIN dmihfclos.tblTypeDetail dt1 ON l.sourcingChannelPartnerTypeDetailID = dt1.typeDetailID
Left JOIN dmihfclos.tblTypeDetail dt2 ON l.campanianTypeDetailID = dt2.typeDetailID
Left JOIN dmihfclos.tblTypeDetail dt3 ON l.currentStatusTypeDetailID = dt3.typeDetailID
Left JOIN LeadFollowUpAggregations lfa ON l.leadID = lfa.leadId;
