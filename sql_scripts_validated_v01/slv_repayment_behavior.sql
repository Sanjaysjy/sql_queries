--  slv_repayment_behavior
drop table  if exists silver.slv_repayment_behavior;

CREATE TABLE silver.slv_repayment_behavior
DISTKEY(loan_application_id)
SORTKEY(pay_schedule_id)
AS



WITH
payment_receipts  AS (
   select
   a.loanapplicationid,
   a.presentationdetailid as id,
   isknockoff,
   a.pdcachdate as payment_date,
   a.pdcachamount as collected_amount,
   pre.presentationtypedetailid payment_mode_id,
   case when pre.isrepresentation is not null  and pre.isrepresentation = 1 then  concat(typ.typedetaildisplaytext,'_represented')
      else typ.typedetaildisplaytext end as payment_mode,
   a.chequerefrenceno as reference_no,
   'presentation' as paytype
from dmihfclos.tblloanapplicationpresentationdetail a
inner join
dmihfclos.tblloanapplicationpresentationbatch pre on a.presentationid = pre.presentationid
left join dmihfclos.tbltypedetail typ on pre.presentationtypedetailid = typ.typedetailid
where a.isactive = 1 and pre.isactive = 1 and a.statustypedetailid = 2066

union all

select
loanapplicationid,
receiptid as id,
isknockoff,
case when typedetaildisplaytext = 'Cash' then dateofcollection
else statusupdateddate end as payment_date,
collectedamount as collected_amount,
collectedthroughtypedetailid as payment_mode_id,
typedetaildisplaytext payment_mode,
chequenumber as reference_no,
'manualreceipt' as paytype
FROM dmihfclos.tblloanapplicationManualReceipt mn
left join
dmihfclos.tbltypedetail typ on mn.collectedthroughtypedetailid = typ.typedetailid
        where mn.isactive = 1 and mn.statustypedetailid = 219
        and feesfortypedetailid = 1861
),

loan_repayment as (
   SELECT
        a.payscheduleid,
        a.loanapplicationid,
        a.head,
        a.duedate,
        a.dueamount,
        a.principal,
        a.interst as interest,
        a.closingbalance,
        a.applicableroi,
        a.paymentreceiveddate,
        a.delinquentdays,
        a.clearenceflag,
        b.paytype,
        COALESCE(b.presentationdetailid, b.receiptid) id
    FROM dmihfclos.tblloanapplicationpayschedule a
    left join dmihfclos.tblloanpaymentadjust b on a.loanapplicationid = b.loanapplicationid and a.payscheduleid = b.payscheduleid
    left join payment_receipts c on c.loanapplicationid = a.loanapplicationid and c.paytype = b.paytype and COALESCE(b.presentationdetailid, b.receiptid) = c.id
    WHERE a.isactive = 1 and a.duedate<= current_date
    and  b.isactive = 1 and b.payscheduleid is not NULL

)

select * from loan_repayment