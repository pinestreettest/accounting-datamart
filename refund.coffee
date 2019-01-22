---------------------------------------
---------------------------------------
---------------------------------------
--- Create Refund Table ---

create table acct_refund as
  select
    c.borrower_id,
    b.loan_id,
    case when a.amount is not null then '2019-01-16' end as src_dt,
    a.amount
from refund a
left join charge b on a.charge_id = b.id
left join acct_receivables c on b.loan_id = c.loan_id
where cast(a.created as date) = '2019-01-16'
and c.borrower_id is not null;

create table acct_refund_rollup as
  select
    a.account_id,
    b.src_dt,
    sum(b.amount) as refunds
from acct_fact a
left join acct_refund b on a.borrower_id = b.borrower_id
where b.src_dt is not null
group by a.account_id, b.src_dt;

insert into perpay_accounting_datamart.acct_refund select distinct * from acct_refund_rollup;

---------------------------------------

---------------------------------------
---------------------------------------
---------------------------------------
