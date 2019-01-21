
---------------------------------------
---------------------------------------
---------------------------------------
--- Create ACH File ---

create table acct_ach_payments as
  select
    b.id as borrower_id,
    b.user_id,
    a.account_id,
    case when a.account_id is not null then '2019-01-16' else null end as src_dt,
    a.amount,
    a.created as received_dt
from deposit a
left join borrower b on a.account_id = b.account_id
where cast(a.created as date) > '2019-01-15'
and cast(a.created as date) < '2019-01-17'
and a.content_type_id = 46
and a.status in ('valid');

create table acct_ach_payments_1 as
  select
    borrower_id,
    account_id,
    user_id,
    src_dt,
    count(amount) as payment_count,
    sum(amount) as payment_amount
from acct_ach_payments
group by
    borrower_id,
    account_id,
    user_id,
    src_dt;

drop table acct_ach_payments;
alter table acct_ach_payments_1 rename to acct_ach_payments;
---------------------------------------

-- Categorize Incoming ACH --

create table acct_ach_payments_1 as
  select
    a.account_id,
    a.borrower_id,
    a.user_id,
    a.payment_amount,
    a.src_dt,
    cast(b.created as date) as created,
    sum(b.amount) as amount
from acct_ach_payments a
left join payment b on a.borrower_id = b.borrower_id
where cast(b.created as date) >= a.src_dt
group by
    a.account_id,
    a.borrower_id,
    a.user_id,
    a.payment_amount,
    a.src_dt,
    cast(b.created as date);

create table acct_ach_payments_2 as
  select
    *
  from (select *,
rank() over (partition by borrower_id order by created) as record_rank
from acct_ach_payments_1 order by borrower_id, created) as ranked
where ranked.record_rank = 1;

create table acct_ach_payments_3 as
  select
    *,
    case when payment_amount <= amount then payment_amount
    when payment_amount > amount then amount end as applied_to_loans,
    case when payment_amount <= amount then null
    when payment_amount > amount then payment_amount - amount end as applied_to_credit
from acct_ach_payments_2;

drop table acct_ach_payments;
drop table acct_ach_payments_1;
drop table acct_ach_payments_2;
alter table acct_ach_payments_3 rename to acct_ach_payments;

insert into perpay_accounting_datamart.acct_ach_payments
  select distinct
    account_id,
    src_dt,
    payment_amount as ach_amount,
    applied_to_loans,
    applied_to_credit
from acct_ach_payments;
---------------------------------------
---------------------------------------
---------------------------------------
