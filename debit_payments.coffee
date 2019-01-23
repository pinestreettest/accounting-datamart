
---------------------------------------
---------------------------------------
---------------------------------------
--- Create Debit Card File ---

create table acct_debit_payments as
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
and a.content_type_id = 23
and a.status in ('valid');

create table acct_debit_payments_1 as
  select
    borrower_id,
    account_id,
    user_id,
    src_dt,
    count(amount) as payment_count,
    sum(amount) as payment_amount
from acct_debit_payments
group by
    borrower_id,
    account_id,
    user_id,
    src_dt;

drop table acct_debit_payments;
alter table acct_debit_payments_1 rename to acct_debit_payments;
---------------------------------------

-- Aggregate Same Day Payments to Account Level --

create table acct_debit_payments_1 as
  select
    a.account_id,
    cast(b.created as date) as created,
    sum(b.amount) as amount
from acct_debit_payments a
left join payment b on a.borrower_id = b.borrower_id
where cast(b.created as date) = a.src_dt
group by
    a.account_id,
    cast(b.created as date);

create table acct_debit_payments_2 as
  select
    a.account_id,
    a.borrower_id,
    a.user_id,
    a.payment_amount,
    a.src_dt,
    sum(b.amount) as amount
from acct_debit_payments a
left join acct_debit_payments_1 b on a.account_id = b.account_id
group by
    a.account_id,
    a.borrower_id,
    a.user_id,
    a.payment_amount,
    a.src_dt;

drop table acct_debit_payments;
drop table acct_debit_payments_1;
alter table acct_debit_payments_2 rename to acct_debit_payments;
---------------------------------------

-- Add Receivables Data to Help Tag Debit Card Transactions --

create table acct_debit_payments_1 as
  select
    a.*,
    b.prev_balance,
    b.curr_balance
from acct_debit_payments a
left join acct_receivables_rollup b on a.account_id = b.account_id;

drop table acct_debit_payments;
alter table acct_debit_payments_1 rename to acct_debit_payments;
---------------------------------------

-- Categorize Incoming Debit Card Payments --

create table acct_debit_payments_1 as
  select
    *,
    case
      when amount is null then null
      when payment_amount <= amount then payment_amount
      when payment_amount > amount then amount end as applied_to_loans,
    case
      when amount is null then payment_amount
      when payment_amount <= amount then null
      when payment_amount > amount then payment_amount - amount end as applied_to_credit
from acct_debit_payments;

drop table acct_debit_payments;
alter table acct_debit_payments_1 rename to acct_debit_payments;
---------------------------------------


insert into perpay_accounting_datamart.acct_debit_payments
  select distinct
    account_id,
    src_dt,
    payment_amount as debitcard_amount,
    applied_to_loans,
    applied_to_debit
from acct_debit_payments;
---------------------------------------
---------------------------------------
---------------------------------------
