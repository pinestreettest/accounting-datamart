---------------------------------------
---------------------------------------
---------------------------------------
--- Create Reconciliation Table ---

create table acct_reconcile as
  select
    a.borrower_id,
    a.user_id,
    a.account_id,
    a.email,
    b.prev_balance,
    b.curr_balance
from acct_fact a
left join acct_receivables_rollup b on a.account_id = b.account_id;

alter table acct_reconcile add column src_dt date;
update acct_reconcile set src_dt = '2019-01-16';

-- Add Credit Balance Statuses --

create table acct_reconcile_1 as
  select
    user_id,
    id,
    created,
    starting_balance as acct_bal_s,
    ending_balance as acct_bal_e
from core_accountbalancehistory
where cast(created as date) = '2019-01-16';

-- Find Inflows --
create table acct_reconcile_2 as
  select
    user_id,
    sum(acct_bal_e) - sum(acct_bal_s) as inflows_amt,
    count(user_id) as inflows_cnt
from acct_reconcile_1
where acct_bal_s < acct_bal_e
group by user_id;

-- Find Outflows --
create table acct_reconcile_3 as
  select
    user_id,
    sum(acct_bal_s) - sum(acct_bal_e) as outflows_amt,
    count(user_id) as outflows_cnt
from acct_reconcile_1
where acct_bal_e < acct_bal_s
group by user_id;

-- Find Beginning and Ending Acct Balances --
create table acct_reconcile_4 as
  select *
  from
  (select *,
          rank() over (partition by user_id order by id) as record_rank
from acct_reconcile_1 order by user_id, id) as ranked
where ranked.record_rank = 1;

create table acct_reconcile_5 as
  select *
  from
  (select *,
          rank() over (partition by user_id order by id desc) as record_rank
from acct_reconcile_1 order by user_id, id desc) as ranked
where ranked.record_rank = 1;

create table acct_reconcile_6 as
  select
    a.*,
    b.acct_bal_s,
    c.acct_bal_e,
    d.inflows_amt,
    d.inflows_cnt,
    e.outflows_amt,
    e.outflows_cnt
from acct_reconcile a
left join acct_reconcile_4 b on a.user_id = b.user_id
left join acct_reconcile_5 c on a.user_id = c.user_id
left join acct_reconcile_2 d on a.user_id = d.user_id
left join acct_reconcile_3 e on a.user_id = e.user_id;

create table acct_reconcile_7 as
  select *
from acct_reconcile_6
where (prev_balance is not null or curr_balance is not null)
or (acct_bal_s is not null or acct_bal_e is not null);

drop table acct_reconcile;
drop table acct_reconcile_1;
drop table acct_reconcile_2;
drop table acct_reconcile_3;
drop table acct_reconcile_4;
drop table acct_reconcile_5;
drop table acct_reconcile_6;
alter table acct_reconcile_7 rename to acct_reconcile;
---------------------------------------

-- Add New Revenue --

create table acct_reconcile_1 as
  select
    a.account_id,
    sum(b.amount) as amount,
    sum(b.credit) as credit
from acct_reconcile a
left join acct_revenue b on a.account_id = b.account_id
group by a.account_id;

create table acct_reconcile_2 as
  select
    account_id,
    case
      when amount = credit then amount
      else amount - credit
      end as revenue,
    case
      when amount = credit then amount
      else 0
      end as payoff_from_credit
from acct_reconcile_1;

create table acct_reconcile_3 as
  select
    a.*,
    b.revenue,
    b.payoff_from_credit
from acct_reconcile a
left join acct_reconcile_2 b on a.account_id = b.account_id;

drop table acct_reconcile;
drop table acct_reconcile_1;
drop table acct_reconcile_2;
alter table acct_reconcile_3 rename to acct_reconcile;
---------------------------------------

-- Add Incoming Cash To Reconciliation --
create table acct_reconcile_1 as
  select
    a.*,
    b.applied_to_loans as ach_applied_to_loans,
    c.applied_to_loans as creditcard_applied_to_loans,
    d.applied_to_loans as debitcard_applied_to_loans,
    b.applied_to_credit as ach_applied_to_credit,
    c.applied_to_credit as creditcard_applied_to_credit,
    d.applied_to_credit as debitcard_applied_to_credit
from acct_reconcile a
left join acct_ach_payments b on a.account_id = b.account_id
left join acct_credit_payments c on a.account_id = c.account_id
left join acct_debit_payments d on a.account_id = d.account_id;

drop table acct_reconcile;
alter table acct_reconcile_1 rename to acct_reconcile;
---------------------------------------

-- Add Refunds To Reconciliation --
create table acct_reconcile_1 as
  select
    a.*,
    case
      when b.refunds is not null and b.refunds > (a.prev_balance-a.curr_balance) then (a.prev_balance-a.curr_balance)
      else b.refunds end as refunds_to_loans,
    case
      when b.refunds is not null and b.refunds > (a.prev_balance-a.curr_balance) then (b.refunds - a.prev_balance)
      else null end as refunds_to_credit
from acct_reconcile a
left join acct_refund_rollup b on a.account_id = b.account_id;

drop table acct_reconcile;
alter table acct_reconcile_1 rename to acct_reconcile;
---------------------------------------

-- Add Cash Credits Being Applied to AP Loans --

create table acct_reconcile_1 as
  select
    a.account_id,
    sum(b.cash_credit_amount) as cash_credits_to_ap_same_day
from acct_reconcile a
left join acct_awaitpay_creds b on a.account_id = b.account_id
and a.src_dt = b.awaiting_payment_dt
where b.cash_credit_amount is not null
group by a.account_id;

create table acct_reconcile_2 as
  select
    a.account_id,
    sum(b.cash_credit_amount) as cash_credits_to_ap_previously
from acct_reconcile a
left join acct_awaitpay_creds b on a.account_id = b.account_id
and a.src_dt > b.awaiting_payment_dt
where b.cash_credit_amount is not null
and b.cash_credit_amount > 0
group by a.account_id;

create table acct_reconcile_3 as
  select
    a.*,
    b.cash_credits_to_ap_same_day,
    c.cash_credits_to_ap_previously
from acct_reconcile a
left join acct_reconcile_1 b on a.account_id = b.account_id
left join acct_reconcile_2 c on a.account_id = c.account_id;

drop table acct_reconcile;
drop table acct_reconcile_1;
drop table acct_reconcile_2;
alter table acct_reconcile_3 rename to acct_reconcile;
---------------------------------------

-- Add Borrower Credits Being Applied to AP Loans --

create table acct_reconcile_1 as
  select
    a.account_id,
    sum(b.borrower_credit_amount) as brwr_credits_to_ap_same_day
from acct_reconcile a
left join acct_awaitpay_creds b on a.account_id = b.account_id
and a.src_dt = b.awaiting_payment_dt
where b.cash_credit_amount is not null
group by a.account_id;

create table acct_reconcile_2 as
  select
    a.account_id,
    sum(b.borrower_credit_amount) as brwr_credits_to_ap_previously
from acct_reconcile a
left join acct_awaitpay_creds b on a.account_id = b.account_id
and a.src_dt > b.awaiting_payment_dt
where b.cash_credit_amount is not null
and b.cash_credit_amount > 0
group by a.account_id;

create table acct_reconcile_3 as
  select
    a.*,
    b.brwr_credits_to_ap_same_day,
    c.brwr_credits_to_ap_previously
from acct_reconcile a
left join acct_reconcile_1 b on a.account_id = b.account_id
left join acct_reconcile_2 c on a.account_id = c.account_id;

drop table acct_reconcile;
drop table acct_reconcile_1;
drop table acct_reconcile_2;
alter table acct_reconcile_3 rename to acct_reconcile;
---------------------------------------

-- Add Cash Credits Being Removed from Canceled Loans --

create table acct_reconcile_1 as
  select
    a.account_id,
    sum(b.cash_credit_amount) as cash_credits_from_cancels_same_day
from acct_reconcile a
left join acct_canceled_creds b on a.account_id = b.account_id
and a.src_dt = b.canceled_dt
where b.cash_credit_amount is not null
group by a.account_id;

create table acct_reconcile_2 as
  select
    a.*,
    b.cash_credits_from_cancels_same_day
from acct_reconcile a
left join acct_reconcile_1 b on a.account_id = b.account_id;

drop table acct_reconcile;
drop table acct_reconcile_1;
alter table acct_reconcile_2 rename to acct_reconcile;
---------------------------------------

-- Add Borrower Credits Being Removed from Canceled Loans --

create table acct_reconcile_1 as
  select
    a.account_id,
    sum(b.borrower_credit_amount) as brwr_credits_from_cancels_same_day
from acct_reconcile a
left join acct_canceled_creds b on a.account_id = b.account_id
and a.src_dt = b.canceled_dt
where b.borrower_credit_amount is not null
group by a.account_id;

create table acct_reconcile_2 as
  select
    a.*,
    b.brwr_credits_from_cancels_same_day
from acct_reconcile a
left join acct_reconcile_1 b on a.account_id = b.account_id;

drop table acct_reconcile;
drop table acct_reconcile_1;
alter table acct_reconcile_2 rename to acct_reconcile;
---------------------------------------

-- Add Requested Withdrawals --

create table acct_reconcile_1 as
  select
    a.account_id,
    sum(b.amount) as withdrawals_requested
from acct_reconcile a
left join test_perpay_analytics.rec_wd_req b on a.account_id = b.account_id
and cast(b.wd_requested as date) = '2019-01-16'
group by a.account_id;

create table acct_reconcile_2 as
  select
    a.*,
    case when b.withdrawals_requested is null then 0 else b.withdrawals_requested end as withdrawals_requested
from acct_reconcile a
left join acct_reconcile_1 b on a.account_id = b.account_id;

drop table acct_reconcile;
drop table acct_reconcile_1;
alter table acct_reconcile_2 rename to acct_reconcile;
---------------------------------------

-- Add Processed Withdrawals --

create table acct_reconcile_1 as
  select
    a.account_id,
    sum(b.amount) as withdrawals_processed
from acct_reconcile a
left join test_perpay_analytics.rec_wd_paid b on a.account_id = b.account_id
and cast(b.wd_paid as date) = '2019-01-16'
group by a.account_id;

create table acct_reconcile_2 as
  select
    a.*,
    case when b.withdrawals_processed is null then 0 else b.withdrawals_processed end as withdrawals_processed
from acct_reconcile a
left join acct_reconcile_1 b on a.account_id = b.account_id;

drop table acct_reconcile;
drop table acct_reconcile_1;
alter table acct_reconcile_2 rename to acct_reconcile;
---------------------------------------

-- Begin Loan Balance Summary Reconciliation --
drop table acct_reconcile_v2;

create table acct_reconcile_v2 as
  select
    borrower_id,
    user_id,
    account_id,
    email,
    src_dt,
    case when prev_balance is null then 0 else prev_balance end as loanbal_s,
    case when curr_balance is null then 0 else curr_balance end as loanbal_e,
    case when acct_bal_s is null then 0 else acct_bal_s end as acctbal_s,
    case when acct_bal_e is null then 0
         else (acct_bal_e + withdrawals_requested) end as acctbal_e,
    case
      when revenue is null then 0
      else revenue - payoff_from_credit
      end as new_balances,
    case when ach_applied_to_loans is null then 0 else ach_applied_to_loans end as ach_applied_to_loans,
    case when cash_credits_to_ap_same_day is null then 0 else cash_credits_to_ap_same_day end as cash_credits_to_ap_same_day,
    case when refunds_to_loans is null then 0 else refunds_to_loans end as refunds_to_loans,
    case when refunds_to_credit is null then 0 else refunds_to_credit end as refunds_to_credit,
    case when creditcard_applied_to_credit is null then 0 else creditcard_applied_to_credit end as creditcard_applied_to_credit,
    case when ach_applied_to_credit is null then 0 else ach_applied_to_credit end as ach_applied_to_credit,
    payoff_from_credit
from acct_reconcile;

create table acct_reconcile_v2_1 as
  select
    borrower_id,
    user_id,
    account_id,
    email,
    src_dt,
    loanbal_s,
    new_balances,
    case
      when ach_applied_to_loans > 0 and cash_credits_to_ap_same_day > 0 then (ach_applied_to_loans - cash_credits_to_ap_same_day)
      else ach_applied_to_loans
      end as ach_applied_to_loans,
    case
      when (loanbal_s + new_balances - loanbal_e) = (acctbal_s - acctbal_e) then (acctbal_s - acctbal_e)
      else 0
      end as cash_bal_applied_to_loans,
    refunds_to_loans,
    refunds_to_credit,
    loanbal_e,
    acctbal_s,
    acctbal_e,
    creditcard_applied_to_credit,
    ach_applied_to_credit,
    payoff_from_credit
from acct_reconcile_v2;

create table acct_reconcile_v2_2 as
  select
    borrower_id,
    user_id,
    account_id,
    email,
    src_dt,
    loanbal_s,
    new_balances,
    ach_applied_to_loans,
    cash_bal_applied_to_loans,
    refunds_to_loans,
    loanbal_e,
    (loanbal_s - loanbal_e) + new_balances - ach_applied_to_loans - cash_bal_applied_to_loans - refunds_to_loans as loan_rec,
    acctbal_s,
    ach_applied_to_credit,
    creditcard_applied_to_credit,
    refunds_to_credit,
    payoff_from_credit,
    cash_bal_applied_to_loans as cash_bal_applied_to_loans_,
    acctbal_e,
    (acctbal_s - acctbal_e)
      + ach_applied_to_credit
      + creditcard_applied_to_credit
      + refunds_to_credit
      - payoff_from_credit
      - cash_bal_applied_to_loans
      as credit_rec
from acct_reconcile_v2_1;

drop table acct_reconcile_v2;
drop table acct_reconcile_v2_1;
alter table acct_reconcile_v2_2 rename to acct_reconcile_v2;
---------------------------------------




---------------------------------------
---------------------------------------
---------------------------------------
