
---------------------------------------
---------------------------------------
---------------------------------------
--- Create Receivables File ---

-- Pull approved loans --

create table acct_receivables as
select distinct
        borrower.id as borrower_id,
        borrower.account_id,
        loanstatus.loan_id as loan_id,
        loan.amount,
        loan.credit,
        loan.credit_line,
        loan.principal_balance,
        loan.created as loan_start_dt
from loanstatus
join loan on loanstatus.loan_id = loan.id
join borrower on borrower.id = loan.borrower_id
where loanstatus.status in ('approved')
and cast(loan.created as date) > '2016-04-06'
and cast(loanstatus.created as date) < '2019-01-17'
order by loanstatus.loan_id;

-- Find Status as of Previous and Current Day --

create table acct_receivables_1 as
  select
    a.loan_id,
    b.status,
    b.created,
    b.id
from acct_receivables a
left join loanstatus b on a.loan_id = b.loan_id
where cast(b.created as date) < '2019-01-17'
and b.status not in  ('disbursed');

create table acct_receivables_2 as
  select
    *
  from (select *,
rank() over (partition by loan_id order by created desc, id desc) as record_rank
from acct_receivables_1 order by loan_id, created desc, id desc) as ranked
where ranked.record_rank = 1;

create table acct_receivables_3 as
  select *
from acct_receivables_1
where cast(created as date) < '2019-01-16';

create table acct_receivables_4 as
  select
    *
  from (select *,
rank() over (partition by loan_id order by created desc, id desc) as record_rank
from acct_receivables_3 order by loan_id, created desc, id desc) as ranked
where ranked.record_rank = 1;

create table acct_receivables_5 as
  select
    a.*,
    b.status as prev_status,
    c.status as curr_status
from acct_receivables a
left join acct_receivables_4 b on a.loan_id = b.loan_id
left join acct_receivables_2 c on a.loan_id = c.loan_id;

drop table acct_receivables;
drop table acct_receivables_2;
drop table acct_receivables_3;
drop table acct_receivables_4;
alter table acct_receivables_5 rename to acct_receivables;
---------------------------------------

-- Find Last Good and Bad Status --

create table acct_receivables_2 as
  select *
from acct_receivables_1
where status in ('late','default','charged_off');

create table acct_receivables_3 as
  select
    *
  from (select *,
rank() over (partition by loan_id order by created desc, id desc) as record_rank
from acct_receivables_2 order by loan_id, created desc, id desc) as ranked
where ranked.record_rank = 1;

create table acct_receivables_4 as
  select *
from acct_receivables_1
where status in ('approved','repayment');

create table acct_receivables_5 as
  select
    *
  from (select *,
rank() over (partition by loan_id order by created desc, id desc) as record_rank
from acct_receivables_4 order by loan_id, created desc, id desc) as ranked
where ranked.record_rank = 1;

create table acct_receivables_6 as
  select
    a.*,
    b.created as last_bad_dt,
    c.created as last_good_dt,
    case when a.curr_status in ('late','default','charged_off')
      then cast(b.created as date) - cast(c.created as date)
      else null end as days_past_due
from acct_receivables a
left join acct_receivables_3 b on a.loan_id = b.loan_id
left join acct_receivables_5 c on a.loan_id = c.loan_id;

drop table acct_receivables;
drop table acct_receivables_1;
drop table acct_receivables_2;
drop table acct_receivables_3;
drop table acct_receivables_4;
drop table acct_receivables_5;
alter table acct_receivables_6 rename to acct_receivables;
---------------------------------------

-- Find Balance as of Previous and Current Day --

create table acct_receivables_1 as
  select
    a.loan_id,
    b.ending_balance,
    b.created,
    b.id
from acct_receivables a
left join payment_loanprincipalbalancehistory b on a.loan_id = b.loan_id
where cast(b.created as date) < '2019-01-17';

create table acct_receivables_2 as
  select distinct
    *
  from (select *,
rank() over (partition by loan_id order by created desc, id desc) as record_rank
from acct_receivables_1 order by loan_id, created desc, id desc) as ranked
where ranked.record_rank = 1;

create table acct_receivables_3 as
  select *
from acct_receivables_1
where cast(created as date) < '2019-01-16';

create table acct_receivables_4 as
  select distinct
    *
  from (select *,
rank() over (partition by loan_id order by created desc, id desc) as record_rank
from acct_receivables_3 order by loan_id, created desc, id desc) as ranked
where ranked.record_rank = 1;

create table acct_receivables_5 as
  select
    a.*,
    case
      when a.prev_status in ('charged_off') and b.ending_balance is null then a.principal_balance
      when a.prev_status not in ('approved','repayment','late','default','charged_off') then null
      else b.ending_balance
      end as prev_balance,
    case
      when a.curr_status in ('charged_off') and c.ending_balance is null then a.principal_balance
      else c.ending_balance
      end as curr_balance
from acct_receivables a
left join acct_receivables_4 b on a.loan_id = b.loan_id
left join acct_receivables_2 c on a.loan_id = c.loan_id;

drop table acct_receivables;
drop table acct_receivables_1;
drop table acct_receivables_2;
drop table acct_receivables_3;
drop table acct_receivables_4;
alter table acct_receivables_5 rename to acct_receivables;
---------------------------------------

--- Filter Out Closed Records ---

create table acct_receivables_1 as
  select
    a.*,
    case when principal_balance = 0 and prev_balance = 0 and curr_balance = 0 then 1
    when principal_balance = 0 and prev_balance is null and curr_balance is null then 1
    when prev_status in ('canceled') and curr_status in ('canceled') then 1
    when prev_status in ('complete') and curr_status in ('complete') then 1
    else 0 end as remove
from acct_receivables a;

create table acct_receivables_2 as
  select
    *,
    case when borrower_id is not null then '2019-01-16' end as src_dt
from acct_receivables_1
where remove = 0;

alter table acct_receivables_2 drop column remove;

drop table acct_receivables;
drop table acct_receivables_1;
alter table acct_receivables_2 rename to acct_receivables;
---------------------------------------

--- Push Rolled-up Receivables to Accounting Data Mart ---

create table acct_receivables_1 as
  select
    account_id,
    src_dt,
    sum(amount) as originations,
    sum(credit) as credits,
    count(loan_id) as loans_outstanding,
    min(days_past_due) as days_past_due,
    sum(prev_balance) as prev_balance,
    sum(curr_balance) as curr_balance
from acct_receivables
group by
    account_id,
    src_dt;

create table acct_receivables_rollup as
  select
    *,
    case when days_past_due is null then 'Current'
    when days_past_due >= 0 and days_past_due < 30 then 'B1'
    when days_past_due >= 30 and days_past_due < 60 then 'B2'
    when days_past_due >= 60 and days_past_due < 90 then 'B3'
    when days_past_due >= 90 and days_past_due < 120 then 'B4'
    when days_past_due >= 120 and days_past_due < 150 then 'B5'
    when days_past_due >= 150 and days_past_due < 180 then 'B6'
    when days_past_due >= 180 then 'ChargedOff' end as status
from acct_receivables_1;

drop table acct_receivables_1;

---------------------------------------
---------------------------------------
---------------------------------------
