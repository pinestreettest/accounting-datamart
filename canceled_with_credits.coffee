
---------------------------------------
---------------------------------------
---------------------------------------
--- Create Revenue File ---

-- Pull Loans That Hit Canceled With a Credit Applied --

create table acct_canceled_creds as
select  borrower.account_id,
        loanstatus.loan_id as loan_id,
        loan.amount,
        loan.credit,
        loan.credit_line,
        loan.principal_balance,
        cast(loan.created as date) as loan_start_dt,
        cast(loanstatus.created as date) as canceled_dt
from loanstatus
join loan on loanstatus.loan_id = loan.id
join borrower on borrower.id = loan.borrower_id
where loanstatus.status in ('canceled')
and cast(loan.created as date) > '2016-04-06'
and cast(loanstatus.created as date) = '2019-01-16'
and loan.credit is not null
and loan.credit > 0
order by loanstatus.loan_id;

create table acct_canceled_creds_1 as
  select
    a.*
from acct_canceled_creds a
left join loanstatus b on a.loan_id = b.loan_id
where b.status in ('awaiting_payment');

drop table acct_canceled_creds;
alter table acct_canceled_creds_1 rename to acct_canceled_creds;

alter table acct_canceled_creds add column src_dt date;
update acct_canceled_creds set src_dt = '2019-01-16';
---------------------------------------

-- Bring In Borrower Credits --

create table acct_canceled_creds_1 as
  select
    a.loan_id,
    a.account_id,
    b.starting_balance,
    b.ending_balance,
    b.amount_redeemed,
    b.credit_id,
    b.id
from acct_canceled_creds a
left join offers_borrowercreditamounthistory b on a.loan_id = b.loan_id;

create table acct_canceled_creds_2 as
  select
    *
  from
  (select *,
          rank() over (partition by loan_id, credit_id order by id) as record_rank
from acct_canceled_creds_1 order by loan_id, credit_id, id) as ranked
where ranked.record_rank = 1;

create table acct_canceled_creds_3 as
  select
    account_id,
    loan_id,
    credit_id,
    case
      when (starting_balance <> 0 and starting_balance is not null) then starting_balance
      when (ending_balance <> 0 and ending_balance is not null) then ending_balance
      when (amount_redeemed <> 0 and amount_redeemed is not null) then amount_redeemed
    end as borrower_credit_amount
from acct_canceled_creds_2
where credit_id is not null;

create table acct_canceled_creds_4 as
  select
    account_id,
    loan_id,
    sum(borrower_credit_amount) as borrower_credit_amount
from acct_canceled_creds_3
group by
    account_id,
    loan_id;

create table acct_canceled_creds_5 as
  select
    a.*,
    b.borrower_credit_amount
from acct_canceled_creds a
left join acct_canceled_creds_4 b on a.loan_id = b.loan_id;

create table acct_canceled_creds_6 as
  select
    *,
    case
      when borrower_credit_amount is null then credit
      when borrower_credit_amount is not null then credit - borrower_credit_amount
    end as cash_credit_amount
from acct_canceled_creds_5;

drop table acct_canceled_creds;
drop table acct_canceled_creds_1;
drop table acct_canceled_creds_2;
drop table acct_canceled_creds_3;
drop table acct_canceled_creds_4;
drop table acct_canceled_creds_5;
alter table acct_canceled_creds_6 rename to acct_canceled_creds;
---------------------------------------

---------------------------------------
---------------------------------------
---------------------------------------
