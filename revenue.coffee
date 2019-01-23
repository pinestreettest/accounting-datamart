
---------------------------------------
---------------------------------------
---------------------------------------
--- Create Revenue File ---

-- Pull approved loans --

create table acct_revenue as
select  borrower.account_id,
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
and cast(loanstatus.created as date) = '2019-01-16'
order by loanstatus.loan_id;



---------------------------------------
---------------------------------------
---------------------------------------
