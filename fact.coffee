---------------------------------------
---------------------------------------
---------------------------------------
--- Create Driver File Table ---

create table acct_fact as
select distinct
        a.id as borrower_id,
        a.user_id,
        a.account_id,
        d.first_name,
        d.last_name,
        d.email,
        d.city,
        d.state,
        d.zipcode,
        c.name,
        case when (position(':"weekly"' in b.pay_cycle_json)) > 0 then 'weekly'
        when (position(':"bi_weekly"' in b.pay_cycle_json)) > 0 then 'bi_weekly'
        when (position(':"semi_monthly"' in b.pay_cycle_json)) > 0 then 'semi_monthly'
        when (position(':"monthly"' in b.pay_cycle_json)) > 0 then 'monthly' end as pay_cycle,
        d.date_joined
from borrower a
left join job b on a.id = b.borrower_id
left join company c on c.id = b.company_id
left join "user" d on a.user_id = d.id
left join deposit f on a.account_id = f.account_id
where b.status = 'primary'
and f.status in ('valid')
and (f.content_type_id = 23 or f.content_type_id = 22 or f.content_type_id = 46)
order by a.account_id;

---------------------------------------
---------------------------------------
---------------------------------------
