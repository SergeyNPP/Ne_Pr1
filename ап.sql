create table if not exists ds.dm_account_balance_f(
  oper_date date not null
  ,account_rk numeric not null
  ,currency_rk numeric
  ,balance_out bigint
  ,balance_out_rub bigint
);

with exchange_rate as (
  select merd.reduced_cource, merd.currency_rk
  from ds.md_exchange_rate_d merd
  where '2017-12-31' between merd.data_actual_date and merd.data_actual_end_date
)
insert into ds.dm_account_balance_f
  select 
    ft_bf.on_date
	,ft_bf.account_rk
	,ft_bf.currency_rk
	,ft_bf.balance_out
	,ft_bf.balance_out*coalesce(er.reduced_cource, 1)
  from ds.ft_balance_f ft_bf
  left join exchange_rate er using(currency_rk);


truncate ds.dm_account_turnover_f;
do
$$
begin
  for i in 1..31 loop
    call ds.dm_account_turnover_f(('2018-01-'|| i)::date);
  end loop;
end
$$ language plpgsql;
select * from ds.dm_account_turnover_f;

-- begin
--   truncate ds.dm_account_turnover_f;
--   call ds.fill_account_turnover_f('2018-01-08');
--   select * from ds.dm_account_turnover_f;
--   call ds.fill_account_turnover_f('2018-01-09');
--   select * from ds.dm_account_turnover_f;
--   call ds.fill_account_turnover_f('2018-01-10');
--   select * from ds.dm_account_turnover_f;
-- rollback;
-- commit;

truncate ds.dm_account_turnover_f;

create or replace procedure ds.full_account_balance_out_f(i_OnDate date)
as 
$$
declare
start_log timestamp;
end_log timestamp;
begin
  start_log = select now();
delete from ds.dm_account_balance_f where oper_date = i_OnDate;

with actual_acc as (
  select 
    mdad.account_rk
	,mdad.char_type
  from ds.md_account_d mdad
)
  ,current_balance as (
  select *
  from ds.dm_account_turnover_f dm_atf
  where dm_atf.oper_date = i_OnDate
)
insert into ds.dm_account_balance_f
select
  i_OnDate as oper_date
  ,previous.account_rk as account_rk
  ,previous.currency_rk as currency_rk
  ,case 
    when act_a.char_type = 'А' 
	  then coalesce(previous.balance_out, 0)+coalesce(cb.debet_amount,0)-coalesce(cb.credit_amount,0)
    else  
	  coalesce(previous.balance_out, 0)-coalesce(cb.debet_amount,0)+coalesce(cb.credit_amount,0)
  end as balance_out
 ,case 
    when act_a.char_type = 'А' 
	  then coalesce(previous.balance_out_rub, 0)+coalesce(cb.reduced_cource_debet,0)-coalesce(cb.reduced_cource_credit,0)
    else  
	  coalesce(previous.balance_out_rub, 0)-coalesce(cb.reduced_cource_debet,0)+coalesce(cb.reduced_cource_credit,0)
  end as balance_out_rub

from ds.dm_account_balance_f previous
join actual_acc act_a using(account_rk)
left join current_balance cb using(account_rk)
where previous.oper_date = i_OnDate - interval '1 day';

end_log = select now();
INSERT INTO log.logt (execution_datetime, event_datetime, event_name)
VALUES (start_log, end_log, 'log_dm_account_balance_f');
end
$$ language plpgsql;


  select * from ds.dm_account_balance_f;

truncate ds.dm_account_balance_f;

  begin;
  call ds.full_account_balance_out_f('2018-01-01');
  select * from ds.dm_account_balance_f
  order by account_rk, oper_date;
  do
  $$
  begin
    for i in 1..31 loop
	  call ds.full_account_balance_out_f(('2018-01-'||i)::date);
	end loop;
  end
  $$ language plpgsql;
  
  select * from ds.dm_account_balance_f
  order by account_rk;

  call ds.fill_account_balance_f('2018-01-11');
  select * from ds.dm_account_balance_f dabf
  where dabf.on_date between '2018-01-10' and '2018-01-11';

  call ds.fill_account_balance_f('2018-01-12');
  select * from ds.dm_account_balance_f dabf
  where dabf.on_date between '2018-01-11' and '2018-01-12';

  rollback;
  commit;

  


