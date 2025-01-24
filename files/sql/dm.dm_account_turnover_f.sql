-- CREATE TABLE dm.dm_account_turnover_f(
--   oper_date         DATE
--   ,account_rk        int
--   , debet_amount      NUMERIC(19,2)
--   , reduced_cource_debet bigint
--   , credit_amount     NUMERIC(19,2)
--   ,reduced_cource_credit bigint
--  );
--Создание функции dm_account_turnover_f 
--вывод проводок по счетам за дату i_OnDate
--ковертация в рубли дебетового и кредитного счетов
create or replace procedure dm.dm_account_turnover_f(i_OnDate date)
AS $$
declare 
start_log timestamp;
end_log timestamp;
begin
start_log = (select now());
delete from dm.dm_account_turnover_f where oper_date = i_OnDate;
with sum_cred_p as (
  SELECT 
    sum(credit_amount) as credit_amount
    , credit_account_rk as account_rk
  FROM ds.ft_posting_f
  where oper_date = i_OnDate
  group by credit_account_rk
  )
,reduced_cource as (
  SELECT 
    reduced_cource
	,currency_rk
  FROM ds.md_exchange_rate_d
  where i_OnDate 
    between data_actual_date 
	and data_actual_end_date
  )
,sum_deb_p as (
  SELECT 
    sum(debet_amount ) as debet_amount 
    ,debet_account_rk as account_rk
  FROM ds.ft_posting_f
  where oper_date = i_OnDate
  group by debet_account_rk
  )
insert into dm.dm_account_turnover_f(
  oper_date
  ,account_rk
  ,debet_amount
  ,reduced_cource_debet
  ,credit_amount
  ,reduced_cource_credit
)

SELECT
  i_OnDate as oper_date
  ,account_rk
  ,sdp.debet_amount as debet_amount
  ,(sdp.debet_amount * coalesce (reduced_cource, 1)) AS reduced_cource_debet
  ,scp.credit_amount as credit_amount
  ,(scp.credit_amount * coalesce (reduced_cource, 1)) AS reduced_cource_credit
FROM sum_deb_p sdp
full join sum_cred_p scp using (account_rk)
left join ds.md_account_d md_ad using (account_rk)
left join reduced_cource rc using (currency_rk);

end_log = (select now());
  
INSERT INTO log.logt (execution_datetime, event_datetime, event_name)
VALUES (start_log, end_log, 'log_dm_account_turnover_f '|| (i_OnDate));
end
$$
LANGUAGE plpgsql;
---
call dm.dm_account_turnover_f('2018-01-09');
select * from dm.dm_account_turnover_f;
truncate dm.dm_account_turnover_f;
--


--создание таблицы dm_account_balance_f в схеме dm
create table if not exists dm.dm_account_balance_f(
  oper_date date not null
  ,account_rk numeric not null
  ,currency_rk numeric
  ,balance_out bigint
  ,balance_out_rub bigint
);
--Расчет суммы за 2017-12-31 по заданию. 
with exchange_rate as (
  select merd.reduced_cource, merd.currency_rk
  from ds.md_exchange_rate_d merd
  where '2017-12-31' between merd.data_actual_date and merd.data_actual_end_date
)
insert into dm.dm_account_balance_f
  select 
    ft_bf.on_date
	,ft_bf.account_rk
	,ft_bf.currency_rk
	,ft_bf.balance_out
	,ft_bf.balance_out*coalesce(er.reduced_cource, 1)
  from ds.ft_balance_f ft_bf
  left join exchange_rate er using(currency_rk);

--
select * from dm.dm_account_turnover_f;
--

do
$$
begin
  for i in 1..31 loop
    call dm.dm_account_turnover_f(('2018-01-'|| i)::date);
  end loop;
end
$$ language plpgsql;

--
select * from dm.dm_account_turnover_f;
truncate dm.dm_account_turnover_f;
--

-- begin
--   truncate dm.dm_account_turnover_f;
--   call ds.fill_account_turnover_f('2018-01-08');
--   select * from dm.dm_account_turnover_f;
--   call ds.fill_account_turnover_f('2018-01-09');
--   select * from dm.dm_account_turnover_f;
--   call ds.fill_account_turnover_f('2018-01-10');
--   select * from dm.dm_account_turnover_f;
-- rollback;
-- commit;


--Процедура нахождения остатков на счетах на текущую дату
create or replace procedure dm.fill_account_balance_out_f(i_OnDate date)
as 
$$
declare
start_log timestamp;
end_log timestamp;
begin
  start_log = (select now());
delete from dm.dm_account_balance_f where oper_date = i_OnDate;

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
insert into dm.dm_account_balance_f
select
  i_OnDate as oper_date
  ,previous.account_rk as account_rk
  ,previous.currency_rk as currency_rk
  ,(case
    when act_a.char_type = 'А' 
	  then coalesce(previous.balance_out, 0)+coalesce(cb.debet_amount,0)-coalesce(cb.credit_amount,0)
    else  
	  coalesce(previous.balance_out, 0)-coalesce(cb.debet_amount,0)+coalesce(cb.credit_amount,0)
  end) as balance_out
 ,(case 
   when act_a.char_type = 'А' 
	  then coalesce(previous.balance_out_rub, 0)+coalesce(cb.reduced_cource_debet,0)-coalesce(cb.reduced_cource_credit,0)
    else  
	  coalesce(previous.balance_out_rub, 0)-coalesce(cb.reduced_cource_debet,0)+coalesce(cb.reduced_cource_credit,0)
  end) as balance_out_rub

from dm.dm_account_balance_f previous
join actual_acc act_a using(account_rk)
left join current_balance cb using(account_rk)
where previous.oper_date = i_OnDate - interval '1 day';

end_log = (select now());
INSERT INTO log.logt (execution_datetime, event_datetime, event_name)
VALUES (start_log, end_log, 'log_dm_account_balance_f '||i_OnDate);
end
$$ language plpgsql;

--Заполним таблицу остатков за весь месяц
do
  $$
  begin
    for i in 1..31 loop
	  call dm.fill_account_balance_out_f(('2018-01-'||i)::date);
	end loop;
  end
  $$ language plpgsql;

select * from dm.dm_account_balance_f;

truncate dm.dm_account_balance_f;

  begin;
  call dm.fill_account_balance_out_f('2018-01-01');
  select * from dm.dm_account_balance_f
  order by account_rk, oper_date;
  
  
  select * from dm.dm_account_balance_f
  order by account_rk;

  call ds.fill_account_balance_f('2018-01-11');
  select * from dm.dm_account_balance_f dabf
  where dabf.on_date between '2018-01-10' and '2018-01-11';

  call ds.fill_account_balance_f('2018-01-12');
  select * from dm.dm_account_balance_f dabf
  where dabf.on_date between '2018-01-11' and '2018-01-12';

  rollback;
  commit;

  



