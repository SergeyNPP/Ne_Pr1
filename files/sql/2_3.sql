--Прототип витрины
SELECT a.account_rk,
	   COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
	   a.department_rk,
	   ab.effective_date,
	   ab.account_in_sum,
	   ab.account_out_sum
FROM rd.account a
LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd


begin;
--Если текущий остаток верный, завтра счет неверно
select 
  t.account_rk
  ,t.effective_date
  ,t.account_out_sum
  ,f.account_in_sum as future_account_in_sum
  ,case 
    when t.account_out_sum <> f.account_in_sum
    then t.account_out_sum
	else f.account_in_sum
  end as correct_in_sum_future
from rd.account_balance t
join rd.account_balance f 
  on t.account_rk = f.account_rk
  and f.effective_date = t.effective_date + interval '1 day';

--Если текущий счет неверно, вчера остаток верный
select 
  t.account_rk
  ,t.effective_date
  ,t.account_in_sum
  ,p.account_out_sum as previos_account_out_sum
  ,case 
    when t.account_in_sum <> p.account_out_sum
    then p.account_out_sum
	else t.account_in_sum
  end as correct_in_sum_today
from rd.account_balance t
join rd.account_balance p 
  on t.account_rk = p.account_rk
  and p.effective_date = t.effective_date - interval '1 day';

--Подготовить запрос, который поправит данные в таблице rd.account_balance используя уже имеющийся запрос из п.1

begin; 
--Запрос на изменение account_in_sum
update rd.account_balance t
set account_in_sum = (
--Если текущий счет неверно, вчера остаток верный
  select p.account_out_sum
  from rd.account_balance p
  where t.account_rk = p.account_rk
    and t.account_in_sum <> p.account_out_sum
    and t.effective_date = p.effective_date + interval '1 day')
where 
--пройдемся по всему списку, как по массиву
  exists(
 -- если выполняется то запишем в SET и начнем для 2 строки и т.д.
 select 1
  from rd.account_balance p
  where t.account_rk = p.account_rk
    and t.account_in_sum <> p.account_out_sum
    and t.effective_date = p.effective_date + interval '1 day');
select * from rd.account_balance
order by account_rk, effective_date
commit;
rollback;

--Написать процедуру по аналогии с задание 2.2 для перезагрузки данных в витрину
begin;
CREATE OR REPLACE PROCEDURE load_account_balance_turnover()
AS 
$$
BEGIN
-- Очищаем таблицу витрины
TRUNCATE TABLE dm.account_balance_turnover;
-- Загружаем данные из источников
INSERT INTO dm.account_balance_turnover (account_rk, currency_name, department_rk, effective_date, account_in_sum, account_out_sum)
SELECT a.account_rk
	   ,COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name
	   ,a.department_rk
	   ,ab.effective_date
	   ,CASE 
            WHEN ab.account_in_sum <> p.account_out_sum 
            THEN p.account_out_sum 
            ELSE ab.account_in_sum 
        END AS account_in_sum
	   ,ab.account_out_sum
FROM rd.account a
LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
left JOIN rd.account_balance p ON ab.account_rk = p.account_rk 
  and ab.effective_date = p.effective_date + INTERVAL '1 day'
LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd;
END;
$$ LANGUAGE plpgsql;
call load_account_balance_turnover()
select * from dm.account_balance_turnover;
commit;
rollback;
