-- create schema dm;
create table if not exists dm.dm_f101_round_f(
  from_date date
  ,to_date date
  ,chapter char(1)
  ,ledger_account char(5)
  ,characteristic char(1)
  ,balance_in_rub numeric(23,0)
  ,r_balance_in_rub numeric(23,0)
  ,balance_in_val numeric(23,0)
  ,r_balance_in_val numeric(23,0)
  ,balance_in_total numeric(23,0)
  ,r_balance_in_total numeric(23,0)
  ,turn_deb_rub numeric(23,0)
  ,r_turn_deb_rub numeric(23,0)
  ,turn_deb_val numeric(23,0)
  ,r_turn_deb_val numeric(23,0)
  ,turn_deb_total numeric(23,0)
  ,r_turn_deb_total numeric(23,0)
  ,turn_cre_rub numeric(23,0)
  ,r_turn_cre_rub numeric(23,0)
  ,turn_cre_val numeric(23,0)
  ,r_turn_cre_val numeric(23,0)
  ,turn_cre_total numeric(23,0)
  ,r_turn_cre_total numeric(23,0)
  ,balance_out_rub numeric(23,0)
  ,r_balance_out_rub numeric(23,0)
  ,balance_out_val numeric(23,0)
  ,r_balance_out_val numeric(23,0)
  ,balance_out_total numeric(23,0)
  ,r_balance_out_total numeric(23,0)
);
--Витрина об остатках и оборотах за отчетный период по форме 101 за январь 2018 года
--сгруппированных по балансовым счетам второго порядка
create or replace procedure dm.fill_f101_round_f(i_OnDate date)
as $$
declare
start_log timestamp;
end_log timestamp;
FD date;
TD date;
begin
  start_log = (select now());
--первый/последний	 день отчетного периода
  FD = i_OnDate - interval '1 month';
  TD = i_OnDate - interval '1 day';
 --для построения отчета за одни и те же даты отчетного периода
delete from dm.dm_f101_round_f where FD = from_date and TD = to_date;
insert into dm.dm_f101_round_f(
  from_date
  ,to_date
  ,chapter
  ,ledger_account
  ,characteristic
  ,balance_in_rub
  ,balance_in_val
  ,balance_in_total
  ,turn_deb_rub
  ,turn_deb_val
  ,turn_deb_total
  ,turn_cre_rub
  ,turn_cre_val
  ,turn_cre_total
  ,balance_out_rub
  ,balance_out_val
  ,balance_out_total
)
--балансовый счет второго порядка – это первые 5 символов номера счета
with led_acc as (
  select 
    md_ad.account_rk
--характеристика счета
	,md_ad.char_type
	,(left(account_number, 5))::int as ledger_account
 from ds.md_account_d md_ad
 where md_ad.data_actual_date >= FD AND md_ad.data_actual_date <= TD
)
  ,rub_acc as (
	select *
	from ds.md_account_d mdad
	where currency_code = '810' 
	  or currency_code = '643'
)
--баланс счета за день до начала отчетного периода
--по витрине остатков
  , balance_in_val_f as(
	 select *
	 from dm.dm_account_balance_f dm_abf
     where dm_abf.oper_date = (FD - interval '1 day')
)
--баланс счета в последний день отчетного периода
  ,balance_l as(
    select *
    from dm.dm_account_balance_f dm_abf
    where dm_abf.oper_date = TD
)
--обороты в рублях за все дни отчетного периода
  ,turnovers as(
  	select *
	from ds.dm_account_turnover_f dm_atf
	where dm_atf.oper_date 
	  between FD and TD 
)
select
  FD
  ,TD
--глава из справочника балансовых счетов
  ,md_las.chapter
  ,la.ledger_account
  ,la.char_type as characteristic
  ,sum(case
    	 when b_invf.account_rk in (select account_rk from rub_acc)
		 then b_invf.balance_out_rub
		 else 0
	   end) as balance_in_rub --сумма остатков в рублях за день, предшествующему первому дню отчетного периода
  ,sum(case
    	 when b_invf.account_rk not in (select account_rk from rub_acc)
		 then b_invf.balance_out_rub
		 else 0
	   end) as balance_in_val --сумма остатков в рублях за день, предшествующему первому дню отчетного периода для всех счетов, кроме рублевых
  ,sum(b_invf.balance_out_rub) as balance_in_total --сумма остатков в рублях за день, предшествующему первому дню отчетного периода для всех счетов;
  ,sum(case 
  		  when t.account_rk in (select account_rk from rub_acc)
		  then t.reduced_cource_debet
		  else 0
		end) as turn_deb_rub -- сумма дебетовых оборотов в рублях
  ,sum(case
  		  when t.account_rk not in (select account_rk from rub_acc)
		  then t.reduced_cource_debet
		  else 0
		end) as turn_deb_val
  ,sum(t.reduced_cource_debet) as turn_deb_total
  ,sum(case
  		  when t.account_rk in (select account_rk from rub_acc)
		  then t.reduced_cource_credit
		  else 0
		end) as turn_cre_rub
  ,sum(case
  		  when t.account_rk not in (select account_rk from rub_acc)
		  then t.reduced_cource_credit
		  else 0
		end) as turn_cre_val
  ,sum(t.reduced_cource_credit) as turn_cre_total
  ,sum(case
    	 when bl.account_rk in (select account_rk from rub_acc)
		 then bl.balance_out_rub
		 else 0
	   end) as balance_out_rub --сумма остатков в рублях за день за последний день отчетного периода для рублевых счетов;
  ,sum(case
    	 when bl.account_rk not in (select account_rk from rub_acc)
		 then bl.balance_out_rub
		 else 0
	   end) as balance_out_val
  ,sum(bl.balance_out_rub) as balance_out_total --сумма остатков для всех счетов

from led_acc la
join ds.md_ledger_account_s md_las using (ledger_account)
join rub_acc using (account_rk)
join balance_in_val_f b_invf using (account_rk)
join balance_l bl using (account_rk)
left join turnovers t using (account_rk)
group by FD
  		 ,TD
  		 ,md_las.chapter --глава из справочника балансовых счетов
 		 ,la.ledger_account  --балансовый счет второго порядка
 		 ,la.char_type;  --глава из справочника балансовых счетов
end_log = (select now());
INSERT INTO log.logt (execution_datetime, event_datetime, event_name)
VALUES (start_log, end_log, 'fill_f101_round_f '||i_OnDate);
end $$
language plpgsql;

call dm.fill_f101_round_f('2018-02-01');
truncate dm.dm_f101_round_f;
select * from dm.dm_f101_round_f;






