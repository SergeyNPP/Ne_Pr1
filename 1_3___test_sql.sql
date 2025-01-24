with led_acc as (
  select 
    md_ad.account_rk
	,md_ad.char_type
	,(left(account_number, 5))::int as ledger_account
 from ds.md_account_d md_ad
 where md_ad.data_actual_date >= '2018-01-01' AND md_ad.data_actual_date <= '2018-01-31'
)
  ,rub_acc as (
	select *
	from ds.md_account_d mdad
	where currency_code = '810' 
	  or currency_code = '643'
)
--баланс счета за день до начала отчетного периода
  , balance_in_val_f as(
	 select *
	 from dm.dm_account_balance_f dm_abf
     where dm_abf.oper_date = '2018-01-01'::timestamp(0) - interval '1 day'
)
--баланс счета в последний день отчетного периода
  ,balance_l as(
    select *
    from dm.dm_account_balance_f dm_abf
    where dm_abf.oper_date = '2018-01-31'::timestamp(0)
)
  ,turnovers as(
  	select *
	from ds.dm_account_turnover_f dm_atf
	where dm_atf.oper_date 
	  between '2018-01-01'::timestamp(0) and '2018-01-31'::timestamp(0) 
)

select
  '2018-01-01'::timestamp(0) as FD
  ,'2018-01-31'::timestamp(0) as TD
  ,md_las.chapter
  ,la.ledger_account
  ,la.char_type as characteristic
  ,sum(case
    	 when b_invf.account_rk in (select account_rk from rub_acc)
		 then b_invf.balance_out_rub
		 else 0
	   end) as balance_in_rub
  ,sum(case
    	 when b_invf.account_rk not in (select account_rk from rub_acc)
		 then b_invf.balance_out_rub
		 else 0
	   end) as balance_in_val
  ,sum(b_invf.balance_out_rub) as balance_in_total
  ,sum(case
  		  when t.account_rk in (select account_rk from rub_acc)
		  then t.reduced_cource_debet
		  else 0
		end) as turn_deb_rub
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
	   end) as balance_out_rub
  ,sum(case
    	 when bl.account_rk not in (select account_rk from rub_acc)
		 then bl.balance_out_rub
		 else 0
	   end) as balance_out_val
  ,sum(bl.balance_out_rub) as balance_out_total

from led_acc la
left join ds.md_ledger_account_s md_las using (ledger_account)
join rub_acc using (account_rk)
left join balance_in_val_f b_invf using (account_rk)
join balance_l bl using (account_rk)
left join turnovers t using (account_rk)
group by FD
  		 ,TD
  		 ,md_las.chapter
 		 ,la.ledger_account
 		 ,la.char_type;