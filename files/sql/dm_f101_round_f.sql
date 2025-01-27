INSERT INTO ds.dm_f101_round_f(
  from_date
  ,to_date 
  ,chapter
  ,ledger_account
  ,characteristic
  ,balance_in_rub
  ,r_balance_in_rub
  ,balance_in_val
  ,r_balance_in_val
  ,balance_in_total
  ,r_balance_in_total
  ,turn_deb_rub
  ,r_turn_deb_rub
  ,turn_deb_val
  ,r_turn_deb_val
  ,turn_deb_total
  ,r_turn_deb_total
  ,turn_cre_rub
  ,r_turn_cre_rub
  ,turn_cre_val
  ,r_turn_cre_val
  ,turn_cre_total
  ,r_turn_cre_total
  ,balance_out_rub
  ,r_balance_out_rub
  ,balance_out_val
  ,r_balance_out_val
  ,balance_out_total
  ,r_balance_out_total
)
SELECT 
  to_date(f.from_date, '%Y-%m-%d') as from_date,
  f.to_date::date as to_date,
  chapter::varchar(1) as chapter,
  ledger_account::varchar(5) as ledger_account,
  characteristic::varchar(1) as characteristic,
  f.balance_in_rub::numeric(23,1) as to_balance_in_rub,
  f.r_balance_in_rub,
  f.balance_in_val::numeric(23,1) as balance_in_val,
  f.r_balance_in_val,
  f.balance_in_total::numeric(23,1) as balance_in_total,
  f.r_balance_in_total,
  f.turn_deb_rub::numeric(23,1) as turn_deb_rub,
  f.r_turn_deb_rub,
  f.turn_deb_val::numeric(23,1) as turn_deb_val,
  f.r_turn_deb_val,
  f.turn_deb_total::numeric(23,1) as turn_deb_total,
  f.r_turn_deb_total,
  f.turn_cre_rub::numeric(23,1) as turn_cre_rub,
  f.r_turn_cre_rub,
  f.turn_cre_val::numeric(23,1) as turn_cre_val,
  f.r_turn_cre_val,
  f.turn_cre_total::numeric(23,1) as turn_cre_total,
  f.r_turn_cre_total,
  f.balance_out_rub::numeric(23,1) as balance_out_rub,
  f.r_balance_out_rub,
  f.balance_out_val::numeric(23,1) as balance_out_val,
  f.r_balance_out_val,
  f.balance_out_total::numeric(23,1) as balance_out_total,
  f.r_balance_out_total
FROM stage.dm_f101_round_f f;

