CREATE TABLE IF NOT EXISTS ds.dm_f101_round_f
(
    from_date date,
    to_date date,
    chapter character(1) COLLATE pg_catalog."default",
    ledger_account character(5) COLLATE pg_catalog."default",
    characteristic character(1) COLLATE pg_catalog."default",
    balance_in_rub numeric(23,1),
    r_balance_in_rub numeric(23,1),
    balance_in_val numeric(23,1),
    r_balance_in_val numeric(23,1),
    balance_in_total numeric(23,1),
    r_balance_in_total numeric(23,1),
    turn_deb_rub numeric(23,1),
    r_turn_deb_rub numeric(23,1),
    turn_deb_val numeric(23,1),
    r_turn_deb_val numeric(23,1),
    turn_deb_total numeric(23,1),
    r_turn_deb_total numeric(23,1),
    turn_cre_rub numeric(23,1),
    r_turn_cre_rub numeric(23,1),
    turn_cre_val numeric(23,1),
    r_turn_cre_val numeric(23,1),
    turn_cre_total numeric(23,1),
    r_turn_cre_total numeric(23,1),
    balance_out_rub numeric(23,1),
    r_balance_out_rub numeric(23,1),
    balance_out_val numeric(23,1),
    r_balance_out_val numeric(23,1),
    balance_out_total numeric(23,1),
    r_balance_out_total numeric(23,1)
)
call dm.fill_f101_round_f('2018-02-01')
select * from dm.dm_f101_round_f
