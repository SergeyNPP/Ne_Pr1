 INSERT INTO ds.md_currency_d(
      currency_rk
    , data_actual_date
    , data_actual_end_date
    , currency_code
    , code_iso_char
)
SELECT distinct
    mcd."CURRENCY_RK"
    , to_date(mcd."DATA_ACTUAL_DATE", '%Y-%m-%d') as data_actual_date
    , to_date(mcd."DATA_ACTUAL_END_DATE", '%Y-%m-%d') as data_actual_end_date
    , mcd."CURRENCY_CODE" 
	, mcd."CODE_ISO_CHAR" 
FROM stage.md_currency_d mcd
WHERE mcd."CURRENCY_RK" IS NOT NULL
   AND mcd."DATA_ACTUAL_DATE" IS NOT NULL
On conflict on constraint md_currency_d_pkey do update
	set data_actual_end_date=excluded.data_actual_end_date
		,currency_code=excluded.currency_code
		,code_iso_char=excluded.code_iso_char; 