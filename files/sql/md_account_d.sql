INSERT INTO ds.md_account_d( 
     data_actual_date
    ,data_actual_end_date
    ,account_rk
    ,account_number
    ,char_type
	,currency_rk
	,currency_code
)
SELECT distinct
      to_date(mad."DATA_ACTUAL_DATE", '%Y-%m-%d') 
    , to_date(mad."DATA_ACTUAL_END_DATE", '%Y-%m-%d')
    , mad."ACCOUNT_RK"
    , to_number(mad."ACCOUNT_NUMBER", '999999999999999999999')::BIGINT
	, mad."CHAR_TYPE"
	, mad."CURRENCY_RK"
	, mad."CURRENCY_CODE"
FROM stage.md_account_d mad
WHERE mad."DATA_ACTUAL_DATE" IS NOT NULL
   AND mad."DATA_ACTUAL_END_DATE" IS NOT NULL
   AND mad."ACCOUNT_RK" IS NOT NULL
   AND mad."ACCOUNT_NUMBER" IS NOT NULL
   AND mad."CHAR_TYPE" IS NOT NULL
   AND mad."CURRENCY_RK" IS NOT NULL
   AND mad."CURRENCY_CODE" IS NOT NULL
On conflict on constraint md_account_d_pkey do update
	set data_actual_end_date=excluded.data_actual_end_date
		,account_number=excluded.account_number
		,char_type=excluded.char_type
		,currency_rk=excluded.currency_rk
		,currency_code=excluded.currency_code; 



