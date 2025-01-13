INSERT INTO ds.md_exchange_rate_d( 
    data_actual_date
    ,data_actual_end_date
    ,currency_rk
	,reduced_cource
	,code_iso_num
)
SELECT distinct
    to_date(merd."DATA_ACTUAL_DATE", 'YYYY-mm-dd') as data_actual_date
	, to_date(merd."DATA_ACTUAL_END_DATE", 'YYYY-mm-dd') as data_actual_end_date
	, merd."CURRENCY_RK"
	, merd."REDUCED_COURCE"
	, merd."CODE_ISO_NUM"
FROM stage.md_exchange_rate_d merd
on conflict on constraint md_exchange_rate_d_pkey do update
	set data_actual_end_date=excluded.data_actual_end_date
		,reduced_cource=excluded.reduced_cource
		,code_iso_num=excluded.code_iso_num; 