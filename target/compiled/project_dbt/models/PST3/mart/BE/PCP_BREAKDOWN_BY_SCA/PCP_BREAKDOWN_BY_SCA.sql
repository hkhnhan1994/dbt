SELECT
    ma.COUNT_AREA,
    ma.TRMNL_LCTN,
    ma.INTTN_CHNNL,
    ma.RMT_INTTN,
    ma.PYMNT_SCHM,
    ma.SCA,
    ma.FRD_TYP,
    COUNT(ma.TRANSACTION_PUBLIC_IDENTIFIER) AS COUNT_OF_TRANSACTION_PUBLIC_IDENTIFIER,
    SUM(ma.AMOUNT) AS SUM_OF_AMOUNT,
    ma.Period,
    ma.Period_begin_date,
    ma.Period_end_date,
    ma.LOAD_TIMESTAMP,
FROM `pj-bu-dw-data-sbx`.`dev_dm_pst3_BE`.`PCP_MERCHANT_ACCT_TRX` as ma
GROUP BY 1,2,3,4,5,6,7,10,11,12,13