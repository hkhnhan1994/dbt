WITH country_codes AS (
SELECT *
FROM UNNEST (['FR','DE','AT','BE','BG','CY','HR','DK','ES','EE','FI','GR','HU','IE','IS','IT','LV','LI','LT','LU','MT','NO','NL','PL','PT','CZ','RO','SK','SI','SE']) as code
),
DM as (
SELECT
  deb.FINANCIAL_INSTITUTION_COUNTRY_CODE AS payer_psp_country,
  COUNT(*) AS inbound_ibis_payments_trx_count,
  ROUND(COALESCE(SUM(ftr.TRANSACTION_AMOUNT), 0.00), 2) AS inbound_ibis_payments_amount_sum_in_EUR,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_ACCOUNT_TRANSACTIONS_DECRYPTED` ftr
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` iacc
  ON ftr.T_D_IBIS_ACCOUNT_DIM_KEY = iacc.T_DIM_KEY
  AND iacc.ACCOUNT_TYPE = 'PAYMENT'
JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` cred
  ON iacc.T_D_BANK_ACCOUNT_DIM_KEY = cred.T_DIM_KEY
  AND cred.FINANCIAL_INSTITUTION_COUNTRY_CODE =  'FR'
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` deb
  ON deb.T_DIM_KEY = ftr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ACCOUNT_TRANSACTION_CURRENT` dtr
  ON ftr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
  AND dtr.TRANSACTION_STATUS  IN ('SETTLED', 'RETURNED')
  AND cred.FINANCIAL_INSTITUTION_COUNTRY_CODE =  'FR'
  AND ftr.TRANSACTION_BANK_FAMILY = 'RCDT'
  AND iacc.ACCOUNT_TYPE = 'PAYMENT'
  AND dtr.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))
  AND dtr.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))
WHERE ftr.TRANSACTION_DIRECTION = 'INBOUND'
  AND ftr.transaction_type  = 'REGULAR'
GROUP BY deb.FINANCIAL_INSTITUTION_COUNTRY_CODE
ORDER BY payer_psp_country ASC
)
SELECT
   c.code,
  DM.*,
  (SELECT SUM(inbound_ibis_payments_trx_count) FROM DM WHERE payer_psp_country != 'FR') as total_trx_count_without_FR,
  (SELECT SUM(inbound_ibis_payments_amount_sum_in_EUR) FROM DM WHERE payer_psp_country != 'FR') as total_amount_sum_without_FR,
  CURRENT_TIMESTAMP AS Load_timestamp,
  ""  AS Period,
FROM DM
FULL OUTER JOIN country_codes as c on c.code = DM.payer_psp_country