
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_DE`.`ZVS41_Inbound_credit_transfers`
      
    
    

    OPTIONS()
    as (
      SELECT
  d_counterparty_bank_accounts.FINANCIAL_INSTITUTION_COUNTRY_CODE AS PAYER_PSP_COUNTRY,
  COUNT(DISTINCT FAT.T_SOURCE_PK_ID) AS OUTBOUND_IBIS_PAYMENTS_TRX_COUNT,
  COALESCE(SUM(FAT.TRANSACTION_AMOUNT), 0) AS OUTBOUND_IBIS_PAYMENTS_AMOUNT_SUM_IN_EUR,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  '2024S1' AS PERIOD,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_ACCOUNT_TRANSACTIONS_DECRYPTED` AS FAT
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ACCOUNT_TRANSACTION_CURRENT` AS d_account_transaction ON FAT.T_D_ACCOUNT_TRANSACTION_DIM_KEY = d_account_transaction.T_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` AS d_ibis_account ON FAT.T_D_IBIS_ACCOUNT_DIM_KEY = d_ibis_account.T_DIM_KEY
INNER JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` AS d_bank_accounts ON d_ibis_account.T_D_BANK_ACCOUNT_DIM_KEY = d_bank_accounts.T_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` AS d_counterparty_bank_accounts ON d_counterparty_bank_accounts.T_DIM_KEY = FAT.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
WHERE FAT.TRANSACTION_DIRECTION = 'INBOUND'
  AND FAT.TRANSACTION_TYPE = 'REGULAR'
  AND d_account_transaction.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '2024-01-01', 'Etc/UTC'))
  AND d_account_transaction.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '2024-06-30', 'Etc/UTC'))
  AND d_account_transaction.TRANSACTION_STATUS IN ('RETURNED', 'SETTLED')
  AND d_ibis_account.ACCOUNT_TYPE = 'PAYMENT'
  AND FAT.TRANSACTION_BANK_FAMILY = 'RCDT'
  AND d_bank_accounts.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'DE'
GROUP BY 1
ORDER BY 1
    );
  