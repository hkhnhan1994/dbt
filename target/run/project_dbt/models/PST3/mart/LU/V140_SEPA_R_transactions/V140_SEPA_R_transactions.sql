
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_LU`.`V140_SEPA_R_transactions`
      
    
    

    OPTIONS()
    as (
      SELECT
     'SEPA' as paymentScheme,
   case when (ftr.TRANSACTION_DIRECTION = "INBOUND") then 'CPSP'
        when (ftr.TRANSACTION_DIRECTION = "OUTBOUND") then 'DPSP'
        end RoleofReporting,
   'CORP' as customerCategory,
   case when ( (ftr.TRANSACTION_DIRECTION = "INBOUND" and substr(bank.BANK_ACCOUNT_NUMBER,5,3) = substr(counter.BANK_ACCOUNT_NUMBER,5,3) and counter.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LU')
        or (ftr.TRANSACTION_DIRECTION = "OUTBOUND" and substr(counter.BANK_ACCOUNT_NUMBER,5,3) = substr(bank.BANK_ACCOUNT_NUMBER,5,3) and bank.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LU')) then 'ONUS'
              else 'PSPN'
         end settlementChannel,
  'SEPA Credit Transfer' as R_TransactionType,
     case when  (ftr.TRANSACTION_DIRECTION = "INBOUND") then counter.FINANCIAL_INSTITUTION_COUNTRY_CODE
          when (ftr.TRANSACTION_DIRECTION = "OUTBOUND") then BANK.FINANCIAL_INSTITUTION_COUNTRY_CODE
          end creditorPspCountry,
     case when (ftr.TRANSACTION_DIRECTION = "OUTBOUND") then BANK.FINANCIAL_INSTITUTION_COUNTRY_CODE
          when (ftr.TRANSACTION_DIRECTION = "INBOUND") then counter.FINANCIAL_INSTITUTION_COUNTRY_CODE
          end debtorPspCountry,
     ftr.transaction_currency as currency,
     'VOLU' as metric,
     count(*) as reportedAmount,
     CURRENT_TIMESTAMP AS Load_timestamp,
     ""  AS Period,

  FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_ACCOUNT_TRANSACTIONS_DECRYPTED` as ftr
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ACCOUNT_TRANSACTION_CURRENT` as dtr  ON ftr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` AS ibis ON ftr.T_D_IBIS_ACCOUNT_DIM_KEY = ibis.T_DIM_KEY
  INNER JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` as bank ON ibis.T_D_BANK_ACCOUNT_DIM_KEY = bank.T_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` as counter ON counter.T_DIM_KEY = ftr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
  WHERE
      ibis.ACCOUNT_TYPE = 'PAYMENT'
  AND ftr.TRANSACTION_TYPE IN ('RETURN')
  AND dtr.TRANSACTION_STATUS IN ('SETTLED', 'RETURNED')
  AND dtr.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '2024-10-01 10:34:13.824572', 'Etc/UTC'))   -- +01 for winter time, +02 for summer time
  AND dtr.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '2024-10-31 10:34:13.824572', 'Etc/UTC'))  -- like timezone('UTC', to_timestamp('2023-12-31 UTC+01', 'YYYY-MM-DD ""UTC""TZH') + interval '1 day') -- +01 for winter time, +02 for summer time
  AND ( bank.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LU'
      or  COUNTER.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LU')
  group by 1,2,3,4,5,6,7,8

  union all

  SELECT
        'SEPA' as paymentScheme,
      case when (ftr.TRANSACTION_DIRECTION = "INBOUND") then 'CPSP'
            when (ftr.TRANSACTION_DIRECTION = "OUTBOUND") then 'DPSP'
            end RoleofReporting,
      'CORP' as customerCategory,
      case when ( (ftr.TRANSACTION_DIRECTION = "INBOUND" and substr(bank.BANK_ACCOUNT_NUMBER,5,3) = substr(counter.BANK_ACCOUNT_NUMBER,5,3) and counter.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LU')
            or (ftr.TRANSACTION_DIRECTION = "OUTBOUND" and substr(counter.BANK_ACCOUNT_NUMBER,5,3) = substr(bank.BANK_ACCOUNT_NUMBER,5,3) and bank.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LU')) then 'ONUS'
                  else 'PSPN'
            end settlementChannel,
      'SEPA Credit Transfer' as R_TransactionType,
        case when  (ftr.TRANSACTION_DIRECTION = "INBOUND") then counter.FINANCIAL_INSTITUTION_COUNTRY_CODE
              when (ftr.TRANSACTION_DIRECTION = "OUTBOUND") then BANK.FINANCIAL_INSTITUTION_COUNTRY_CODE
              end creditorPspCountry,
        case when (ftr.TRANSACTION_DIRECTION = "OUTBOUND") then BANK.FINANCIAL_INSTITUTION_COUNTRY_CODE
              when (ftr.TRANSACTION_DIRECTION = "INBOUND") then counter.FINANCIAL_INSTITUTION_COUNTRY_CODE
              end debtorPspCountry,
        ftr.transaction_currency as currency,
        'VALE' as metric,
        SUM(ftr.TRANSACTION_AMOUNT) as reportedAmount,
        CURRENT_TIMESTAMP AS Load_timestamp,
        ""  AS Period,

  FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_ACCOUNT_TRANSACTIONS_DECRYPTED` as ftr
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ACCOUNT_TRANSACTION_CURRENT` as dtr  ON ftr.T_D_ACCOUNT_TRANSACTION_DIM_KEY = dtr.T_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` AS ibis ON ftr.T_D_IBIS_ACCOUNT_DIM_KEY = ibis.T_DIM_KEY
  INNER JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` as bank ON ibis.T_D_BANK_ACCOUNT_DIM_KEY = bank.T_DIM_KEY
  LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` as counter ON counter.T_DIM_KEY = ftr.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
  WHERE
      ibis.ACCOUNT_TYPE = 'PAYMENT'
  AND ftr.TRANSACTION_TYPE IN ('RETURN')
  AND dtr.TRANSACTION_STATUS IN ('SETTLED', 'RETURNED')
  AND dtr.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '2024-10-01 10:34:13.824572', 'Etc/UTC'))   -- +01 for winter time, +02 for summer time
  AND dtr.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '2024-10-31 10:34:13.824572', 'Etc/UTC'))  -- like timezone('UTC', to_timestamp('2023-12-31 UTC+01', 'YYYY-MM-DD ""UTC""TZH') + interval '1 day') -- +01 for winter time, +02 for summer time
  AND ( bank.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LU'
      or  COUNTER.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LU')
  group by 1,2,3,4,5,6,7,8
  order by 1,2,3,4,5,6,7,8
    );
  