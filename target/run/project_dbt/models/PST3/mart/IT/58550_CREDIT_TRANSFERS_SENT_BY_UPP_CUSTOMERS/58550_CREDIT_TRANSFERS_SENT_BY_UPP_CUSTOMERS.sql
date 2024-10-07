
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_IT`.`58550_CREDIT_TRANSFERS_SENT_BY_UPP_CUSTOMERS`
      
    
    

    OPTIONS()
    as (
      WITH credit_transfers as
  (
  SELECT
    CASE
        WHEN FAT.TRANSACTION_CHANNEL in ('DASHBOARD', 'ADMIN', 'OTHER') THEN 'WITH TRADITIONAL MODES'
        WHEN FAT.TRANSACTION_CHANNEL in ('TPP', 'OCS', 'H2H') THEN 'WITH AUTOMATED MODES'
    END Entrymode,
    CASE
        WHEN FAT.TRANSACTION_CHANNEL in ('DASHBOARD', 'ADMIN', 'OTHER', 'TPP','OCS') THEN 'Single entry'
        WHEN FAT.TRANSACTION_CHANNEL in ('H2H') THEN 'Batch entry'
    END BatchMode,
    CBA.FINANCIAL_INSTITUTION_COUNTRY_CODE AS Payee_PSP_country,
    IA.ACCOUNT_MASTER_DATA_ID as account_master_data_id_4lookup,
    case when FAB.BALANCE_AMOUNT < 12500 then '66'
          when FAB.BALANCE_AMOUNT between 12500 and 50000 then '67'
          when FAB.BALANCE_AMOUNT > 50000 then '89'
    end balance_class,
    FAT.TRANSACTION_AMOUNT AS amount,
    CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
    ""  AS Period,

    FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_ACCOUNT_TRANSACTIONS_DECRYPTED` AS FAT
    LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_ACCOUNT_TRANSACTION_CURRENT` AS DAT
      ON FAT.T_D_ACCOUNT_TRANSACTION_DIM_KEY = DAT.T_DIM_KEY
    LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` AS IA
      ON FAT.T_D_IBIS_ACCOUNT_DIM_KEY = IA.T_DIM_KEY
    JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` AS BA
      ON IA.T_D_BANK_ACCOUNT_DIM_KEY = BA.T_DIM_KEY
    LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` AS CBA
      ON CBA.T_DIM_KEY = FAT.T_COUNTERPARTY_BANK_ACCOUNT_DIM_KEY
    JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_ACCOUNT_BALANCE` FAB
      ON FAB.T_D_IBIS_ACCOUNT_DIM_KEY = IA.T_DIM_KEY
    WHERE FAT.TRANSACTION_DIRECTION = "OUTBOUND"
      AND FAT.TRANSACTION_TYPE = 'REGULAR'
      AND DAT.TRANSACTION_STATUS IN ('RETURNED', 'SETTLED')
      AND IA.ACCOUNT_TYPE = 'PAYMENT'
      AND  FAT.TRANSACTION_BANK_FAMILY = 'ICDT'
      AND FAT.TRANSACTION_CHANNEL <> 'CARDS'
      AND BA.FINANCIAL_INSTITUTION_COUNTRY_CODE IN ('IT')
      AND DAT.TRANSACTION_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '2024-07-01', 'Etc/UTC'))
      AND DAT.TRANSACTION_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '2024-09-30', 'Etc/UTC'))
      AND  FAB.BALANCE_DATE =
      (
          SELECT max(max.BALANCE_DATE)
          FROM  `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_ACCOUNT_BALANCE` max
          JOIN  `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` a
            ON max.T_D_IBIS_ACCOUNT_DIM_KEY = a.T_DIM_KEY
          WHERE TIMESTAMP(max.BALANCE_DATE) <= TIMESTAMP(DATETIME( '2024-09-30', 'Etc/UTC'))
      )
  ),
  cmd as (
    SELECT
          P.T_SOURCE_PK_ID as account_Master_Data_Id_4lookup_cmd,
          L.ENTERPRISE_COUNTRY_OF_INCORPORATION,
          L.ENTERPRISE_PROVINCE_OF_INCORPORATION,
          --ba.code as ""NACEcodeToBeMappedToSettore"" ==> to add by CMD team (dragos)
      FROM  `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_ACCOUNT_CURRENT` P
      inner join `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_LEGAL_ENTITY_PAYMENT_ACCOUNT_ROLES` LPR
      on LPR.T_D_PAYMENT_ACCOUNT_DIM_KEY = P.T_DIM_KEY
      inner join `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_LEGAL_ENTITY_DECRYPTED` L
      on LPR.T_D_LEGAL_ENTITY_DIM_KEY = L.T_DIM_KEY
      WHERE
              LPR.ROLE = 'ACCOUNT_HOLDER'
        -- AND  ba.main is true ==> to add by CMD team (dragos)
        and L.ENTERPRISE_COUNTRY_OF_INCORPORATION = 'IT'
  )
  SELECT * FROM credit_transfers
  inner join cmd
  on credit_transfers.account_master_data_id_4lookup = cmd.account_Master_Data_Id_4lookup_cmd
    );
  