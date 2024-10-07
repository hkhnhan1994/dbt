
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_PT`.`PAY_CPMC`
      
    
    

    OPTIONS()
    as (
      WITH operation_types AS (
    (select 'ICDT' as family, 'ESCT' as subfamily, '22' as TipOper) union all
    (select 'MCOP' as family, 'OTHR' as subfamily, '27' as TipOper) union all
    (select 'MDOP' as family, 'OTHR' as subfamily, '28' as TipOper) union all
    (select 'RCDT' as family, 'ESCT' as subfamily, '21' as TipOper) union all
    (select 'RCDT' as family, 'RRTN' as subfamily, '21' as TipOper) union all
    (select 'RDDT' as family, 'ESDD' as subfamily, '22' as TipOper) union all
    (select 'RDDT' as family, 'OODD' as subfamily, '22' as TipOper)
)
SELECT
    "X" AS Ot,
    row_number() over () as IDReg,
    LAST_DAY(date_add(DATE(TIMESTAMP(DATETIME( '2024-10-01 10:33:49.775907', 'Etc/UTC'))), INTERVAL -1 MONTH)) as DtRef,
    "5845" AS ASPSP,
    ifnull(t.TipOper, 'NA') AS TipOper,
    count(1) as Quant,
    sum(am.MOVEMENT_AMOUNT) as Mont,
    ""  AS Period,
    CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_ACCOUNT_MOVEMENTS` as am
JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT` AS ia
    ON am.T_D_IBIS_ACCOUNT_DIM_KEY = ia.T_DIM_KEY
JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` AS bad
        ON ia.T_D_BANK_ACCOUNT_DIM_KEY = bad.T_DIM_KEY
LEFT JOIN operation_types t
    ON t.family = am.MOVEMENT_FAMILY
    AND t.subfamily = am.MOVEMENT_SUBFAMILY
WHERE
    left(bad.BANK_ACCOUNT_NUMBER,2) = 'BE'
    AND am.MOVEMENT_BOOKING_DATE_AT >= TIMESTAMP(DATETIME( '2024-10-01 10:33:49.775907', 'Etc/UTC'))
    AND am.MOVEMENT_BOOKING_DATE_AT <= TIMESTAMP(DATETIME( '2024-10-31 10:33:49.775907', 'Etc/UTC'))
GROUP BY 1,3,4,5,8,9
    );
  