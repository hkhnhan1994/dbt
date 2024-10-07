
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_PT`.`PAY_CPNC`
      
    
    

    OPTIONS()
    as (
      SELECT
    "X" AS Ot,
    row_number() over () as IDReg,
    LAST_DAY(date_add(DATE(TIMESTAMP(DATETIME( '2024-10-01 10:33:50.297425', 'Etc/UTC'))), INTERVAL -1 MONTH)) as DtRef,
    "5845" AS ASPSP,
    "5493000RZ2KSLKCYNN98" AS LEIASPSP,
    "N.E." AS `If`,
    "2" AS TipCont,
    "" AS DepOvTr,
    le.ENTERPRISE_ADDRESS_COUNTRY as PasTit,
    le.ENTERPRISE_ADDRESS_POSTAL_CODE AS CPos,
    pa.PAYMENT_ACCOUNT_CURRENCY AS Div,
    "Y" AS ContrOB,
    count(1) as NCont,
    count(1) as NContAOn,
    0 AS NContAISPM,
    0 AS SaldoME,
    ""  AS Period,
    CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` as iac
JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_ACCOUNT_DECRYPTED` AS pa
    ON iac.T_D_PAYMENT_ACCOUNT_DIM_KEY = pa.T_DIM_KEY
    AND LEFT(pa.PAYMENT_ACCOUNT_NUMBER,2) = 'BE'
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_LEGAL_ENTITY_PAYMENT_ACCOUNT_ROLES` AS lep
        ON lep.T_D_PAYMENT_ACCOUNT_DIM_KEY = pa.T_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_LEGAL_ENTITY_DECRYPTED` AS le
    ON le.T_DIM_KEY = lep.T_D_LEGAL_ENTITY_DIM_KEY
WHERE
    (
        pa.PAYMENT_ACCOUNT_STATUS = 'ACTIVE'
        OR(
            pa.PAYMENT_ACCOUNT_STATUS != 'ACTIVE'
            AND pa.PAYMENT_ACCOUNT_UPDATED_AT >= TIMESTAMP('2024-10-01 10:33:50.297425')
        )
    )
    AND left(pa.PAYMENT_ACCOUNT_NUMBER,2) = 'BE'
    AND pa.PAYMENT_ACCOUNT_CREATED_AT <= TIMESTAMP('2024-10-31 10:33:50.297425')
GROUP BY 1,3,4,5,6,7,8,9,10,11,12,15,16,17,18
    );
  