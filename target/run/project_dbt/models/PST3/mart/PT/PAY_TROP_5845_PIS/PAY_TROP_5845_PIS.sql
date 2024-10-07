
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_PT`.`PAY_TROP_5845_PIS`
      
    
    

    OPTIONS()
    as (
      SELECT
    'I' AS Ot,
    '' AS Ref,
    '' AS ORef,
    IF(deb.FINANCIAL_INSTITUTION_COUNTRY_CODE ='BE', substr(deb.BANK_ACCOUNT_NUMBER, 5,4), '9999') AS Ord,
    IF(cred.FINANCIAL_INSTITUTION_COUNTRY_CODE  ='BE', substr(cred.BANK_ACCOUNT_NUMBER,5,4), '9999') AS Ben,
    pi.T_SOURCE_PK_ID  AS PayID,
    deb.BANK_ACCOUNT_NUMBER  as debtor_account_identifier,
    cred.BANK_ACCOUNT_NUMBER as creditor_account_identifier ,
    '' AS BICOrd,
    '' AS BICBen,
    '' AS BICSen,
    '' AS BICRec,
    fi.FINANCIAL_INSTITUTION_COUNTRY AS PasOrd,
    cred.FINANCIAL_INSTITUTION_COUNTRY_CODE AS PasBen,
    '' as LEIOrd,
    '' as LEIBen,
    '28' as Sch,
    '' as Pro,
    cast(null as DATE) as DatLiq,
    date(pi.PAYMENT_INITIATION_CREATED_AT) as DtPISP,
    '' AS TsOrd,
    '' AS TsBen,
    '1' AS TipTR,
    it.INBOUND_TRANSACTION_CURRENCY_CODE as Div,
    it.INBOUND_TRANSACTION_AMOUNT as Mont,
    it.INBOUND_TRANSACTION_AMOUNT as MontOrg,
    '4' as TipCan,
    '2' as FormEnv,
    'Y' as OperElet,
    'Y' as OperRem,
    'Y' as OperECom,
    'N' as IniPISP,
    '1' as ModAc,
    'N' as SCA,
    '10' as MnonSCA,
    'N.E.' as SI,
    '' as PasmC,
    '' as PasnC,
    '' as CPosm,
    '' AS TipDoc,
    '' AS NumDoc,
    '' AS LEIC,
    '' AS ENI,
    '' AS InfUltBen,
    '' AS InfUltOrd,
    ""  AS Period,
    CURRENT_TIMESTAMP AS load_timestamp,

FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_INBOUND_PAYMENT_INFO_DECRYPTED` AS ip
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_FINANCIAL_PLATFORMS_DECRYPTED`
    AS fp ON ip.T_D_FINANCIAL_PLATFORM_DIM_KEY=fp.T_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_FINANCIAL_INSTITUTIONS`
    AS fi ON fi.T_DIM_KEY = fp.T_D_FINANCIAL_INSTITUTION_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`F_INBOUND_TRANSACTIONS_DECRYPTED`
    AS it ON ip.T_DIM_KEY=it.T_D_INBOUND_PAYMENT_INFO_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_INBOUND_TRANSACTION_INFO_DECRYPTED`
    AS dit ON dit.T_DIM_KEY=it.T_D_INBOUND_TRANSACTION_INFO_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_PAYMENT_INITIATIONS`
    AS pi ON ip.T_DIM_KEY=pi.T_D_INBOUND_PAYMENT_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_APPLICATIONS_DECRYPTED`
    AS app ON ip.T_D_APPLICATION_DIM_KEY = app.T_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED`
    AS deb ON deb.T_DIM_KEY=ip.T_D_DEBTOR_BANK_ACCOUNTS_DIM_KEY
LEFT JOIN `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED`
    AS cred ON cred.T_DIM_KEY=it.T_D_CREDITOR_BANK_ACCOUNTS_DIM_KEY

WHERE pi.PAYMENT_INITIATION_STATUS = 'SUCCESSFUL'
    AND fi.FINANCIAL_INSTITUTION_CODE <> 'IBIS'
    AND (
        app.APPLICATION_NAME IN ('PAY-PXG-COMMUNITY')
        OR
            (
                app.APPLICATION_NAME IN ('PAY-PXG-OCS')
                AND (cred.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'BE'
                AND substr(cred.BANK_ACCOUNT_NUMBER,5,3) = '504'    )
            )
        )
    --  AND (
    --     app.APPLICATION_NAME = 'PAY-PXG-BANQUPPT'
    --     OR (
    --         app.APPLICATION_NAME = 'PAY-PXG-OCS'
    --         AND cred.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'BE'
    --         AND substr(cred.BANK_ACCOUNT_NUMBER,5,4) = '5845'
    --         )
    --     )-- creditor account is a PANX PT account
    AND TIMESTAMP(dit.INBOUND_TRANSACTION_CREATED_AT) >= TIMESTAMP(DATETIME( '2024-10-06', 'Etc/UTC')) --pt winter time
    AND TIMESTAMP(dit.INBOUND_TRANSACTION_CREATED_AT) <= TIMESTAMP(DATETIME( '2024-10-07', 'Etc/UTC'))  --pt winter time
    );
  