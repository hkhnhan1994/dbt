
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_BE`.`PCP_MERCHANT_ACCT_TRX`
      
    
    

    OPTIONS()
    as (
      WITH pre AS(
  SELECT
      pcp.PRODUCT,
      pcp.TRANSACTION_PUBLIC_IDENTIFIER,
      pcp.PAYERCOUNTRY,
      pcp.MERCHANTCOUNTRY,
      pcp.AMOUNT,
      pcp.CURRENCY,
      pcp.MERCHANT_PUBLIC_IDENTIFIER,
      IF(ocs.companyid_tobeexcluded IS NULL, "BE", ocs.to_be_reported_in_country) AS TO_BE_REPORTED_IN_COUNTRY,
      CASE
          WHEN pcp.PRODUCT = 'BANCONTACT_COLLECT' THEN pcp.PAYERCOUNTRY
          WHEN pcp.CARD_NETWORK = 'MASTERCARD' THEN pcp.CARD_COUNTRY
          WHEN pcp.CARD_NETWORK = 'VISA' THEN pcp.CARD_COUNTRY
          ELSE "no match"
      END AS COUNTERPARTY_AREA,
      pcp.MERCHANTCOUNTRY AS POS_LOCATION,
      pcp.CARDFUNCTION,
      pcp.singleWIP AS WIP_TRANSACTION,
      Case
        WHEN PCP.PRODUCT = 'BANCONTACT_COLLECT' THEN 'Bancontact'
        WHEN PCP.PRODUCT = 'SILVERFLOW_CARDS' AND PCP.CARD_NETWORK = 'MASTERCARD' THEN 'Mastercard'
        WHEN PCP.PRODUCT = 'SILVERFLOW_CARDS' AND pcp.CARD_NETWORK = 'VISA' THEN 'Visa'
      END AS CARD_NETWORK,
      pcp.SCA_REASON,
      '2000' AS INTTN_CHNNL,
      'R' AS RMT_INTTN,
      'PCS_ALL' AS PYMNT_SCHM,
      '_Z' AS FRD_TYP,
      pcp.LOAD_TIMESTAMP,
      pcp.PERIOD,
      pcp.Period_begin_date,
      pcp.Period_end_date,
  FROM `pj-bu-dw-data-sbx`.`dev_dm_pst3_BE`.`PCP` AS pcp
  LEFT JOIN `pj-bu-dw-data-sbx`.`dev_dm_pst3_BE`.`PCP_ALBATROS_CMD_OCS` as ocs
    ON ocs.companyid_tobeexcluded = pcp.TRANSACTION_PUBLIC_IDENTIFIER
  ),
  code as(
  SELECT pre.*,
   IF(pre.TO_BE_REPORTED_IN_COUNTRY = pre.COUNTERPARTY_AREA,
        'W2',
        pre.COUNTERPARTY_AREA) AS AREA_CODE,
    IF( pre.TO_BE_REPORTED_IN_COUNTRY = pre.POS_LOCATION,
        'W2',
        pre.POS_LOCATION) AS LOCATION_CODE,
    CASE pre.CARDFUNCTION
      WHEN 'Debit' THEN '11'
      WHEN 'Credit' THEN '13'
      WHEN 'Deferred Debit' THEN '12'
      ELSE '_Z'
    END AS CRD_FNCTN,
  FROM pre
)
SELECT
   code.*EXCEPT(AREA_CODE,LOCATION_CODE,LOAD_TIMESTAMP,PERIOD),
  COALESCE(pst3_cp.CODE, 'Extra EEA')  AS COUNT_AREA,
  COALESCE(pst3_pos.CODE, 'Extra EEA') AS TRMNL_LCTN,
  pst3_sca.CODE AS SCA,
  LOAD_TIMESTAMP,
  PERIOD,
FROM code
LEFT JOIN `pj-bu-dw-dm-prod`.`prd_data_mart_PPST_BE`.`PST3_MAPPING` AS pst3_cp
    ON code.AREA_CODE = pst3_cp.CODE
    AND pst3_cp.TYPE = 'Geographical area'
LEFT JOIN `pj-bu-dw-dm-prod`.`prd_data_mart_PPST_BE`.`PST3_MAPPING` AS pst3_pos
    ON code.LOCATION_CODE = pst3_pos.CODE
    AND pst3_pos.TYPE = 'Geographical area'
LEFT JOIN `pj-bu-dw-dm-prod`.`prd_data_mart_PPST_BE`.`PST3_MAPPING` AS pst3_sca
    ON code.SCA_REASON = pst3_sca.name
    AND pst3_sca.TYPE = 'Strong customer authentication (SCA)'
    );
  