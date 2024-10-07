SELECT
  'PMAC' as AccountType,
  case
    when ACCOUNT_USAGE in ('CARE','CARE_EXPENSE','ESTATE') then 'HSNP' -- Households and NPISHs
    when ACCOUNT_USAGE is null then 'CORP' -- Non-financial corporations
    else 'check the query'
  end as Customer_Category,
  'VOLU' as metric,
  count(*) as reportedAmount,
  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP,
  ""  AS Period,
FROM `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_IBIS_ACCOUNT_CURRENT` IACC
left join `pj-bu-dw-dwh-prod`.`prd_data_warehouse_STRP`.`D_BANK_ACCOUNTS_DECRYPTED` BANK on IACC.T_D_BANK_ACCOUNT_DIM_KEY = BANK.T_DIM_KEY
where ACCOUNT_TYPE = 'PAYMENT'
and BANK.FINANCIAL_INSTITUTION_COUNTRY_CODE = 'LU'
and IACC.ACCOUNT_STATUS = 'OPEN'
or (
  IACC.ACCOUNT_STATUS = 'CLOSED'
  and IACC.ACCOUNT_UPDATED_AT > TIMESTAMP(DATETIME( '2024-10-01 10:34:12.673010', 'Etc/UTC'))
  )
and IACC.ACCOUNT_CREATED_AT < TIMESTAMP(DATETIME( '2024-10-31 10:34:12.673010', 'Etc/UTC'))
group by 1,2