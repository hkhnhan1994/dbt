
{% set period_time = period_calculate(time = 'monthly', selection_date="today", prefix='', suffix='M' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'LU' -%}


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
  "{{period}}"  AS Period,
FROM {{ source('source_dwh_STRP','D_IBIS_ACCOUNT_CURRENT') }} IACC
left join {{ source('source_dwh_STRP','D_BANK_ACCOUNTS_DECRYPTED') }} BANK on IACC.T_D_BANK_ACCOUNT_DIM_KEY = BANK.T_DIM_KEY
where ACCOUNT_TYPE = 'PAYMENT'
and BANK.FINANCIAL_INSTITUTION_COUNTRY_CODE = '{{country_code}}'
and IACC.ACCOUNT_STATUS = 'OPEN'
or (
  IACC.ACCOUNT_STATUS = 'CLOSED'
  and IACC.ACCOUNT_UPDATED_AT > TIMESTAMP(DATETIME( '{{period_time['begin_date']}}', '{{time_zone}}'))
  )
and IACC.ACCOUNT_CREATED_AT < TIMESTAMP(DATETIME( '{{period_time['end_date']}}', '{{time_zone}}'))
group by 1,2
