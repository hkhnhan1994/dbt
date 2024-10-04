
{% set period_time = period_calculate(time = 'semesterly', selection_date="today", prefix='', suffix='S' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'BE' -%}


        SELECT
  Counterpart_area,
  POS_location,
  remote_non_remote_initiation,
  SCA_used,
  SCA_Exemption_reason,
  sum(number_of_transactions) as sum_of_count,
  sum(total_sum) as sum_of_total,
  LOAD_TIMESTAMP,
  Period,
  Period_begin_date,
  Period_end_date,
FROM {{ ref('PCP_POS_ONLINE') }}
group by
  1,2,3,4,5,8,9,10,11
