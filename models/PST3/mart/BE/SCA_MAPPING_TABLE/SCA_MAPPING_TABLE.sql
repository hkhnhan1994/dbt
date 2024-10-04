
{% set period_time = period_calculate(time = 'semesterly', selection_date="today", prefix='', suffix='' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'BE' -%}


        SELECT
  "full-auth" AS SCA_RESULT,
  "SCA used" AS SCA_REASON,
UNION ALL
SELECT
  "attempt" AS SCA_RESULT,
  "non-SCA used: reason is others" AS SCA_REASON,
UNION ALL
SELECT
  "non-authenticated" AS SCA_RESULT,
  "non-SCA used: reason is others" AS SCA_REASON,
UNION ALL
SELECT
  NULL AS SCA_RESULT,
  "non-SCA used: reason is others" AS SCA_REASON,
