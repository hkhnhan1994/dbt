
{% set period_time = period_calculate(time = 'daily', selection_date="today", prefix='', suffix='Q' ) -%}
{% set time_zone = "Etc/UTC" -%}
{% set country_code = 'FR' -%}


        SELECT
    payee_psp_country,
    outbound_ibis_payments_trx_count,
    outbound_ibis_payments_amount_sum_in_EUR,
    SUM(outbound_ibis_payments_trx_count) OVER(PARTITION BY period) AS total_outbound_ibis_payments_trx_count,
    SUM(outbound_ibis_payments_amount_sum_in_EUR) OVER(PARTITION BY period) AS total_outbound_ibis_payments_amount_sum_in_EUR,
    load_timestamp,
    period
FROM {{ ref('CT1_agMoyPaiTypeOpeCanalTransactZoneGeo') }}
