SELECT
    payee_psp_country,
    outbound_ibis_payments_trx_count,
    outbound_ibis_payments_amount_sum_in_EUR,
    SUM(outbound_ibis_payments_trx_count) OVER(PARTITION BY period) AS total_outbound_ibis_payments_trx_count,
    SUM(outbound_ibis_payments_amount_sum_in_EUR) OVER(PARTITION BY period) AS total_outbound_ibis_payments_amount_sum_in_EUR,
    load_timestamp,
    period
FROM {{ source('source_dm_strp,CT1_agMoyPaiTypeOpeCanalTransactZoneGeo') }}
