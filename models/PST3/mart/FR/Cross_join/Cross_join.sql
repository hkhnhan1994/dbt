WITH type AS (
  SELECT
    'agMoyPaiTypeOpeCanalTransactZoneGeo' AS axe,
    'VIREMENT' AS moyenPaiementtype,
    'VIR_EMIS' AS Operationtype,
    'ELECTRONIQUE' AS canaltype,
    'DISTANCE' AS typeTransaction,
    'axe1'  AS alias
  UNION all
  SELECT 'agMoyPaiTypeOpeCanalTransactZoneGeo', 'VIREMENT', 'VIR_EMIS', 'ELECTRONIQUE', 'PROXIMITE', 'axe1'
  UNION all
  SELECT 'agMoyPaiTypeOpeCanalTransactZoneGeoMcc', 'CARTE', 'CARTE_EMIS', 'ELECTRONIQUE', 'DISTANCE', 'axe2'
  UNION all
  SELECT 'agMoyPaiTypeOpeZoneGeo', 'PRELEVEM', 'PRELEVEM_EMIS', 'NA', 'NA', 'axe3'
  UNION all
  SELECT 'agMoyPaiTypeOpeZoneGeo', 'MON_ELEC', 'PAIEMT_ME_EMIS', 'NA', 'NA', 'axe3'
  UNION all
  SELECT 'agMoyPaiTypeOpeZoneGeo', 'CHEQ', 'CHEQ_RECU', 'NA', 'NA', 'axe3'
),
final as (
  SELECT
    axe,
    alias,
    moyenPaiementtype,
    Operationtype,
    canaltype,
    typeTransaction,
    ct.code as country_code,
    mcc.Coded as MCC_code
  FROM {{ source('source_dm_strp,COUNTRIES') }} ct
  cross join type
  cross join {{ source('source_dm_strp,MCC_CODES') }} mcc
  where type.axe = 'agMoyPaiTypeOpeCanalTransactZoneGeoMcc' and typeTransaction = 'DISTANCE'
  UNION ALL
  SELECT
    axe,
    alias,
    moyenPaiementtype,
    Operationtype,
    canaltype,
    typeTransaction,
    ct.code as country_code,
    NULL as MCC_code,
  FROM {{ source('source_dm_strp,COUNTRIES') }} ct
  cross join type
  cross join {{ source('source_dm_strp,MCC_CODES') }} mcc
  where type.axe <> 'agMoyPaiTypeOpeCanalTransactZoneGeoMcc'
)
SELECT
axe,
alias,
moyenPaiementtype,
Operationtype,
canaltype,
typeTransaction,
'5493004MX17L5E8MUB29' AS LEI,
payee_psp_country,
COALESCE(outbound_ibis_payments_trx_count, 0) AS outbound_ibis_payments_trx_count,
COALESCE(outbound_ibis_payments_amount_sum_in_EUR, 0) AS outbound_ibis_payments_amount_sum_in_EUR,
COALESCE(total_outbound_ibis_payments_trx_count, 0) AS total_outbound_ibis_payments_trx_count,
COALESCE(total_outbound_ibis_payments_amount_sum_in_EUR, 0) AS total_outbound_ibis_payments_amount_sum_in_EUR,
load_timestamp,
period,
FROM final
LEFT JOIN {{ source('source_dm_strp,CT1_agMoyPaiTypeOpeCanalTransactZoneGeo_totals') }}
ON payee_psp_country = country_code
