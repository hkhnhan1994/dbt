with surrogate_key as(
    select
		{{ dbt_utils.generate_surrogate_key([
				'T_BUS_KEY', 
				'T_ROW_HASH'
			])
		}} as T_UNIQUE_KEY, 
		*
	FROM {{ref('stg_strh_D_ENTERPRISE')}}

)
SELECT 
    {% if is_incremental() %}
        (SELECT COALESCE(MAX(T_DIM_KEY), 1) FROM {{ source('dwh_strh', 'D_ENTERPRISE') }} ) + ROW_NUMBER() over (Order by surrogate_key.T_INGESTION_TIMESTAMP ASC) as T_DIM_KEY,
    {% else %}
        ROW_NUMBER() over (Order by surrogate_key.T_INGESTION_TIMESTAMP ASC) as T_DIM_KEY,
    {% endif %}
    *
FROM surrogate_key