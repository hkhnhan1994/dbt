with 
staging as (
   select * from {{ref('stg_D_AUTHORIZED_PERSON')}}
),
dwh as(

   select * 
   from {{ source('dwh_strp', 'dwh_D_AUTHORIZED_PERSON') }}
),
surrogate_key as(
    select
		{{ dbt_utils.generate_surrogate_key([
				'T_BUS_KEY', 
				'T_ROW_HASH'
			])
		}} as T_UNIQUE_KEY, 
		*
	from staging
)
SELECT 
    {% if is_incremental() %}
        (SELECT COALESCE(MAX(T_DIM_KEY), 1) FROM dwh ) + ROW_NUMBER() over (Order by surrogate_key.T_INGESTION_TIMESTAMP ASC) as T_DIM_KEY,
    {% else %}
        CAST(0 AS INT64) AS T_DIM_KEY,
    {% endif %}
    *
FROM surrogate_key