

  create or replace view `pj-bu-dw-data-sbx`.`dev`.`dwh_strh_D_AUTHORIZED_PERSON`
  OPTIONS()
  as with 
staging as (
   select * from `pj-bu-dw-data-sbx`.`dev_staging_view_cmd`.`stg_strh_D_AUTHORIZED_PERSON`
),
dwh as(

   select * 
   from `pj-bu-dw-data-sbx`.`dev_dwh_view_cmd`.`dwh_strh_D_AUTHORIZED_PERSON`
),
surrogate_key as(
    select
		
    
to_hex(md5(cast(coalesce(cast(T_BUS_KEY as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(T_ROW_HASH as string), '_dbt_utils_surrogate_key_null_') as string))) as T_UNIQUE_KEY, 
		*
	from staging
)
SELECT 
    
        CAST(0 AS INT64) AS T_DIM_KEY,
    
    *
FROM surrogate_key;

