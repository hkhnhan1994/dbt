
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dwh_view_cmd`.`D_AUTHORIZED_PERSON`
      
    
    

    OPTIONS()
    as (
      with surrogate_key as(
    select
		
    
to_hex(md5(cast(coalesce(cast(T_BUS_KEY as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(T_ROW_HASH as string), '_dbt_utils_surrogate_key_null_') as string))) as T_UNIQUE_KEY, 
		*
	FROM `pj-bu-dw-data-sbx`.`dev_staging_view_cmd`.`D_AUTHORIZED_PERSON`

)
SELECT 
    
        ROW_NUMBER() over (Order by surrogate_key.T_INGESTION_TIMESTAMP ASC) as T_DIM_KEY,
    
    *
FROM surrogate_key
    );
  