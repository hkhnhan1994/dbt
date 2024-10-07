
  
    

    create or replace table `pj-bu-dw-data-sbx`.`dev_dm_pst3_BE`.`PCP_POS_ONLINE_WITH_OUT_SCA`
      
    
    

    OPTIONS()
    as (
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
FROM `pj-bu-dw-data-sbx`.`dev_dm_pst3_BE`.`PCP_POS_ONLINE`
group by
  1,2,3,4,5,8,9,10,11
    );
  