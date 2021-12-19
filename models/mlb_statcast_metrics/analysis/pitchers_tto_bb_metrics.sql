{{
    config(
      materialized= 'table',
      unique_key= 'tab_pk'

    )
}}

WITH cte_times_thru_order AS (
  SELECT * FROM {{ref('pitchers_time_thru_order')}}
), cte_metrics AS (
  SELECT
     _year
    ,pitcher_id
    ,pitcher_full_name
    ,times_thru_order
    ,_year || times_thru_order AS tab_pk
    ,ROUND(br_spin_rate, 2) AS br_spin_rate
    ,ROUND(br_x_axis_movement, 2) AS br_x_axis_movement
    ,ROUND(br_z_axis_movement, 2) AS br_z_axis_movemen
    ,ROUND(br_spin_rate - LAG(br_spin_rate, 1) OVER (PARTITION BY _year, pitcher_id  ORDER BY times_thru_order), 2) AS var_br_spin_rate_from_1st_time_thru_order
    ,ROUND(br_spin_rate - LAG(br_spin_rate, 2) OVER (PARTITION BY _year, pitcher_id  ORDER BY times_thru_order), 2) AS var_br_spin_rate_from_2nd_time_thru_order
    ,ROUND(br_spin_rate - LAG(br_spin_rate, 3) OVER (PARTITION BY _year, pitcher_id  ORDER BY times_thru_order), 2) AS var_br_spin_rate_from_3rd_time_thru_order
    ,ROUND(br_x_axis_movement - LAG(br_x_axis_movement, 1) OVER (PARTITION BY _year, pitcher_id  ORDER BY times_thru_order), 2) AS var_br_x_axis_from_1st_time_thru_order
    ,ROUND(br_x_axis_movement - LAG(br_x_axis_movement, 2) OVER (PARTITION BY _year, pitcher_id  ORDER BY times_thru_order), 2) AS var_br_x_axis_from_2nd_time_thru_order
    ,ROUND(br_x_axis_movement - LAG(br_x_axis_movement, 3) OVER (PARTITION BY _year, pitcher_id  ORDER BY times_thru_order), 2)AS var_br_x_axis_from_3rd_time_thru_order
    ,ROUND(br_z_axis_movement - LAG(br_z_axis_movement, 1) OVER (PARTITION BY _year, pitcher_id  ORDER BY times_thru_order), 2) AS var_br_z_axis_from_1st_time_thru_order
    ,ROUND(br_z_axis_movement - LAG(br_z_axis_movement, 2) OVER (PARTITION BY _year, pitcher_id  ORDER BY times_thru_order), 2) AS var_br_z_axis_from_2nd_time_thru_order
    ,ROUND(br_z_axis_movement - LAG(br_z_axis_movement, 3) OVER (PARTITION BY _year, pitcher_id  ORDER BY times_thru_order), 2) AS var_br_z_axis_from_3rd_time_thru_order
  FROM cte_times_thru_order
),
cte_final AS (
  SELECT * FROM cte_metrics
)
SELECT * FROM cte_final