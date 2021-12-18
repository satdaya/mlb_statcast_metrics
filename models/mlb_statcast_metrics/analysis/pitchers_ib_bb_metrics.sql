{{
    config(
        materialized= 'table',
        unique_key= 'tab_pk' 
    )
}}

WITH cte_innings_block AS (
  SELECT * FROM {{ref('pitchers_inning_blocks')}}
), cte_metrics AS (
  SELECT
     _year
    ,pitcher_id
    ,pitcher_full_name
    ,inning_block
    ,_year || inning_block AS tab_pk
    ,ROUND(br_spin_rate, 2) AS br_spin_rate
    ,ROUND(br_x_axis_movement, 2) AS br_x_axis_movement
    ,ROUND(br_z_axis_movement, 2) AS br_z_axis_movement
    ,ROUND(br_spin_rate - LAG(br_spin_rate, 1) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_1st_block
    ,ROUND(br_spin_rate - LAG(br_spin_rate, 2) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_2nd_block
    ,ROUND(br_spin_rate - LAG(br_spin_rate, 3) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_3rd_block
    ,ROUND(br_spin_rate - LAG(br_spin_rate, 4) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_4th_block
    ,ROUND(br_spin_rate - LAG(br_spin_rate, 5) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_5th_block
    ,ROUND(br_spin_rate - LAG(br_spin_rate, 6) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_6th_block
    ,ROUND(br_spin_rate - LAG(br_spin_rate, 7) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_7th_block
    ,ROUND(br_spin_rate - LAG(br_spin_rate, 8) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_8th_block
    ,ROUND(br_spin_rate - LAG(br_spin_rate, 9) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_9th_block
    ,ROUND(br_x_axis_movement - LAG(br_x_axis_movement, 1) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_1st_block
    ,ROUND(br_x_axis_movement - LAG(br_x_axis_movement, 2) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_2nd_block
    ,ROUND(br_x_axis_movement - LAG(br_x_axis_movement, 3) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_3rd_block
    ,ROUND(br_x_axis_movement - LAG(br_x_axis_movement, 4) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_4th_block
    ,ROUND(br_x_axis_movement - LAG(br_x_axis_movement, 5) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_5th_block
    ,ROUND(br_x_axis_movement - LAG(br_x_axis_movement, 6) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_6th_block
    ,ROUND(br_x_axis_movement - LAG(br_x_axis_movement, 7) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_7th_block
    ,ROUND(br_z_axis_movement - LAG(br_x_axis_movement, 8) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_8th_block
    ,ROUND(br_z_axis_movement - LAG(br_x_axis_movement, 9) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_9th_block
    ,ROUND(br_z_axis_movement - LAG(br_z_axis_movement, 1) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_1st_block
    ,ROUND(br_z_axis_movement - LAG(br_z_axis_movement, 2) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_2nd_block
    ,ROUND(br_z_axis_movement - LAG(br_z_axis_movement, 3) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_3rd_block
    ,ROUND(br_z_axis_movement - LAG(br_z_axis_movement, 4) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_4th_block
    ,ROUND(br_z_axis_movement - LAG(br_z_axis_movement, 5) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_5th_block
    ,ROUND(br_z_axis_movement - LAG(br_z_axis_movement, 6) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_6th_block
    ,ROUND(br_z_axis_movement - LAG(br_z_axis_movement, 7) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_7th_block
    ,ROUND(br_z_axis_movement - LAG(br_z_axis_movement, 8) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_8th_block
    ,ROUND(br_z_axis_movement - LAG(br_z_axis_movement, 9) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_9th_block
  FROM cte_innings_block
),
cte_final AS (
  SELECT * FROM cte_metrics
)
SELECT * FROM cte_final