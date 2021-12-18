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
    ,ROUND(fb_velo, 2) AS fb_velo
    ,ROUND(fb_spin_rate, 2) AS fb_spin_rate
    ,ROUND(fb_x_axis_movement, 2) AS fb_x_axis_movement
    ,ROUND(fb_z_axis_movement, 2) AS fb_z_axis_movement
    ,ROUND(fb_velo - LAG(fb_velo, 1) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_velo_from_1st_block
    ,ROUND(fb_velo - LAG(fb_velo, 2) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_velo_from_2nd_block
    ,ROUND(fb_velo - LAG(fb_velo, 3) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_velo_from_3rd_block
    ,ROUND(fb_velo - LAG(fb_velo, 4) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_velo_from_4th_block
    ,ROUND(fb_velo - LAG(fb_velo, 5) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_velo_from_5th_block
    ,ROUND(fb_velo - LAG(fb_velo, 6) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_velo_from_6th_block
    ,ROUND(fb_velo - LAG(fb_velo, 7) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_velo_from_7th_block
    ,ROUND(fb_velo - LAG(fb_velo, 8) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_velo_from_8th_block
    ,ROUND(fb_velo - LAG(fb_velo, 9) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_velo_from_9th_block
    ,ROUND(fb_velo - LAG(fb_spin_rate, 1) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_1st_block
    ,ROUND(fb_velo - LAG(fb_spin_rate, 2) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_2nd_block
    ,ROUND(fb_velo - LAG(fb_spin_rate, 3) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_3rd_block
    ,ROUND(fb_velo - LAG(fb_spin_rate, 4) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_4th_block
    ,ROUND(fb_velo - LAG(fb_spin_rate, 5) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_5th_block
    ,ROUND(fb_velo - LAG(fb_spin_rate, 6) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_6th_block
    ,ROUND(fb_velo - LAG(fb_spin_rate, 7) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_7th_block
    ,ROUND(fb_velo - LAG(fb_spin_rate, 8) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_8th_block
    ,ROUND(fb_velo - LAG(fb_spin_rate, 9) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_spin_from_9th_block
    ,ROUND(fb_velo - LAG(fb_x_axis_movement, 1) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_1st_block
    ,ROUND(fb_velo - LAG(fb_x_axis_movement, 2) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_2nd_block
    ,ROUND(fb_velo - LAG(fb_x_axis_movement, 3) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_3rd_block
    ,ROUND(fb_velo - LAG(fb_x_axis_movement, 4) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_4th_block
    ,ROUND(fb_velo - LAG(fb_x_axis_movement, 5) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_5th_block
    ,ROUND(fb_velo - LAG(fb_x_axis_movement, 6) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_6th_block
    ,ROUND(fb_velo - LAG(fb_x_axis_movement, 7) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_7th_block
    ,ROUND(fb_velo - LAG(fb_x_axis_movement, 8) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_8th_block
    ,ROUND(fb_velo - LAG(fb_x_axis_movement, 9) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_x_ax_from_9th_block
    ,ROUND(fb_velo - LAG(fb_z_axis_movement, 1) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_1st_block
    ,ROUND(fb_velo - LAG(fb_z_axis_movement, 2) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_2nd_block
    ,ROUND(fb_velo - LAG(fb_z_axis_movement, 3) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_3rd_block
    ,ROUND(fb_velo - LAG(fb_z_axis_movement, 4) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_4th_block
    ,ROUND(fb_velo - LAG(fb_z_axis_movement, 5) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_5th_block
    ,ROUND(fb_velo - LAG(fb_z_axis_movement, 6) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_6th_block
    ,ROUND(fb_velo - LAG(fb_z_axis_movement, 7) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_7th_block
    ,ROUND(fb_velo - LAG(fb_z_axis_movement, 8) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_8th_block
    ,ROUND(fb_velo - LAG(fb_z_axis_movement, 9) OVER (PARTITION BY _year, pitcher_id ORDER BY inning_block), 2) AS var_z_ax_from_9th_block
  FROM cte_innings_block
),
cte_final AS (
  SELECT * FROM cte_metrics
)
SELECT * FROM cte_final