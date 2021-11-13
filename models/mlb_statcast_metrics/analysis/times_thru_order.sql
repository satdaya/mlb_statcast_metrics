{{
    config(
      materialized= 'table',
      unique_key= 'tab_pk'

    )
}}

WITH cte_pitchers_time_thru_order AS (
  SELECT * FROM {{ref('pitchers_time_thru_order')}}
),
cte_remove_individual_pitchers AS (
  SELECT
     _year
    ,times_thru_order
    ,AVG(fb_velo) AS fb_velo
    ,AVG(fb_spin_rate) AS fb_spin_rate
    ,AVG(fb_x_axis_movement) AS fb_x_axis_movement
    ,AVG(fb_z_axis_movement) AS fb_z_axis_movement
    ,AVG(br_velo) AS br_velo
    ,AVG(br_spin_rate) AS br_spin_rate
    ,AVG(br_x_axis_movement) AS br_x_axis_movement
    ,AVG(br_z_axis_movement) AS br_z_axis_movement
  FROM cte_pitchers_time_thru_order
  GROUP BY 1,2
  ),
cte_consolidation AS (
  SELECT 
     _year
    ,times_thru_order
    ,_year || times_thru_order AS tab_pk
    ,MAX(fb_velo) AS fb_velo
    ,MAX(fb_spin_rate) AS fb_spin_rate
    ,MAX(fb_x_axis_movement) AS fb_x_axis_movement
    ,MAX(fb_z_axis_movement) AS fb_z_axis_movement
    ,MAX(br_velo) AS br_velo
    ,MAX(br_spin_rate) AS br_spin_rate
    ,MAX(br_x_axis_movement) AS br_x_axis_movement
    ,MAX(br_z_axis_movement) AS br_z_axis_movement
  FROM cte_remove_individual_pitchers
  GROUP BY 1,2,3
  ORDER BY 1,2
),
cte_variance AS (
  SELECT
     _year
    ,times_thru_order
    ,_year || times_thru_order AS tab_pk
    ,fb_velo
    ,fb_velo - LAG(fb_velo, 1) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_fb_velo_from_1st_time_thru_order
    ,fb_velo - LAG(fb_velo, 2) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_fb_velo_from_2nd_time_thru_order
    ,fb_velo - LAG(fb_velo, 3) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_fb_velo_from_3rd_time_thru_order
    ,fb_spin_rate
    ,fb_spin_rate - LAG(fb_spin_rate, 1) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_fb_spin_rate_from_1st_time_thru_order
    ,fb_spin_rate - LAG(fb_spin_rate, 2) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_fb_spin_rate_from_2nd_time_thru_order
    ,fb_spin_rate - LAG(fb_spin_rate, 3) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_fb_spin_rate_from_3rd_time_thru_order
    ,fb_x_axis_movement
    ,fb_x_axis_movement - LAG(fb_x_axis_movement, 1) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_fb_x_axis_from_1st_time_thru_order
    ,fb_x_axis_movement - LAG(fb_x_axis_movement, 2) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_fb_x_axis_from_2nd_time_thru_order
    ,fb_x_axis_movement - LAG(fb_x_axis_movement, 3) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_fb_x_axis_from_3rd_time_thru_order
    ,fb_z_axis_movement
    ,fb_z_axis_movement - LAG(fb_z_axis_movement, 1) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_fb_z_axis_from_1st_time_thru_order
    ,fb_z_axis_movement - LAG(fb_z_axis_movement, 2) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_fb_z_axis_from_2nd_time_thru_order
    ,fb_z_axis_movement - LAG(fb_z_axis_movement, 3) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_fb_z_axis_from_3rd_time_thru_order
    ,br_spin_rate
    ,br_spin_rate - LAG(br_spin_rate, 1) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_br_spin_rate_from_1st_time_thru_order
    ,br_spin_rate - LAG(br_spin_rate, 2) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_br_spin_rate_from_2nd_time_thru_order
    ,br_spin_rate - LAG(br_spin_rate, 3) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_br_spin_rate_from_3rd_time_thru_order
    ,br_x_axis_movement
    ,br_x_axis_movement - LAG(br_x_axis_movement, 1) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_br_x_axis_from_1st_time_thru_order
    ,br_x_axis_movement - LAG(br_x_axis_movement, 2) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_br_x_axis_from_2nd_time_thru_order
    ,br_x_axis_movement - LAG(br_x_axis_movement, 3) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_br_x_axis_from_3rd_time_thru_order
    ,br_z_axis_movement
    ,br_z_axis_movement - LAG(br_z_axis_movement, 1) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_br_z_axis_from_1st_time_thru_order
    ,br_z_axis_movement - LAG(br_z_axis_movement, 2) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_br_z_axis_from_2nd_time_thru_order
    ,br_z_axis_movement - LAG(br_z_axis_movement, 3) OVER (PARTITION BY _year ORDER BY times_thru_order) AS var_br_z_axis_from_3rd_time_thru_order
   FROM cte_consolidation
   ORDER BY 3
),
cte_final AS (
  SELECT * FROM cte_variance
)
SELECT * FROM cte_final