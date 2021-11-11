{{
    config(
      materialized= 'table',
      unique_key= 'tab_pk'
    )
}}

WITH cte_pitchers_time_thru_order AS (
  SELECT * FROM pitchers_time_thru_order
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
  GROUP BY 1,2
  ORDER BY 1,2
),
cte_final AS (
  SELECT * FROM cte_consolidation
)
SELECT * FROM cte_final