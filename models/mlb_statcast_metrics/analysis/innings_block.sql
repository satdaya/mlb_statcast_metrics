{{
    config(
      materialized= 'table',
      unique_key= 'tab_pk'

    )
}}

WITH cte_pitchers_time_thru_order AS (
  SELECT * FROM {{ref('pitchers_inning_blocks')}}
),
cte_remove_individual_pitchers AS (
  SELECT
     _year
    ,inning_block
    ,AVG(fb_velo) AS fb_velo
    ,AVG(fb_spin_rate) AS fb_spin_rate
    ,AVG(fb_x_axis_movement) AS fb_x_axis_movement
    ,AVG(fb_z_axis_movement) AS fb_z_axis_movement
    ,AVG(br_spin_rate) AS br_spin_rate
    ,AVG(br_x_axis_movement) AS br_x_axis_movement
    ,AVG(br_z_axis_movement) AS br_z_axis_movement
  FROM cte_pitchers_time_thru_order
  GROUP BY 1,2
  ),
cte_consolidation AS (
  SELECT 
     _year
    ,inning_block
    ,_year || inning_block AS tab_pk
    ,MAX(fb_velo) AS fb_velo
    ,MAX(fb_spin_rate) AS fb_spin_rate
    ,MAX(fb_x_axis_movement) AS fb_x_axis_movement
    ,MAX(fb_z_axis_movement) AS fb_z_axis_movement
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
    ,inning_block
    ,_year || inning_block AS tab_pk
    ,fb_velo
    ,fb_spin_rate
    ,fb_x_axis_movement
    ,fb_z_axis_movement
    ,br_spin_rate
    ,br_x_axis_movement
    ,br_z_axis_movement
   FROM cte_consolidation
   ORDER BY 3
),
cte_final AS (
  SELECT * FROM cte_variance
)
SELECT * FROM cte_final