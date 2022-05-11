{{
    config(
      materialized= 'table',
      unique_key= 'tab_pk'

    )
}}

with cte_pitchers_time_thru_order AS (
  select * from {{ref('pitchers_inning_blocks')}}
),
cte_remove_individual_pitchers AS (
  select
     _year
    ,inning_block
    ,avg(fb_velo) AS fb_velo
    ,avg(fb_spin_rate) AS fb_spin_rate
    ,avg(fb_x_axis_movement) AS fb_x_axis_movement
    ,avg(fb_z_axis_movement) AS fb_z_axis_movement
    ,avg(br_spin_rate) AS br_spin_rate
    ,avg(br_x_axis_movement) AS br_x_axis_movement
    ,avg(br_z_axis_movement) AS br_z_axis_movement
  from cte_pitchers_time_thru_order
  GROUP BY 1,2
  ),
cte_consolidation AS (
  select 
     _year
    ,inning_block
    ,_year || inning_block AS tab_pk
    ,max(fb_velo) AS fb_velo
    ,max(fb_spin_rate) AS fb_spin_rate
    ,max(fb_x_axis_movement) AS fb_x_axis_movement
    ,max(fb_z_axis_movement) AS fb_z_axis_movement
    ,max(br_spin_rate) AS br_spin_rate
    ,max(br_x_axis_movement) AS br_x_axis_movement
    ,max(br_z_axis_movement) AS br_z_axis_movement
  from cte_remove_individual_pitchers
  GROUP BY 1,2,3
  ORDER BY 1,2
),
cte_variance AS (
  select
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
   from cte_consolidation
   order by 3
),
cte_final AS (
  select * from cte_variance
)
select * from cte_final