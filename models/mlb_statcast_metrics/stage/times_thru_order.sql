{{
    config(
      materialized= 'table',
      unique_key= 'tab_pk'

    )
}}

with _pitchers_time_thru_order as (
  select * from {{ref('pitchers_time_thru_order')}}
),
_remove_individual_pitchers as (
  select
     _year
    ,times_thru_order
    ,avg(fb_velo) as fb_velo
    ,avg(fb_spin_rate) as fb_spin_rate
    ,avg(fb_x_axis_movement) as fb_x_axis_movement
    ,avg(fb_z_axis_movement) as fb_z_axis_movement
    ,avg(br_spin_rate) as br_spin_rate
    ,avg(br_x_axis_movement) as br_x_axis_movement
    ,avg(br_z_axis_movement) as br_z_axis_movement
  from _pitchers_time_thru_order
  {{ dbt_utils.group_by(2) }}
  ),
_consolidation as (
  select 
     _year
    ,times_thru_order
    ,_year || times_thru_order as tab_pk
    ,max(fb_velo) as fb_velo
    ,max(fb_spin_rate) as fb_spin_rate
    ,max(fb_x_axis_movement) as fb_x_axis_movement
    ,max(fb_z_axis_movement) as fb_z_axis_movement
    ,max(br_spin_rate) as br_spin_rate
    ,max(br_x_axis_movement) as br_x_axis_movement
    ,max(br_z_axis_movement) as br_z_axis_movement
  from _remove_individual_pitchers
{{ dbt_utils.group_by(3) }}
),
_variance as (
  select
     _year
    ,times_thru_order
    ,_year || times_thru_order as tab_pk
    ,fb_velo
    ,fb_spin_rate
    ,fb_x_axis_movement
    ,fb_z_axis_movement
    ,br_spin_rate
    ,br_x_axis_movement
    ,br_z_axis_movement
   from _consolidation
),
_final as (
  select * from _variance
)
select * from _final