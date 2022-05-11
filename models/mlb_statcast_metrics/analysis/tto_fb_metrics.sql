{{
    config(
      materialized= 'table',
      unique_key= 'tab_pk'

    )
}}

with _times_thru_order as (
  select * from {{ref('times_thru_order')}}
), _metrics as (
  select
     _year
    ,times_thru_order
    ,_year || times_thru_order as tab_pk
    ,round(fb_velo, 2) as fb_velo
    ,round(fb_spin_rate, 2) as fb_spin_rate
    ,round(fb_x_axis_movement, 2) as fb_x_axis_movement
    ,round(fb_z_axis_movement, 2) as fb_z_axis_movement
    ,round(fb_velo - lag(fb_velo, 1) over (partition by _year order by times_thru_order), 2)  as var_fb_velo_from_1st_time_thru_order
    ,round(fb_velo - lag(fb_velo, 2) over (partition by _year order by times_thru_order), 2) as var_fb_velo_from_2nd_time_thru_order
    ,round(fb_velo - lag(fb_velo, 3) over (partition by _year order by times_thru_order), 2) as var_fb_velo_from_3rd_time_thru_order
    ,round(fb_spin_rate - lag(fb_spin_rate, 1) over (partition by _year order by times_thru_order), 2) as var_fb_spin_rate_from_1st_time_thru_order
    ,round(fb_spin_rate - lag(fb_spin_rate, 2) over (partition by _year order by times_thru_order), 2) as var_fb_spin_rate_from_2nd_time_thru_order
    ,round(fb_spin_rate - lag(fb_spin_rate, 3) over (partition by _year order by times_thru_order), 2) as var_fb_spin_rate_from_3rd_time_thru_order
    ,round(fb_x_axis_movement - lag(fb_x_axis_movement, 1) over (partition by _year order by times_thru_order), 2) as var_fb_x_axis_from_1st_time_thru_order
    ,round(fb_x_axis_movement - lag(fb_x_axis_movement, 2) over (partition by _year order by times_thru_order), 2) as var_fb_x_axis_from_2nd_time_thru_order
    ,round(fb_x_axis_movement - lag(fb_x_axis_movement, 3) over (partition by _year order by times_thru_order), 2)as var_fb_x_axis_from_3rd_time_thru_order
    ,round(fb_z_axis_movement - lag(fb_z_axis_movement, 1) over (partition by _year order by times_thru_order), 2) as var_fb_z_axis_from_1st_time_thru_order
    ,round(fb_z_axis_movement - lag(fb_z_axis_movement, 2) over (partition by _year order by times_thru_order), 2) as var_fb_z_axis_from_2nd_time_thru_order
    ,round(fb_z_axis_movement - lag(fb_z_axis_movement, 3) over (partition by _year order by times_thru_order), 2) as var_fb_z_axis_from_3rd_time_thru_order
  from _times_thru_order
),
_final as (
  select * from _metrics
)
select * from _final