{{
    config(
      materialized= 'table',
      unique_key= 'tab_pk'

    )
}}

with _times_thru_order as (
  select * from {{ref('pitchers_time_thru_order')}}
),
_metrics as (
  select
     _year
    ,pitcher_id
    ,pitcher_full_name
    ,times_thru_order
    ,_year || times_thru_order as tab_pk
    ,round(br_spin_rate, 2) as br_spin_rate
    ,round(br_x_axis_movement, 2) as br_x_axis_movement
    ,round(br_z_axis_movement, 2) as br_z_axis_movemen
    ,round(br_spin_rate - lag(br_spin_rate, 1) over (partition by _year, pitcher_id  order by times_thru_order), 2) as var_br_spin_rate_from_1st_time_thru_order
    ,round(br_spin_rate - lag(br_spin_rate, 2) over (partition by _year, pitcher_id  order by times_thru_order), 2) as var_br_spin_rate_from_2nd_time_thru_order
    ,round(br_spin_rate - lag(br_spin_rate, 3) over (partition by _year, pitcher_id  order by times_thru_order), 2) as var_br_spin_rate_from_3rd_time_thru_order
    ,round(br_x_axis_movement - lag(br_x_axis_movement, 1) over (partition by _year, pitcher_id  order by times_thru_order), 2) as var_br_x_axis_from_1st_time_thru_order
    ,round(br_x_axis_movement - lag(br_x_axis_movement, 2) over (partition by _year, pitcher_id  order by times_thru_order), 2) as var_br_x_axis_from_2nd_time_thru_order
    ,round(br_x_axis_movement - lag(br_x_axis_movement, 3) over (partition by _year, pitcher_id  order by times_thru_order), 2)as var_br_x_axis_from_3rd_time_thru_order
    ,round(br_z_axis_movement - lag(br_z_axis_movement, 1) over (partition by _year, pitcher_id  order by times_thru_order), 2) as var_br_z_axis_from_1st_time_thru_order
    ,round(br_z_axis_movement - lag(br_z_axis_movement, 2) over (partition by _year, pitcher_id  order by times_thru_order), 2) as var_br_z_axis_from_2nd_time_thru_order
    ,round(br_z_axis_movement - lag(br_z_axis_movement, 3) over (partition by _year, pitcher_id  order by times_thru_order), 2) as var_br_z_axis_from_3rd_time_thru_order
  from _times_thru_order
),
_final as (
  select * from _metrics
)
select * from _final