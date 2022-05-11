{{
    config(
        materialized= 'table',
        unique_key= 'tab_pk' 
    )
}}

with _innings_block as (
  select * from {{ref('pitchers_inning_blocks')}}
), _metrics as (
  select
     _year
    ,pitcher_id
    ,pitcher_full_name
    ,inning_block
    ,_year || inning_block as tab_pk
    ,round(br_spin_rate, 2) as br_spin_rate
    ,round(br_x_axis_movement, 2) as br_x_axis_movement
    ,round(br_z_axis_movement, 2) as br_z_axis_movement
    ,round(br_spin_rate - lag(br_spin_rate, 1) over (partition by _year, pitcher_id order by inning_block), 2) as var_spin_from_1st_block
    ,round(br_spin_rate - lag(br_spin_rate, 2) over (partition by _year, pitcher_id order by inning_block), 2) as var_spin_from_2nd_block
    ,round(br_spin_rate - lag(br_spin_rate, 3) over (partition by _year, pitcher_id order by inning_block), 2) as var_spin_from_3rd_block
    ,round(br_spin_rate - lag(br_spin_rate, 4) over (partition by _year, pitcher_id order by inning_block), 2) as var_spin_from_4th_block
    ,round(br_spin_rate - lag(br_spin_rate, 5) over (partition by _year, pitcher_id order by inning_block), 2) as var_spin_from_5th_block
    ,round(br_spin_rate - lag(br_spin_rate, 6) over (partition by _year, pitcher_id order by inning_block), 2) as var_spin_from_6th_block
    ,round(br_spin_rate - lag(br_spin_rate, 7) over (partition by _year, pitcher_id order by inning_block), 2) as var_spin_from_7th_block
    ,round(br_spin_rate - lag(br_spin_rate, 8) over (partition by _year, pitcher_id order by inning_block), 2) as var_spin_from_8th_block
    ,round(br_spin_rate - lag(br_spin_rate, 9) over (partition by _year, pitcher_id order by inning_block), 2) as var_spin_from_9th_block
    ,round(br_x_axis_movement - lag(br_x_axis_movement, 1) over (partition by _year, pitcher_id order by inning_block), 2) as var_x_ax_from_1st_block
    ,round(br_x_axis_movement - lag(br_x_axis_movement, 2) over (partition by _year, pitcher_id order by inning_block), 2) as var_x_ax_from_2nd_block
    ,round(br_x_axis_movement - lag(br_x_axis_movement, 3) over (partition by _year, pitcher_id order by inning_block), 2) as var_x_ax_from_3rd_block
    ,round(br_x_axis_movement - lag(br_x_axis_movement, 4) over (partition by _year, pitcher_id order by inning_block), 2) as var_x_ax_from_4th_block
    ,round(br_x_axis_movement - lag(br_x_axis_movement, 5) over (partition by _year, pitcher_id order by inning_block), 2) as var_x_ax_from_5th_block
    ,round(br_x_axis_movement - lag(br_x_axis_movement, 6) over (partition by _year, pitcher_id order by inning_block), 2) as var_x_ax_from_6th_block
    ,round(br_x_axis_movement - lag(br_x_axis_movement, 7) over (partition by _year, pitcher_id order by inning_block), 2) as var_x_ax_from_7th_block
    ,round(br_z_axis_movement - lag(br_x_axis_movement, 8) over (partition by _year, pitcher_id order by inning_block), 2) as var_x_ax_from_8th_block
    ,round(br_z_axis_movement - lag(br_x_axis_movement, 9) over (partition by _year, pitcher_id order by inning_block), 2) as var_x_ax_from_9th_block
    ,round(br_z_axis_movement - lag(br_z_axis_movement, 1) over (partition by _year, pitcher_id order by inning_block), 2) as var_z_ax_from_1st_block
    ,round(br_z_axis_movement - lag(br_z_axis_movement, 2) over (partition by _year, pitcher_id order by inning_block), 2) as var_z_ax_from_2nd_block
    ,round(br_z_axis_movement - lag(br_z_axis_movement, 3) over (partition by _year, pitcher_id order by inning_block), 2) as var_z_ax_from_3rd_block
    ,round(br_z_axis_movement - lag(br_z_axis_movement, 4) over (partition by _year, pitcher_id order by inning_block), 2) as var_z_ax_from_4th_block
    ,round(br_z_axis_movement - lag(br_z_axis_movement, 5) over (partition by _year, pitcher_id order by inning_block), 2) as var_z_ax_from_5th_block
    ,round(br_z_axis_movement - lag(br_z_axis_movement, 6) over (partition by _year, pitcher_id order by inning_block), 2) as var_z_ax_from_6th_block
    ,round(br_z_axis_movement - lag(br_z_axis_movement, 7) over (partition by _year, pitcher_id order by inning_block), 2) as var_z_ax_from_7th_block
    ,round(br_z_axis_movement - lag(br_z_axis_movement, 8) over (partition by _year, pitcher_id order by inning_block), 2) as var_z_ax_from_8th_block
    ,round(br_z_axis_movement - lag(br_z_axis_movement, 9) over (partition by _year, pitcher_id order by inning_block), 2) as var_z_ax_from_9th_block
  from _innings_block
),
_final as (
  select * from _metrics
)
select * from _final