{{
    config(
      materialized= 'table',
      unique_key= 'tab_pk'
    )
}}

with _base_statcast as (
  select * from {{ref('base_statcast')}}
),
_statcast_events as (
  select * from {{ref('statcast_events')}}
),
_pitch_types as (
  select * from {{ref('pitch_types')}}
),
_pa as (
  select
    game_pk || pitcher_id || batter_id || inning as plt_apprnc_pk
   ,pitcher_id
   ,game_pk
   ,inning
   ,at_bat_number
   ,case when _events is not null
         then _events
         end as outcome
   from _base_statcast
),
_num_batter as (
  select
    distinct plt_apprnc_pk
   ,dense_rank() over (partition by pitcher_id, game_pk order by inning, at_bat_number) as num_of_batters
from _pa
join _statcast_events
  on _pa.outcome = _statcast_events._events
where outcome is not null
),
_times_thru_the_order as (
  select
   distinct plt_apprnc_pk
  ,num_of_batters
  ,case when num_of_batters between 1 and 9
        then 1
        when num_of_batters between 10 and 18
        then 2
        when num_of_batters between 19 and 27
        then 3
        when num_of_batters between 28 and 36
        then 4
        when num_of_batters between 37 and 45
        then 5
        end as times_thru_order
  from _num_batter
),
_time_thru_the_order_stats as (
  select
    a.plt_apprnc_pk
   ,a.game_pk
   ,year(a.gm_date) as _year
   ,gm_date
   ,pitcher_id
   ,pitcher_full_name
   ,batter_id
   ,batter_full_name
   ,inning
   ,inning_topbot
   ,num_of_batters
   ,pitch_type_cond_lvii
   ,b.times_thru_order
   ,release_speed
   ,release_spin_rate
   ,pfx_x
   ,pfx_z
  from _base_statcast a
  join _times_thru_the_order b
    on a.plt_apprnc_pk = b.plt_apprnc_pk
),
_avg as (
  select
    pitcher_id
   ,pitcher_full_name
   ,_year
   ,times_thru_order
   ,pitch_type_cond_lvii
   ,case when pitch_type_cond_lvii = 'fb'
         then avg(release_speed)
         end as fb_velo
   ,case when pitch_type_cond_lvii = 'fb'
         then avg(release_spin_rate)
         end as fb_spin_rate
   ,case when pitch_type_cond_lvii = 'fb'
         then avg(pfx_x)
         end as fb_x_axis_movement
   ,case when pitch_type_cond_lvii = 'fb'
         then avg(pfx_z)
         end as fb_z_axis_movement
   ,case when pitch_type_cond_lvii = 'br'
         then avg(release_speed)
         end as br_velo
   ,case when pitch_type_cond_lvii = 'br'
         then avg(release_spin_rate)
         end as br_spin_rate
   ,case when pitch_type_cond_lvii = 'br'
         then avg(pfx_x)
         end as br_x_axis_movement
   ,case when pitch_type_cond_lvii = 'br'
         then avg(pfx_z)
         end as br_z_axis_movement
  from _time_thru_the_order_stats
  {{ dbt_utils.group_by(5) }}
),
_consolidation as (
  select
    pitcher_id
   ,pitcher_full_name
   ,_year
   ,times_thru_order
   ,pitcher_id || _year || times_thru_order as tab_pk
   ,round ( max(fb_velo), 2) as fb_velo
   ,round ( max(fb_spin_rate), 2) as fb_spin_rate
   ,round ( max(fb_x_axis_movement), 2) as fb_x_axis_movement
   ,round ( max(fb_z_axis_movement), 2) as fb_z_axis_movement
   ,round ( max(br_velo), 2) as br_velo
   ,round ( max(br_spin_rate), 2) as br_spin_rate
   ,round ( max(br_x_axis_movement), 2) as br_x_axis_movement
   ,round ( max(br_z_axis_movement), 2) as br_z_axis_movement
  from _avg
  {{ dbt_utils.group_by(4) }}
),
_variance as (
  select
     pitcher_id
    ,pitcher_full_name
    ,_year
    ,times_thru_order
    ,pitcher_id || _year || times_thru_order as tab_pk
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
