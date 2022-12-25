{{
    config(
      materialized= 'table',
      unique_key= 'tab_pk'
    )
}}

with _base_statcast AS (
  select * from {{ref('base_statcast')}}
),
_statcast_events AS (
  select * from {{ref('statcast_events')}}
),
_pitch_types AS (
  select * from {{ref('pitch_types')}}
),
_pa AS (
  select
    game_pk || pitcher_id || batter_id || inning AS plt_apprnc_pk
   ,pitcher_id
   ,pitcher_full_name
   ,game_pk
   ,gm_date
   ,game_year
   ,inning
   ,inning_topbot
   ,at_bat_number
   ,case when _base_statcast._events is not null
         then  _base_statcast._events
         end AS outcome
   ,case when safe_or_out = 'out' then 1
         when safe_or_out = 'safe' then 0
         else null end AS reverse_safe_or_out_bool
   ,sum(reverse_safe_or_out_bool) over (partition by pitcher_id, game_year order by game_pk, inning, at_bat_number) as running_outs_by_pitcher
   from _base_statcast
   left join _statcast_events
     on _base_statcast._events = _statcast_events._events
   where outcome is not null
), 
_innings_partitions as (
  select
    plt_apprnc_pk
   ,pitcher_id
   ,pitcher_full_name
   ,game_pk
   ,gm_date
   ,game_year
   ,inning
   ,inning_topbot
   ,at_bat_number
   ,case when running_outs_by_pitcher between 0 and 60
         then 'a_0_20'
         when running_outs_by_pitcher between 61 and 120
         then 'b_21_40'
         when running_outs_by_pitcher between 121 and 180
         then 'c_41_60'
         when running_outs_by_pitcher between 181 and 240
         then 'd_61_80'
         when running_outs_by_pitcher between 241 and 300
         then 'e_81_100'
         when running_outs_by_pitcher between 301 and 360
         then 'f_101_120'
         when running_outs_by_pitcher between 361 and 420
         then 'g_121_140'
         when running_outs_by_pitcher between 421 and 480
         then 'h_141_160'
         when running_outs_by_pitcher between 481 and 540
         then 'i_161_180'
         when running_outs_by_pitcher between 541 and 600
         then 'j_181_200'
         when running_outs_by_pitcher between 601 and 660
         then 'k_201_220'
         when running_outs_by_pitcher between 661 and 720
         then 'l_221_240'
         end AS inning_block
  from _pa
),
_inning_partition_stats AS (
  select
    a.plt_apprnc_pk
   ,a.game_pk
   ,year(a.gm_date) AS _year
   ,a.gm_date
   ,a.pitcher_id
   ,a.pitcher_full_name
   ,batter_id
   ,batter_full_name
   ,a.inning
   ,a.inning_topbot
   ,inning_block
   ,pitch_type_cond_lvii
   ,release_speed
   ,release_spin_rate
   ,pfx_x
   ,pfx_z
  from _base_statcast a
  join _innings_partitions b
    on a.plt_apprnc_pk = b.plt_apprnc_pk
),
_avg AS (
  select
    pitcher_id
   ,pitcher_full_name
   ,_year
   ,inning_block
   ,pitch_type_cond_lvii
   ,case when pitch_type_cond_lvii = 'fb'
         then avg(release_speed)
         end AS fb_velo
   ,case when pitch_type_cond_lvii = 'fb'
         then avg(release_spin_rate)
         end AS fb_spin_rate
   ,case when pitch_type_cond_lvii = 'fb'
         then avg(pfx_x)
         end AS fb_x_axis_movement
   ,case when pitch_type_cond_lvii = 'fb'
         then avg(pfx_z)
         end AS fb_z_axis_movement
   ,case when pitch_type_cond_lvii = 'br'
         then avg(release_speed)
         end AS br_velo
   ,case when pitch_type_cond_lvii = 'br'
         then avg(release_spin_rate)
         end AS br_spin_rate
   ,case when pitch_type_cond_lvii = 'br'
         then avg(pfx_x)
         end AS br_x_axis_movement
   ,case when pitch_type_cond_lvii = 'br'
         then avg(pfx_z)
         end AS br_z_axis_movement
  from _inning_partition_stats 
  {{ dbt_utils.group_by(5) }}
),
_consolidation AS (
  select
    pitcher_id
   ,pitcher_full_name
   ,_year
   ,inning_block
   ,pitcher_id || _year || inning_block AS tab_pk
    ,round ( max(fb_velo), 2) AS fb_velo
    ,round ( max(fb_spin_rate), 2) AS fb_spin_rate
    ,round ( max(fb_x_axis_movement), 2) AS fb_x_axis_movement
    ,round ( max(fb_z_axis_movement), 2) AS fb_z_axis_movement
    ,round ( max(br_velo), 2) AS br_velo
    ,round ( max(br_spin_rate), 2) AS br_spin_rate
    ,round ( max(br_x_axis_movement), 2) AS br_x_axis_movement
    ,round ( max(br_z_axis_movement), 2) AS br_z_axis_movement
  from _avg
  {{ dbt_utils.group_by(4) }}
),
_variance AS (
  select
     pitcher_id
    ,pitcher_full_name
    ,_year
    ,inning_block
    ,pitcher_id || _year || inning_block AS tab_pk
    ,fb_velo
    ,fb_spin_rate
    ,fb_x_axis_movement
    ,fb_z_axis_movement
    ,br_spin_rate
    ,br_x_axis_movement
    ,br_z_axis_movement
  from _consolidation
  ),
_final AS (
  select * from _variance
  )
select * from _final
