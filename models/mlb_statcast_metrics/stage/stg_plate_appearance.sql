{{
    config(
      materialized= 'table',
      unique_key= 'plt_apprnc_pk'
    )
}}

with _base_statcast as (
  select * from {{ref('base_statcast')}}
),_statcast_events as (
  select * from {{ref('statcast_events')}}
),_pitch_sequence as (
  select
     game_pk
    ,pitcher_id
    ,pitcher_full_name
    ,batter_id
    ,batter_full_name
    ,batting_team
    ,fielding_team
    ,inning
    ,game_pk || pitcher_id || batter_id || inning as plt_apprnc_pk
  from _base_statcast
),
_batting_metrics as (
  select
   gm_date
  ,game_year
  ,game_pk
  ,pitcher_id
  ,pitcher_full_name
  ,batter_id
  ,batter_full_name
  ,batting_team
  ,fielding_team
  ,inning
  ,fld_score as fielding_team_score
  ,bat_score as batting_score
  ,hit_distance_sc as hit_distance
  ,bs._events
  ,game_pk || pitcher_id || batter_id || inning as plt_apprnc_pk
  ,se.is_ab as is_at_bat
  ,se.is_ab_bool as is_at_bat_bool
  ,se.ab_safe_or_out as ab_safe_or_out
  ,se.ab_safe_or_out_bool as ab_safe_or_out_bool
  ,se.is_pa as is_plate_appearance
  ,se.is_pa_bool as is_plate_appearance_bool
  ,se.pa_safe_or_out as pa_safe_or_out
  ,se.pa_safe_or_out_bool as pa_safe_or_out_bool
  ,se.bases_for_slg
  ,case when bs._events = 'hit_by_pitch'
        then 1
        else 0 
        end as hbp
  ,case when bs._events = 'walk'
        then 1
        else 0
        end as walk
  ,case when bs._events = 'intentional_walk'
        then 1
        else 0
        end as ibb
  ,case when bs._events = 'single'
        then 1
        else 0
        end as single
  ,case when bs._events = 'double'
        then 1
        else 0
        end as double
  ,case when bs._events = 'triple'
        then 1
        else 0 
        end as triple
  ,case when bs._events = 'home_run'
        then 1
        else 0 
        end as home_run
  ,case when bs._events ilike ('%sac%')
        then 1
        else 0 
        end as sf
  ,case when bs._events ilike ('%strikeout%')
        then 1
        else 0 
        end as strikeout
  ,max(pitch_number) as no_of_pitches
from _base_statcast bs
join _statcast_events se
  on bs._events = se._events
where bs._events is not null
  and bs._events not in ('wild_pitch', 'passed_ball')
{{ dbt_utils.group_by(25) }}
),
_final as (
  select *
  from _batting_metrics
)

select *
from _final