{{
    config(
        materialized= 'table',
        unique_key= 'prim_key' 
    )
}}

with _base_statcast as (
  select * from base_statcast --limit 10
),
_statcast_events as (
  select * from statcast_events
),
_pitch_types as (
  select * from pitch_types
),
--establish plate appearance with ending event
_pa_all as (
  select
     distinct plt_apprnc_pk
    ,_events
  from _base_statcast
  where _events is not null
),
_pa_fb as (
  select
     distinct plt_apprnc_pk
    ,pitch_type_cond_lvi_name
  from _base_statcast
  where pitch_type_cond_lvi_name = 'fastball'
),
_pa_sl as (
  select
     distinct plt_apprnc_pk
    ,pitch_type_cond_lvi_name
  from _base_statcast
  where pitch_type_cond_lvi_name = 'slider'
),
_pa_rollup as (
  select _pa_all.*
  from _pa_all
  inner join _pa_fb
    on _pa_all.plt_apprnc_pk = _pa_fb.plt_apprnc_pk
  inner join _pa_sl
    on _pa_all.plt_apprnc_pk = _pa_sl.plt_apprnc_pk
),
--join unique endings with broader dataset 
_pk_dataset as (
  select
    a.plt_apprnc_pk
   ,a._events
   ,b.pitcher_id
   ,b.pitcher_full_name
   ,b.game_pk
   ,b.gm_date
   ,b.game_year
   ,b.inning
   ,b.pitch_type_cond_lvi_name
   ,b.release_speed
   ,b.release_spin_rate
  from _pa_rollup a
  join _base_statcast b
    on a.plt_apprnc_pk = b.plt_apprnc_pk
), 
_aggregates as (
  select
    plt_apprnc_pk
   ,pitcher_id
   ,pitcher_full_name
   ,game_year
   ,date_trunc('month', gm_date) as gm_month
   ,pitch_type_cond_lvi_name
   ,_events
   ,case when pitch_type_cond_lvi_name = 'fastball'
         then avg(release_speed) end as avg_fastball_velo
   ,case when pitch_type_cond_lvi_name = 'slider'
         then avg(release_speed) end as avg_slider_velo
  from _pk_dataset
  group by 1,2,3,4,5,6,7
),
--add conditional aggregation
_consolidate as (
  select
    plt_apprnc_pk
   ,pitcher_id
   ,pitcher_full_name
   ,game_year
   ,gm_month
   ,_events
   ,max(avg_fastball_velo) as avg_fastball_velo_
   ,max(avg_slider_velo) as avg_slider_velo_
  from _aggregates
  group by 1,2,3,4,5,6
),
--adding in unique  to avoid join/aggregate fan out
_outcome_join as (
  select
    plt_apprnc_pk
   ,pitcher_id
   ,pitcher_full_name
   ,game_year
   ,gm_month
   ,a._events
   ,avg_fastball_velo_
   ,avg_slider_velo_
   ,is_ab_bool
   ,ab_safe_or_out_bool
   ,is_pa_bool
   ,pa_safe_or_out_bool
   ,bases_for_slg
from _consolidate a 
join _statcast_events b
  on a._events = b._events
),
--find varaince between slider and fastball, establish tranches of velocity variances
_outcome_aggregate as (
  select 
    plt_apprnc_pk
   ,pitcher_id
   ,pitcher_full_name
   ,game_year
   ,gm_month
   ,_events
   ,avg_fastball_velo_
   ,avg_slider_velo_
   ,avg_fastball_velo_
   ,avg_slider_velo_
   ,is_ab_bool
   ,ab_safe_or_out_bool
   ,is_pa_bool
   ,pa_safe_or_out_bool
   ,bases_for_slg
   ,abs( avg_fastball_velo_ - avg_slider_velo_ ) as fb_slider_var
   ,case when fb_slider_var > 8
         then '8 plus'
         when fb_slider_var between 6 and 7
         then '6 - 7 mph'
         when fb_slider_var between 5 and 6
         then '5 - 6 mph'
         when fb_slider_var between 3 and 4
         then '3 - 4 mph'
         when fb_slider_var between 1 and 2
         then '1 - 2 mph'
         when fb_slider_var < 1
         then 'less than 1 mph'
         else null end as velo_var_tranches
  from _outcome_join
  where pitcher_full_name ilike ('%kershaw%')
),
--find batting average
_getting_close as (
  select 
    pitcher_full_name
   ,game_year
   ,gm_month
   ,velo_var_tranches
   ,count(plt_apprnc_pk) as num_of_pa
   ,round( sum(ab_safe_or_out_bool) / sum(is_ab_bool), 3) as batting_average
   ,round( sum(pa_safe_or_out_bool) / sum(is_pa_bool), 3) as obp
   ,round( sum(bases_for_slg) / sum(is_ab_bool), 3) as slg_percentages
  from _outcome_aggregate
  where velo_var_tranches is not null
  group by 1,2,3,4
  order by 2,3
),
closer as (
  select
    *
   ,batting_average  + obp + slg_percentages as ops
  from _getting_close
),
_final as (
  select * from closer
)
select * from _final
