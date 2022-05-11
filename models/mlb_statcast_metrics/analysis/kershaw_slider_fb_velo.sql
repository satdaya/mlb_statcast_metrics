{{
    config(
        materialized= 'table',
        --unique_key= 'tab_pk' 
    )
}}

with cte_base_statcast as (
  select * from base_statcast
),
cte_statcast_events as (
  select * from statcast_events
),
cte_pitch_types as (
  select * from pitch_types
),
--establish plate appearance with ending event
cte_pa as (
  select
     plt_apprnc_pk
    ,_events
  from cte_base_statcast
  where _events is not null
),
--join unique endings with broader dataset 
cte_pk_dataset as (
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
  from cte_pa a
  join cte_base_statcast b
    on a.plt_apprnc_pk = b.plt_apprnc_pk
), 
cte_aggregates as (
  select
    plt_apprnc_pk
   ,pitcher_id
   ,pitcher_full_name
   ,game_year
   ,pitch_type_cond_lvi_name
   ,_events
   ,case when pitch_type_cond_lvi_name = 'fastball'
         then avg(release_speed) end as avg_fastball_velo
   ,case when pitch_type_cond_lvi_name = 'slider'
         then avg(release_speed) end as avg_slider_velo
  from cte_pk_dataset
  group by 1,2,3,4,5,6
),
--add conditional aggregation
cte_consolidate as (
  select
    plt_apprnc_pk
   ,pitcher_id
   ,pitcher_full_name
   ,game_year
   ,_events
   ,max(avg_fastball_velo) as avg_fastball_velo_
   ,max(avg_slider_velo) as avg_slider_velo_
  from cte_aggregates
  group by 1,2,3,4,5
),
--adding in unique cte to avoid join/aggregate fan out
cte_outcome_join as (
  select
    plt_apprnc_pk
   ,pitcher_id
   ,pitcher_full_name
   ,game_year
   ,a._events
   ,avg_fastball_velo_
   ,avg_slider_velo_
   ,is_ab_bool
   ,ab_safe_or_out_bool
   ,is_pa_bool
   ,pa_safe_or_out_bool
   ,bases_for_slg
from cte_consolidate a 
join cte_statcast_events b
  on a._events = b._events
),
--find varaince between slider and fastball, establish tranches of velocity variances
cte_outcome_aggregate as (
  select 
    plt_apprnc_pk
   ,pitcher_id
   ,pitcher_full_name
   ,game_year
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
         when fb_slider_var between 5 and 7
         then '5 - 7 mph'
         when fb_slider_var between 3 and 5
         then '3 - 5 mph'
         when fb_slider_var between 1 and 3
         then '1 -3 mph'
         when fb_slider_var < 1
         then 'less than 1 mph'
         else null end as velo_var_tranches
  from cte_outcome_join
  where pitcher_full_name ilike ('%kershaw%')
),
--find batting average
cte_getting_close as (
  select 
    pitcher_full_name
   ,game_year
   ,velo_var_tranches
   ,round( sum(ab_safe_or_out_bool) / sum(is_ab_bool), 3) as batting_average
   ,round( sum(pa_safe_or_out_bool) / sum(is_pa_bool), 3) as obp
   ,round( sum(bases_for_slg) / sum(is_ab_bool), 3) as slg_percentages
  from cte_outcome_aggregate
  group by 1,2,3
  order by 2,3
),
cte_final as (
  select * from cte_getting_close
)
select *
from cte_final
