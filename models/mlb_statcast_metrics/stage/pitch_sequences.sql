{{
    config(
      materialized= 'table',
      unique_key= 'plate_appearance'

    )
}}

with _base_statcast as (
  select * from {{ref('base_statcast')}}
),
_statcast_events as (
  select * from {{ref('statcast_events')}}
),
_pitch_number as (
  select
     game_pk
    ,pitcher_id
    ,pitcher_full_name
    ,batter_id
    ,batter_full_name
    ,inning
    ,pitch_number
    ,pitch_type_cond_lvi
    ,_events
    ,game_pk || pitcher_id || batter_id || inning  as plt_apprnc_pk
    ,max(case when pitch_number = 1 
              then pitch_type_cond_lvi
              end) as first_pitch 
    ,max(case when pitch_number = 2
              then pitch_type_cond_lvi
              end) as second_pitch 
    ,max(case when pitch_number = 3 
              then pitch_type_cond_lvi
              end) as third_pitch 
    ,max(case when pitch_number = 4
              then pitch_type_cond_lvi
              end) as fourth_pitch  
    ,max(case when pitch_number = 5
              then pitch_type_cond_lvi
              end) as fifth_pitch   
    ,max(case when pitch_number = 6
              then pitch_type_cond_lvi
              end) as sixth_pitch  
    ,max(case when pitch_number = 7
              then pitch_type_cond_lvi
              end) as seventh_pitch 
    ,max(case when pitch_number = 8
              then pitch_type_cond_lvi
              end) as eighth_pitch
    ,max(case when pitch_number = 9
              then pitch_type_cond_lvi
              end) as ninth_pitch
    ,max(case when pitch_number = 10
              then pitch_type_cond_lvi
              end) as tenth_pitch
    ,max(case when pitch_number = 11
              then pitch_type_cond_lvi
              end) as eleventh_pitch
    ,max(case when pitch_number = 12
              then pitch_type_cond_lvi
              end) as twelfth_pitch
    ,max(case when pitch_number = 13
              then pitch_type_cond_lvi
              end) as thirteenth_pitch
    ,max(case when pitch_number = 14
              then pitch_type_cond_lvi
              end) as fourteenth_pitch
    ,max(case when pitch_number = 15
              then pitch_type_cond_lvi
              end) as fifteenth_pitch
    ,max(case when pitch_number = 16
              then pitch_type_cond_lvi
              end) as sixteenth_pitch
    ,max(case when pitch_number = 17
              then pitch_type_cond_lvi
              end) as seventeenth_pitch
    ,max(case when pitch_number = 18
              then pitch_type_cond_lvi
              end) as eighteenth_pitch
    ,max(case when pitch_number = 19
              then pitch_type_cond_lvi
              end) as nineteenth_pitch
    ,max(case when pitch_number = 20
              then pitch_type_cond_lvi
              end) as twentieth_pitch
    ,max(case when pitch_number = 21
              then pitch_type_cond_lvi
              end) as twenty_first_pitch
    ,max(case when pitch_number = 22
              then pitch_type_cond_lvi
              end) as twenty_second_pitch
    ,max(case when pitch_number = 23
              then pitch_type_cond_lvi
              end) as twenty_third_pitch
    ,max(case when pitch_number = 24
              then pitch_type_cond_lvi
              end) as twenty_fourth_pitch
    ,max(case when pitch_number = 25
              then pitch_type_cond_lvi
              end) as twenty_fifth_pitch
  from _base_statcast
  {{ dbt_utils.group_by(9) }}
  ),
_outcome as (
  select
     game_pk
    ,pitcher_id
    ,batter_id
    ,inning
    ,game_pk || pitcher_id || batter_id || inning  as plt_apprnc_pk
    ,case when _events is not null
          then _events
          end as outcome
  from base_statcast
  where _events is not null
   ),
_condensed as (
  select
    DISTINCT pn.plt_apprnc_pk
    ,pn.game_pk
    ,pn.pitcher_id
    ,pitcher_full_name
    ,pn.batter_id
    ,batter_full_name
    ,pn.inning
    ,o.outcome
    ,se.is_ab as is_at_bat
    ,se.is_ab_bool as is_at_bat_bool
    ,se.ab_safe_or_out as ab_safe_or_out
    ,se.ab_safe_or_out_bool as ab_safe_or_out_bool
    ,se.is_pa as is_plate_appearance
    ,se.is_pa_bool as is_plate_appearance_bool
    ,se.pa_safe_or_out as pa_safe_or_out
    ,se.pa_safe_or_out_bool as pa_safe_or_out_bool
    ,se.bases_for_slg
    ,ifnull(max(first_pitch), '' ) as first_pitch
    ,ifnull(max(second_pitch), '' ) as second_pitch
    ,ifnull(max(third_pitch), '' ) as third_pitch
    ,ifnull(max(fourth_pitch), '' ) as fourth_pitch
    ,ifnull(max(fifth_pitch), '' ) as fifth_pitch
    ,ifnull(max(sixth_pitch), '' ) as sixth_pitch
    ,ifnull(max(seventh_pitch), '' ) as seventh_pitch
    ,ifnull(max(eighth_pitch), '' ) as eighth_pitch
    ,ifnull(max(ninth_pitch), '' ) as ninth_pitch
    ,ifnull(max(tenth_pitch), '' ) as tenth_pitch
    ,ifnull(max(eleventh_pitch), '' ) as eleventh_pitch
    ,ifnull(max(twelfth_pitch), '' ) as twelfth_pitch
    ,ifnull(max(thirteenth_pitch), '' ) as thirteenth_pitch
    ,ifnull(max(fourteenth_pitch), '' ) as fourteenth_pitch
    ,ifnull(max(fifteenth_pitch), '' ) as fifteenth_pitch
    ,ifnull(max(sixteenth_pitch), '' ) as sixteenth_pitch
    ,ifnull(max(seventeenth_pitch), '' ) as seventeenth_pitch
    ,ifnull(max(eighteenth_pitch), '' ) as eighteenth_pitch
    ,ifnull(max(nineteenth_pitch), '' ) as nineteenth_pitch
    ,ifnull(max(twentieth_pitch), '' ) as twentieth_pitch
    ,ifnull(max(twenty_first_pitch), '' ) as twenty_first_pitch
    ,ifnull(max(twenty_second_pitch), '' ) as twenty_second_pitch
    ,ifnull(max(twenty_third_pitch), '' ) as twenty_third_pitch
    ,ifnull(max(twenty_fourth_pitch), '' ) as twenty_fourth_pitch
    ,ifnull(max(twenty_fifth_pitch), '' ) as twenty_fifth_pitch
  from _pitch_number pn
  join _outcome o
    on pn.plt_apprnc_pk = o.plt_apprnc_pk
  join _statcast_events se
    on o.outcome = se._events
    {{ dbt_utils.group_by(17) }}
),
_pitch_sequence as (
  select *
      ,rtrim( (first_pitch || ' - ' || second_pitch ||  ' - ' || third_pitch ||  ' - ' || fourth_pitch ||  ' - ' || fifth_pitch ||  ' - ' || sixth_pitch ||
      ' - ' || seventh_pitch  || ' - ' || eighth_pitch  ||  ' - ' || ninth_pitch ||  ' - ' || tenth_pitch ||' - ' || eleventh_pitch || ' - ' ||  twelfth_pitch || 
      ' - ' || thirteenth_pitch  ||  ' - ' || fourteenth_pitch ||  ' - ' || fifteenth_pitch || ' - ' ||  sixteenth_pitch ||  ' - ' || seventeenth_pitch || 
      ' - ' || eighteenth_pitch  ||  ' - ' || nineteenth_pitch ||  ' - ' || twentieth_pitch ||  ' - ' || twenty_first_pitch ||  ' - ' || twenty_second_pitch ||
      ' - ' || twenty_third_pitch ||  ' - ' || twenty_fourth_pitch ||  ' - ' || twenty_fifth_pitch ), ' - ' ) as pitch_sequence
    from _condensed
),
_final as (
  select * from _pitch_sequence
)  
select * from _final