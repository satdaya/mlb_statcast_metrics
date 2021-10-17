{{
    config(
      materialized= 'table',
      unique_key= 'plt_apprnc_pk'
    )
}}

WITH cte_base_statcast AS (
  SELECT * FROM {{ref('base_statcast')}}
),cte_statcast_events AS (
  SELECT * FROM {{ref('statcast_events')}}
),cte_pitch_sequence AS (
  SELECT
     game_pk
    ,pitcher_id
    ,pitcher_full_name
    ,batter_id
    ,batter_full_name
    ,batting_team
    ,fielding_team
    ,inning
    ,game_pk || pitcher_id || batter_id || inning AS plt_apprnc_pk
    ,first_pitch
    ,second_pitch
    ,third_pitch
    ,fourth_pitch
    ,fifth_pitch
    ,sixth_pitch
    ,seventh_pitch
    ,eighth_pitch
    ,ninth_pitch
    ,tenth_pitch
    ,eleventh_pitch
    ,twelfth_pitch
    ,thirteenth_pitch
    ,fourteenth_pitch
    ,fifteenth_pitch
    ,sixteenth_pitch
    ,seventeenth_pitch
    ,eighteenth_pitch
    ,nineteenth_pitch
    ,twentieth_pitch
    ,twenty_first_pitch
    ,twenty_second_pitch
    ,twenty_third_pitch
    ,twenty_fourth_pitch
    ,twenty_fifth_pitch
  FROM cte_base_statcast
),
cte_batting_metrics AS (
  SELECT
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
  ,fld_score AS fielding_team_score
  ,bat_score AS batting_score
  ,hit_distance_sc
  ,pitch_number AS pitch_count_in_pa
  ,bs._events
  ,game_pk || pitcher_id || batter_id || inning AS plt_apprnc_pk
  ,se.is_ab AS is_at_bat
  ,se.is_ab_bool AS is_at_bat_bool
  ,se.ab_safe_or_out AS ab_safe_or_out
  ,se.ab_safe_or_out_bool AS ab_safe_or_out_bool
  ,se.is_pa AS is_plate_appearance
  ,se.is_pa_bool AS is_plate_appearance_bool
  ,se.pa_safe_or_out AS pa_safe_or_out
  ,se.pa_safe_or_out_bool AS pa_safe_or_out_bool
  ,se.bases_for_slg
  ,CASE WHEN bs._events = 'hit_by_pitch'
        THEN 1
        ELSE 0 
        END AS hbp
  ,CASE WHEN bs._events = 'walk'
        THEN 1
        ELSE 0
        END AS walk
  ,CASE WHEN bs._events = 'intentional_walk'
        THEN 1
        ELSE 0
        END AS ibb
  ,CASE WHEN bs._events = 'single'
        THEN 1
        ELSE 0
        END AS single
  ,CASE WHEN bs._events = 'double'
        THEN 1
        ELSE 0
        END AS double
  ,CASE WHEN bs._events = 'triple'
        THEN 1
        ELSE 0 
        END AS triple
  ,CASE WHEN bs._events = 'home_run'
        THEN 1
        ELSE 0 
        END AS home_run
  ,CASE WHEN bs._events ILIKE ('%sac%')
        THEN 1
        ELSE 0 
        END AS sf
  ,CASE WHEN bs._events ILIKE ('%strikeout%')
        THEN 1
        ELSE 0 
        END AS strikeout
  ,MAX(pitch_number) AS no_of_pitches
FROM cte_base_statcast bs
JOIN cte_statcast_events se
  ON bs._events = se._events
WHERE bs._events IS NOT NULL
  AND bs._events NOT IN ('wild_pitch', 'passed_ball')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
),
cte_final AS (
  SELECT *
  FROM cte_batting_metrics
)

SELECT *
FROM cte_final