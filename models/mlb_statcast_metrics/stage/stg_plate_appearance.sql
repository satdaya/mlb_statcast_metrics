{{
    config(
      materialized= 'table',
      unique_key= 'plt_apprnc_pk'
    )
}}

WITH cte_base_statcast AS (
  SELECT * FROM {{ref('base_statcast')}}
), cte_pitch_sequence AS (
  SELECT
     game_pk
    ,pitcher_id
    ,batter_id
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
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
)
plate_appearance cte_batting_metrics AS (
  SELECT
   gm_date
  ,game_year
  ,game_pk
  ,pitcher_id
  ,pitcher_full_name
  ,batter_id
  ,batter_full_name
  ,CASE WHEN inning_topbot = 'Top'
        THEN away_team
        WHEN inning_topbot = 'Bot'
        THEN home_team
        END AS batting_team
  ,CASE WHEN inning_topbot = 'Top'
        THEN home_team
        WHEN inning_topbot = 'Bot'
        THEN away_team
        END AS fielding_team
  ,inning
  ,fld_score AS fielding_team_score
  ,bat_score AS batting_score
  ,hit_distance_sc
  ,pitch_number AS pitch_count_in_pa
  ,_events
  ,game_pk || pitcher_id || batter_id || inning AS plt_apprnc_pk
  ,CASE WHEN _events ILIKE ('%pickoff%')
        THEN 0
        WHEN _events ILIKE ('%steal%')
        THEN 0
        WHEN _events ILIKE ('%stolen%')
        THEN 0
        WHEN _events ILIKE ('%advisory%')
        THEN 0
        WHEN _events ILIKE ('%interf%')
        THEN 0
        WHEN _events = 'passed_ball'
        THEN 0
        WHEN _events = 'wild_pitch'
        THEN 0
        WHEN _events IS NULL
        THEN 0
        ELSE 1
        END AS is_plate_appearance
  ,CASE WHEN _events IN ('single', 'double', 'triple', 'home_run', 'grounded_into_double_play', 'fielders_choice_out', 'triple_play')
        THEN 1
        WHEN _events ILIKE ('%double_play%')
        THEN 1
        WHEN _events ILIKE ('%bunt%')
        THEN 1
        WHEN _events ILIKE ('%out%')
        THEN 1
        WHEN _events ILIKE ('%sac_fly%')
        THEN 1
        WHEN _events ILIKE ('%field_error%')
        THEN 1
        ELSE 0 END
        AS is_at_bat
  ,CASE WHEN is_at_bat = 1
        AND _events IN ('single', 'double', 'triple', 'home_run')
        THEN 1
        ELSE 0
        END AS ab_safe_or_out
  ,CASE WHEN is_plate_appearance = 1
        AND _events IN ('single', 'double', 'triple', 'home_run', 'hit_by_pitch', 'catcher_interf', 'fan_interference', 'walk')
        THEN 1
        ELSE 0
        END AS pa_safe_or_out
  ,CASE WHEN _events = 'single'
        THEN 1
        WHEN _events = 'double'
        THEN 2
        WHEN _events = 'triple'
        THEN 3
        WHEN _events = 'home_run'
        THEN 4
        ELSE 0
        END AS bases_for_slg
  ,CASE WHEN _events = 'hit_by_pitch'
        THEN 1
        ELSE 0 
        END AS hbp
  ,CASE WHEN _events = 'walk'
        THEN 1
        ELSE 0
        END AS walk
  ,CASE WHEN _events = 'intentional_walk'
        THEN 1
        ELSE 0
        END AS ibb
  ,CASE WHEN _events = 'single'
        THEN 1
        ELSE 0
        END AS single
  ,CASE WHEN _events = 'double'
        THEN 1
        ELSE 0
        END AS double
  ,CASE WHEN _events = 'triple'
        THEN 1
        ELSE 0 
        END AS triple
  ,CASE WHEN _events = 'home_run'
        THEN 1
        ELSE 0 
        END AS home_run
  ,CASE WHEN _events ILIKE ('%sac%')
        THEN 1
        ELSE 0 
        END AS sf
  ,CASE WHEN _events ILIKE ('%strikeout%')
        THEN 1
        ELSE 0 
        END AS strikeout
  ,MAX(pitch_number) AS no_of_pitches
FROM cte_base_statcast
WHERE _events IS NOT NULL
  AND _events NOT IN ('wild_pitch', 'passed_ball')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
),
cte_final AS (
  SELECT *
  FROM cte_batting_metrics
)

SELECT *
FROM cte_final
