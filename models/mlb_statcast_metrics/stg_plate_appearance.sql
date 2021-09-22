{{
    config(
      materialized= 'table',
      unique_key= "at_bat_id"
    )
}}

WITH cte_base_statcast AS (
  SELECT * FROM {{ref('base_statcast')}}
), cte_batting_metrics AS (
  SELECT
   gm_date
  ,game_pk
  ,pitcher_id
  ,player_name AS pitcher_full_name
  ,batter_id
  ,batter_full_name
  ,fld_score AS fielding_team_score
  ,bat_score AS batting_score
  ,hit_distance_sc
  ,pitch_number AS pitch_count_in_pa
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
  ,MAX(pitch_number)
FROM base_statcast
WHERE _events IS NOT NULL
  AND _events NOT IN ('wild_pitch', 'passed_ball')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
)