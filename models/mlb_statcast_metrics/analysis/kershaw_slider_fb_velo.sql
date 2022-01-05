WITH cte_base_statcast AS (
  SELECT * FROM base_statcast
),
cte_statcast_events AS (
  SELECT * FROM statcast_events
),
cte_pitch_types AS (
  SELECT * FROM pitch_types
),
--establish plate appearance with ending event
cte_pa AS (
  SELECT
     plt_apprnc_pk
    ,_events
  FROM cte_base_statcast
  WHERE _events IS NOT NULL
),
--join unique endings with broader dataset 
cte_pk_dataset AS (
  SELECT
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
  FROM cte_pa a
  JOIN cte_base_statcast b
    ON a.plt_apprnc_pk = b.plt_apprnc_pk
), 
cte_aggregates AS (
  SELECT
    plt_apprnc_pk
   ,pitcher_id
   ,pitcher_full_name
   ,game_year
   ,pitch_type_cond_lvi_name
   ,_events
   ,CASE WHEN pitch_type_cond_lvi_name = 'fastball'
         THEN AVG(release_speed) END AS avg_fastball_velo
   ,CASE WHEN pitch_type_cond_lvi_name = 'slider'
         THEN AVG(release_speed) END AS avg_slider_velo
  FROM cte_pk_dataset
  GROUP BY 1,2,3,4,5,6
),
--add conditional aggregation
cte_consolidate AS (
  SELECT
    plt_apprnc_pk
   ,pitcher_id
   ,pitcher_full_name
   ,game_year
   ,_events
   ,MAX(avg_fastball_velo) AS avg_fastball_velo_
   ,MAX(avg_slider_velo) AS avg_slider_velo_
  FROM cte_aggregates
  GROUP BY 1,2,3,4,5
),
--adding in unique cte to avoid join/aggregate fan out
cte_outcome_join AS (
  SELECT
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
FROM cte_consolidate a 
JOIN cte_statcast_events b
  ON a._events = b._events
),
--find varaince between slider and fastball, establish tranches of velocity variances
cte_outcome_aggregate AS (
  SELECT 
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
   ,ABS( avg_fastball_velo_ - avg_slider_velo_ ) AS fb_slider_var
   ,CASE WHEN fb_slider_var > 8
         THEN '8 plus'
         WHEN fb_slider_var BETWEEN 5 AND 7
         THEN '5 - 7 mph'
         WHEN fb_slider_var BETWEEN 3 AND 5
         THEN '3 - 5 mph'
         WHEN fb_slider_var BETWEEN 1 AND 3
         THEN '1 -3 mph'
         WHEN fb_slider_var < 1
         THEN 'less than 1 mph'
         ELSE NULL END AS velo_var_tranches
  FROM cte_outcome_join
  WHERE pitcher_full_name ILIKE ('%Kershaw%')
),
--find batting average
cte_getting_close AS (
  SELECT 
    pitcher_full_name
   ,game_year
   ,velo_var_tranches
   ,ROUND( SUM(ab_safe_or_out_bool) / SUM(is_ab_bool), 3) AS batting_average
   ,ROUND( SUM(pa_safe_or_out_bool) / SUM(is_pa_bool), 3) AS obp
   ,ROUND( SUM(bases_for_slg) / SUM(is_ab_bool), 3) AS slg_percentages
  FROM cte_outcome_aggregate
  GROUP BY 1,2,3
  ORDER BY 2,3
),
cte_final AS (
  SELECT * FROM cte_getting_close
)
SELECT *
FROM cte_final
