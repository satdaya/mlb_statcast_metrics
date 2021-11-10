WITH cte_base_statcast AS (
  SELECT * FROM {{ref('base_statcast')}}
),
cte_statcast_events AS (
  SELECT * FROM {{ref('statcast_events')}}
),
cte_pitch_types AS (
  SELECT * FROM {{ref('pitch_types')}}
),
cte_pa AS (
  SELECT
    game_pk || pitcher_id || batter_id || inning AS plt_apprnc_pk
   ,pitcher_id
   ,game_pk
   ,inning
   ,at_bat_number
   ,CASE WHEN _events IS NOT NULL
         THEN _events
         END AS outcome
   FROM cte_base_statcast
),
cte_num_batter AS (
  SELECT
    DISTINCT plt_apprnc_pk
   ,DENSE_RANK() OVER (PARTITION BY pitcher_id, game_pk ORDER BY inning, at_bat_number) AS num_of_batters
FROM cte_pa
JOIN cte_statcast_events
  ON cte_pa.outcome = cte_statcast_events._events
WHERE outcome IS NOT NULL
),
cte_times_thru_the_order AS (
  SELECT
   DISTINCT plt_apprnc_pk
  ,num_of_batters
  ,CASE WHEN num_of_batters BETWEEN 1 AND 9
        THEN 1
        WHEN num_of_batters BETWEEN 10 AND 18
        THEN 2
        WHEN num_of_batters BETWEEN 19 AND 27
        THEN 3
        WHEN num_of_batters BETWEEN 28 AND 36
        THEN 4
        WHEN num_of_batters BETWEEN 37 AND 45
        THEN 5
        END AS times_thru_order
  FROM cte_num_batter
),
cte_time_thru_the_order_stats AS (
  SELECT
    a.plt_apprnc_pk
   ,a.game_pk
   ,YEAR(a.gm_date) AS _year
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
  FROM cte_base_statcast a
  JOIN cte_times_thru_the_order b
    ON a.plt_apprnc_pk = b.plt_apprnc_pk
),
cte_avg AS (
  SELECT
    pitcher_id
   ,pitcher_full_name
   ,_year
   ,times_thru_order
   ,pitch_type_cond_lvii
   ,CASE WHEN pitch_type_cond_lvii = 'fb'
         THEN AVG(release_speed)
         END AS fb_velo
   ,CASE WHEN pitch_type_cond_lvii = 'fb'
         THEN AVG(release_spin_rate)
         END AS fb_spin_rate
   ,CASE WHEN pitch_type_cond_lvii = 'fb'
         THEN AVG(pfx_x)
         END AS fb_x_axis_movement
   ,CASE WHEN pitch_type_cond_lvii = 'fb'
         THEN AVG(pfx_z)
         END AS fb_z_axis_movement
   ,CASE WHEN pitch_type_cond_lvii = 'br'
         THEN AVG(release_speed)
         END AS br_velo
   ,CASE WHEN pitch_type_cond_lvii = 'br'
         THEN AVG(release_spin_rate)
         END AS br_spin_rate
   ,CASE WHEN pitch_type_cond_lvii = 'br'
         THEN AVG(pfx_x)
         END AS br_x_axis_movement
   ,CASE WHEN pitch_type_cond_lvii = 'br'
         THEN AVG(pfx_z)
         END AS br_z_axis_movement
  FROM cte_time_thru_the_order_stats
  GROUP BY 1,2,3,4,5
),
cte_consolidation AS (
  SELECT
    pitcher_id
   ,pitcher_full_name
   ,_year
   ,times_thru_order
   ,MAX(fb_velo) AS fb_velo
   ,MAX(fb_spin_rate) AS fb_spin_rate
   ,MAX(fb_x_axis_movement) AS fb_x_axis_movement
   ,MAX(fb_z_axis_movement) AS fb_z_axis_movement
   ,MAX(br_velo) AS br_velo
   ,MAX(br_spin_rate) AS br_spin_rate
   ,MAX(br_x_axis_movement) AS br_x_axis_movement
   ,MAX(br_z_axis_movement) AS br_z_axis_movement
  FROM cte_avg
  GROUP BY 1,2,3,4
  ORDER BY 1,3,4
),
cte_final AS (
  SELECT * FROM cte_consolidation
  )
SELECT * FROM cte_final
