{{
    config(
      materialized= 'table',
      unique_key= 'tab_pk'
    )
}}

WITH cte_base_statcast AS (
  SELECT * FROM base_statcast
),
cte_statcast_events AS (
  SELECT * FROM statcast_events
),
cte_pitch_types AS (
  SELECT * FROM pitch_types
),
cte_pa AS (
  SELECT
    game_pk || pitcher_id || batter_id || inning AS plt_apprnc_pk
   ,pitcher_id
   ,pitcher_full_name
   ,game_pk
   ,gm_date
   ,game_year
   ,inning
   ,inning_topbot
   ,at_bat_number
   ,CASE WHEN cte_base_statcast._events IS NOT NULL
         THEN  cte_base_statcast._events
         END AS outcome
   ,CASE WHEN safe_or_out = 'out' THEN 1
         WHEN safe_or_out = 'safe' THEN 0
         ELSE NULL END AS reverse_safe_or_out_bool
   ,SUM(reverse_safe_or_out_bool) OVER (PARTITION BY pitcher_id, game_year ORDER BY game_pk, inning, at_bat_number) AS running_outs_by_pitcher
   FROM cte_base_statcast
   LEFT JOIN cte_statcast_events
     ON cte_base_statcast._events = cte_statcast_events._events
   WHERE outcome IS NOT NULL
   ORDER BY 3,6,2
), 
cte_innings_partitions AS (
  SELECT
    plt_apprnc_pk
   ,pitcher_id
   ,pitcher_full_name
   ,game_pk
   ,gm_date
   ,game_year
   ,inning
   ,inning_topbot
   ,at_bat_number
   ,CASE WHEN running_outs_by_pitcher BETWEEN 0 AND 60
         THEN 'a_0_20'
         WHEN running_outs_by_pitcher BETWEEN 61 AND 120
         THEN 'b_21_40'
         WHEN running_outs_by_pitcher BETWEEN 121 AND 180
         THEN 'c_41_60'
         WHEN running_outs_by_pitcher BETWEEN 181 AND 240
         THEN 'd_61_80'
         WHEN running_outs_by_pitcher BETWEEN 241 AND 300
         THEN 'e_81_100'
         WHEN running_outs_by_pitcher BETWEEN 301 AND 360
         THEN 'f_101_120'
         WHEN running_outs_by_pitcher BETWEEN 361 AND 420
         THEN 'g_121_140'
         WHEN running_outs_by_pitcher BETWEEN 421 AND 480
         THEN 'h_141_160'
         WHEN running_outs_by_pitcher BETWEEN 481 AND 540
         THEN 'i_161_180'
         WHEN running_outs_by_pitcher BETWEEN 541 AND 600
         THEN 'j_181_200'
         WHEN running_outs_by_pitcher BETWEEN 601 AND 660
         THEN 'k_201_220'
         WHEN running_outs_by_pitcher BETWEEN 661 AND 720
         THEN 'l_221_240'
         END AS inning_block
  FROM cte_pa
),
cte_inning_partition_stats AS (
  SELECT
    a.plt_apprnc_pk
   ,a.game_pk
   ,YEAR(a.gm_date) AS _year
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
  FROM cte_base_statcast a
  JOIN cte_innings_partitions b
    ON a.plt_apprnc_pk = b.plt_apprnc_pk
),
cte_avg AS (
  SELECT
    pitcher_id
   ,pitcher_full_name
   ,_year
   ,inning_block
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
  FROM cte_inning_partition_stats 
  GROUP BY 1,2,3,4,5
),
cte_consolidation AS (
  SELECT
    pitcher_id
   ,pitcher_full_name
   ,_year
   ,inning_block
   ,pitcher_id || _year || inning_block AS tab_pk
    ,ROUND ( MAX(fb_velo), 2) AS fb_velo
    ,ROUND ( MAX(fb_spin_rate), 2) AS fb_spin_rate
    ,ROUND ( MAX(fb_x_axis_movement), 2) AS fb_x_axis_movement
    ,ROUND ( MAX(fb_z_axis_movement), 2) AS fb_z_axis_movement
    ,ROUND ( MAX(br_velo), 2) AS br_velo
    ,ROUND ( MAX(br_spin_rate), 2) AS br_spin_rate
    ,ROUND ( MAX(br_x_axis_movement), 2) AS br_x_axis_movement
    ,ROUND ( MAX(br_z_axis_movement), 2) AS br_z_axis_movement
  FROM cte_avg
  GROUP BY 1,2,3,4
  ORDER BY 1,3,4
),
cte_variance AS (
  SELECT
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
  FROM cte_consolidation
  ORDER BY 1,3,4
  ),
cte_final AS (
  SELECT * FROM cte_variance
  )
SELECT * FROM cte_final
