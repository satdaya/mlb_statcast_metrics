{{
    config(
      materialized= 'table',
      unique_key= 'dist_pitch_seq'
    )
}}

WITH cte_pitch_sequence AS (
  SELECT * FROM {{ref('pitch_sequences')}}
),
cte_distinct_pitch_sequence AS (
  SELECT
     pitch_sequence AS dist_pitch_seq
    ,COUNT (dist_pitch_seq) AS sum_sequence_used
    ,CASE WHEN SUM(is_at_bat_bool) = 0
          THEN NULL
          ELSE ROUND( SUM(ab_safe_or_out_bool) / SUM(is_at_bat_bool) , 3) 
          END AS batting_avg
    ,CASE WHEN SUM(is_plate_appearance_bool) = 0
          THEN NULL
          ELSE ROUND( SUM(pa_safe_or_out_bool) / SUM(is_plate_appearance_bool), 3)
          END AS obp --on base percentage
    ,CASE WHEN SUM(is_at_bat_bool) = 0
          THEN NULL
          ELSE ROUND( SUM(bases_for_slg) / SUM(is_at_bat_bool), 3)
          END AS slg_percentage
    ,obp + slg_percentage AS ops
   FROM cte_pitch_sequence
  GROUP BY 1
  ORDER BY 2 DESC
  ),
cte_final AS ( 
  SELECT * FROM cte_distinct_pitch_sequence
)

SELECT * FROM cte_final