{{
    config(
      materialized= 'table',
      unique_key= 'plate_appearance'

    )
}}

WITH cte_pitch_number AS (
  SELECT
     game_pk
    ,pitcher_id
    ,batter_id
    ,inning
    ,pitch_number
    ,pitch_type
    ,game_pk || pitcher_id || batter_id || inning  AS plate_appearance
    ,MAX(CASE WHEN pitch_number = 1 
              THEN pitch_type
              END) AS first_pitch 
    ,MAX(CASE WHEN pitch_number = 2
              THEN pitch_type
              END) AS second_pitch 
    ,MAX(CASE WHEN pitch_number = 3 
              THEN pitch_type
              END) AS third_pitch 
    ,MAX(CASE WHEN pitch_number = 4
              THEN pitch_type
              END) AS fourth_pitch  
    ,MAX(CASE WHEN pitch_number = 5
              THEN pitch_type
              END) AS fifth_pitch   
    ,MAX(CASE WHEN pitch_number = 6
              THEN pitch_type
              END) AS sixth_pitch  
    ,MAX(CASE WHEN pitch_number = 7
              THEN pitch_type
              END) AS seventh_pitch 
    ,MAX(CASE WHEN pitch_number = 8
              THEN pitch_type
              END) AS eighth_pitch
    ,MAX(CASE WHEN pitch_number = 9
              THEN pitch_type
              END) AS ninth_pitch
    ,MAX(CASE WHEN pitch_number = 10
              THEN pitch_type
              END) AS tenth_pitch
    ,MAX(CASE WHEN pitch_number = 11
              THEN pitch_type
              END) AS eleventh_pitch
    ,MAX(CASE WHEN pitch_number = 12
              THEN pitch_type
              END) AS twelfth_pitch
    ,MAX(CASE WHEN pitch_number = 13
              THEN pitch_type
              END) AS thirteenth_pitch
    ,MAX(CASE WHEN pitch_number = 14
              THEN pitch_type
              END) AS fourteenth_pitch
    ,MAX(CASE WHEN pitch_number = 15
              THEN pitch_type
              END) AS fifteenth_pitch
    ,MAX(CASE WHEN pitch_number = 16
              THEN pitch_type
              END) AS sixteenth_pitch
    ,MAX(CASE WHEN pitch_number = 17
              THEN pitch_type
              END) AS seventeenth_pitch
    ,MAX(CASE WHEN pitch_number = 18
              THEN pitch_type
              END) AS eighteenth_pitch
    ,MAX(CASE WHEN pitch_number = 19
              THEN pitch_type
              END) AS nineteenth_pitch
    ,MAX(CASE WHEN pitch_number = 20
              THEN pitch_type
              END) AS twentieth_pitch
    ,MAX(CASE WHEN pitch_number = 21
              THEN pitch_type
              END) AS twenty_first_pitch
    ,MAX(CASE WHEN pitch_number = 22
              THEN pitch_type
              END) AS twenty_second_pitch
    ,MAX(CASE WHEN pitch_number = 23
              THEN pitch_type
              END) AS twenty_third_pitch
    ,MAX(CASE WHEN pitch_number = 24
              THEN pitch_type
              END) AS twenty_fourth_pitch
    ,MAX(CASE WHEN pitch_number = 25
              THEN pitch_type
              END) AS twenty_fifth_pitch
  FROM base_statcast
  GROUP BY 1,2,3,4,5,6,7
  ),
cte_condensed AS (
  SELECT
    DISTINCT plate_appearance
    ,IFNULL(MAX(first_pitch), '' ) AS first_pitch
    ,IFNULL(MAX(second_pitch), '' ) AS second_pitch
    ,IFNULL(MAX(third_pitch), '' ) AS third_pitch
    ,IFNULL(MAX(fourth_pitch), '' ) AS fourth_pitch
    ,IFNULL(MAX(fifth_pitch), '' ) AS fifth_pitch
    ,IFNULL(MAX(sixth_pitch), '' ) AS sixth_pitch
    ,IFNULL(MAX(seventh_pitch), '' ) AS seventh_pitch
    ,IFNULL(MAX(eighth_pitch), '' ) AS eighth_pitch
    ,IFNULL(MAX(ninth_pitch), '' ) AS ninth_pitch
    ,IFNULL(MAX(tenth_pitch), '' ) AS tenth_pitch
    ,IFNULL(MAX(eleventh_pitch), '' ) AS eleventh_pitch
    ,IFNULL(MAX(twelfth_pitch), '' ) AS twelfth_pitch
    ,IFNULL(MAX(thirteenth_pitch), '' ) AS thirteenth_pitch
    ,IFNULL(MAX(fourteenth_pitch), '' ) AS fourteenth_pitch
    ,IFNULL(MAX(fifteenth_pitch), '' ) AS fifteenth_pitch
    ,IFNULL(MAX(sixteenth_pitch), '' ) AS sixteenth_pitch
    ,IFNULL(MAX(seventeenth_pitch), '' ) AS seventeenth_pitch
    ,IFNULL(MAX(eighteenth_pitch), '' ) AS eighteenth_pitch
    ,IFNULL(MAX(nineteenth_pitch), '' ) AS nineteenth_pitch
    ,IFNULL(MAX(twentieth_pitch), '' ) AS twentieth_pitch
    ,IFNULL(MAX(twenty_first_pitch), '' ) AS twenty_first_pitch
    ,IFNULL(MAX(twenty_second_pitch), '' ) AS twenty_second_pitch
    ,IFNULL(MAX(twenty_third_pitch), '' ) AS twenty_third_pitch
    ,IFNULL(MAX(twenty_fourth_pitch), '' ) AS twenty_fourth_pitch
    ,IFNULL(MAX(twenty_fifth_pitch), '' ) AS twenty_fifth_pitch
  FROM cte_pitch_number
  GROUP BY 1
),
cte_pitch_sequence AS (
  SELECT *
      ,RTRIM( (first_pitch || ' - ' || second_pitch ||  ' - ' || third_pitch ||  ' - ' || fourth_pitch ||  ' - ' || fifth_pitch ||  ' - ' || sixth_pitch ||
      ' - ' || seventh_pitch  || ' - ' || eighth_pitch  ||  ' - ' || ninth_pitch ||  ' - ' || tenth_pitch ||' - ' || eleventh_pitch || ' - ' ||  twelfth_pitch || 
      ' - ' || thirteenth_pitch  ||  ' - ' || fourteenth_pitch ||  ' - ' || fifteenth_pitch || ' - ' ||  sixteenth_pitch ||  ' - ' || seventeenth_pitch || 
      ' - ' || eighteenth_pitch  ||  ' - ' || nineteenth_pitch ||  ' - ' || twentieth_pitch ||  ' - ' || twenty_first_pitch ||  ' - ' || twenty_second_pitch ||
      ' - ' || twenty_third_pitch ||  ' - ' || twenty_fourth_pitch ||  ' - ' || twenty_fifth_pitch ), ' - ' ) AS pitch_sequence
    FROM cte_condensed
),
cte_final AS (
  SELECT *
  FROM cte_pitch_sequence
)  
SELECT *
FROM cte_pitch_sequence