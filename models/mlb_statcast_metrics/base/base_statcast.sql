{{
    config(
      materialized= 'table',
      unique_key= "at_bat_id"
    )
}}

WITH cte_raw_statcast AS (
  SELECT * FROM {{ source( 'mlb_statcast_metrics', 'statcast_raw_load') }}
), cte_pitch_types AS(
  SELECT * FROM {{ref('pitch_types')}}
), cte_base_statcast AS (
  SELECT 
   cte_raw_statcast.pitch_type AS pitch_type_legacy
  ,pitch_type_new
  ,pitch_type_new_name
  ,pitch_type_cond_lvi
  ,pitch_type_cond_lvi_name
  ,pitch_type_cond_lvii
  ,pitch_type_cond_lvii_name
  ,game_date::date AS gm_date
  ,YEAR(gm_date) :: VARCHAR(4) AS game_year
  ,release_speed
  ,release_pos_x
  ,release_pos_z
  ,player_name AS pitcher_full_name
  ,SPLIT_PART(player_name, ', ', -1) AS pitcher_first_name
  ,SPLIT_PART(player_name, ', ', 1) AS pitcher_last_name
  ,CASE WHEN des ILIKE ('%:%')
        THEN SPLIT_PART(des, ': ', -1)
        ELSE SPLIT_PART(des, ' ', 1) || ' ' || SPLIT_PART(des, ' ', 2)
        END AS step_one_batter_name
  ,SPLIT_PART(step_one_batter_name, ' ', 1) || ' ' || SPLIT_PART(step_one_batter_name, ' ', 2) AS batter_full_name
  ,SPLIT_PART(batter_full_name, ' ', -1) AS batter_first_name
  ,SPLIT_PART(batter_full_name, ' ', 1) AS batter_last_name
  ,batter AS batter_id
  ,pitcher AS pitcher_id
  ,_events
  ,description
  ,spin_dir
  ,spin_rate_deprecated
  ,break_angle_deprecated
  ,break_length_deprecated
  ,zone
  ,des
  ,game_type                       
  ,stand                           
  ,p_throws                        
  ,home_team                       
  ,away_team
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
  ,type                            
  ,hit_location                    
  ,bb_type                               
  ,balls                           
  ,strikes                   
  ,pfx_x                           
  ,pfx_z                           
  ,plate_x                         
  ,plate_z                         
  ,on_3b                           
  ,on_2b                           
  ,on_1b                           
  ,outs_when_up                   
  ,inning                          
  ,inning_topbot                   
  ,hc_x                           
  ,hc_y                            
  ,tfs_deprecated
  ,tfs_zulu_deprecated
  ,fielder_2
  ,umpire
  ,sv_id
  ,ROUND( vx0,11) AS vx0
  ,ROUND( vy0,11) AS vy0
  ,ROUND( vz0,11) AS vz0
  ,ROUND( ax,11) AS ax
  ,ROUND( ay,11) AS ay
  ,ROUND( az,11) AS az
  ,sz_top                          
  ,sz_bot                          
  ,hit_distance_sc                
  ,launch_speed                    
  ,launch_angle                    
  ,effective_speed                 
  ,release_spin_rate               
  ,release_extension               
  ,game_pk                         
  ,pitcher_1                       
  ,fielder_2_1                     
  ,fielder_3                       
  ,fielder_4                       
  ,fielder_5                       
  ,fielder_6                       
  ,fielder_7                       
  ,fielder_8                       
  ,fielder_9                       
  ,release_pos_y                   
  ,estimated_ba_using_speedangle   
  ,estimated_woba_using_speedangle 
  ,woba_value                      
  ,woba_denom                      
  ,babip_value                     
  ,iso_value                      
  ,launch_speed_angle              
  ,at_bat_number::NUMBER(3,0) AS at_bat_number            
  ,pitch_number::NUMBER(3,0) AS pitch_number                   
  ,pitch_name                     
  ,home_score::NUMBER(3,0) AS home_score                  
  ,away_score::NUMBER(3,0) AS away_score                    
  ,bat_score::NUMBER(3,0) AS bat_score                     
  ,fld_score::NUMBER(3,0) AS fld_score                      
  ,post_away_score::NUMBER(3,0) AS post_away_score               
  ,post_home_score::NUMBER(3,0) AS post_home_score               
  ,post_bat_score::NUMBER(3,0) AS post_bat_score
  ,post_fld_score::NUMBER(3,0) AS post_fld_score                
  ,if_fielding_alignment           
  ,of_fielding_alignment           
  ,spin_axis                                
  ,delta_home_win_exp
  ,ROUND( delta_run_exp, 4 ) AS delta_run_exp
  ,pitcher||batter||at_bat_number||pitch_number||game_pk AS at_bat_id
  ,game_pk || pitcher_id || batter_id || inning as plt_apprnc_pk
  ,CASE WHEN stand = 'R' AND zone IN ('1', '4', '7')
        THEN 'inside'
        WHEN stand = 'R' AND zone IN ('2', '5', '8')
        THEN 'middle'
        WHEN stand = 'R' AND zone IN ('3', '6', '9')
        THEN 'outside'
        WHEN stand = 'L' AND zone IN ('1', '4', '7')
        THEN 'outside'
        WHEN stand = 'L' AND zone IN ('2', '5', '8')
        THEN 'middle'
        WHEN stand = 'L' AND zone IN ('3', '6', '9')
        THEN 'inside'
        WHEN zone IN ('11', '13')
        THEN 'inner half border'
        WHEN zone IN ('12', '14')
        THEN 'outer half border'
        ELSE 'ball'
        END AS horizontal_loc
  ,CASE WHEN zone IN ('1', '2', '3')
        THEN 'low'
        WHEN zone IN ('4', '5', '6')
        THEN 'middle'
        WHEN zone IN ('7', '8', '9')
        THEN 'high'
        WHEN zone IN ('11', '12')
        THEN 'high border'
        WHEN zone IN ('11', '12')
        THEN 'low border'
        ELSE 'ball'
        END AS vertical_loc
  ,CASE WHEN horizontal_loc = 'ball' AND vertical_loc = 'ball'
        THEN 'ball'
        ELSE vertical_loc || ' and ' || horizontal_loc
        END AS precision_location
  ,CASE WHEN pitch_number = 1
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS first_pitch
  ,CASE WHEN pitch_number = 2
        THEN pitch_type_cond_lvi
        ELSE NULL 
        END AS second_pitch
  ,CASE WHEN pitch_number = 3 
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS third_pitch
  ,CASE WHEN pitch_number = 4
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS fourth_pitch
  ,CASE WHEN pitch_number = 5 
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS fifth_pitch
  ,CASE WHEN pitch_number = 6
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS sixth_pitch
  ,CASE WHEN pitch_number = 7
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS seventh_pitch
  ,CASE WHEN pitch_number = 8
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS eighth_pitch
  ,CASE WHEN pitch_number = 9
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS ninth_pitch
  ,CASE WHEN pitch_number = 10
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS tenth_pitch
  ,CASE WHEN pitch_number = 11
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS eleventh_pitch
  ,CASE WHEN pitch_number = 12
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS twelfth_pitch
  ,CASE WHEN pitch_number = 13
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS thirteenth_pitch
  ,CASE WHEN pitch_number = 14
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS fourteenth_pitch
  ,CASE WHEN pitch_number = 15
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS fifteenth_pitch
  ,CASE WHEN pitch_number = 16
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS sixteenth_pitch
  ,CASE WHEN pitch_number = 17
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS seventeenth_pitch
  ,CASE WHEN pitch_number = 18
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS eighteenth_pitch
  ,CASE WHEN pitch_number = 19
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS nineteenth_pitch
  ,CASE WHEN pitch_number = 20
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS twentieth_pitch
  ,CASE WHEN pitch_number = 21
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS twenty_first_pitch
  ,CASE WHEN pitch_number = 22
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS twenty_second_pitch
  ,CASE WHEN pitch_number = 23
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS twenty_third_pitch
  ,CASE WHEN pitch_number = 24
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS twenty_fourth_pitch
  ,CASE WHEN pitch_number = 25
        THEN pitch_type_cond_lvi
        ELSE NULL
        END AS twenty_fifth_pitch
FROM cte_raw_statcast
LEFT JOIN cte_pitch_types
  ON cte_raw_statcast.pitch_type = cte_pitch_types.pitch_type
),
cte_final AS (
  SELECT * FROM cte_base_statcast
)

SELECT * FROM cte_final
