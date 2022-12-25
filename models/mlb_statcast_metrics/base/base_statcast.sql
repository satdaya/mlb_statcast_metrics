{{
    config(
      materialized= 'table',
      unique_key= "at_bat_id"
    )
}}

with _raw_statcast as (
  select * from {{ source( 'mlb_statcast_metrics', 'statcast_raw_load') }}
), 
_pitch_types AS(
  select * from {{ref('pitch_types')}}
),
_base_statcast as (
  select 
    _raw_statcast.pitch_type as pitch_type_legacy
   ,pitch_type_new
   ,pitch_type_new_name
   ,pitch_type_cond_lvi
   ,pitch_type_cond_lvi_name
   ,pitch_type_cond_lvii
   ,pitch_type_cond_lvii_name
   ,game_date::date as gm_date
   ,year(gm_date) :: VARCHAR(4) as game_year
   ,release_speed
   ,release_pos_x
   ,release_pos_z
   ,player_name as pitcher_full_name
   ,split_part(player_name, ', ', -1) as pitcher_first_name
   ,split_part(player_name, ', ', 1) as pitcher_last_name
   ,case when des ilike ('%:%')
         then split_part(des, ': ', -1)
         else split_part(des, ' ', 1) || ' ' || split_part(des, ' ', 2)
         end as step_one_batter_name
   ,split_part(step_one_batter_name, ' ', 1) || ' ' || split_part(step_one_batter_name, ' ', 2) as batter_full_name
   ,split_part(batter_full_name, ' ', -1) as batter_first_name
   ,split_part(batter_full_name, ' ', 1) as batter_last_name
   ,batter as batter_id
   ,pitcher as pitcher_id
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
   ,case when inning_topbot = 'Top'
         then away_team
         when inning_topbot = 'Bot'
         then home_team
         end as batting_team
   ,case when inning_topbot = 'Top'
         then home_team
         when inning_topbot = 'Bot'
         then away_team
         end as fielding_team                    
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
   ,round( vx0,11) as vx0
   ,round( vy0,11) as vy0
   ,round( vz0,11) as vz0
   ,round( ax,11) as ax
   ,round( ay,11) as ay
   ,round( az,11) as az
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
   ,at_bat_number::NUMBER(3,0) as at_bat_number            
   ,pitch_number::NUMBER(3,0) as pitch_number                   
   ,pitch_name                     
   ,home_score::NUMBER(3,0) as home_score                  
   ,away_score::NUMBER(3,0) as away_score                    
   ,bat_score::NUMBER(3,0) as bat_score                     
   ,fld_score::NUMBER(3,0) as fld_score                      
   ,post_away_score::NUMBER(3,0) as post_away_score               
   ,post_home_score::NUMBER(3,0) as post_home_score               
   ,post_bat_score::NUMBER(3,0) as post_bat_score
   ,post_fld_score::NUMBER(3,0) as post_fld_score                
   ,if_fielding_alignment           
   ,of_fielding_alignment           
   ,spin_axis                                
   ,delta_home_win_exp
   ,round( delta_run_exp, 4 ) as delta_run_exp
   ,pitcher||batter||at_bat_number||pitch_number||game_pk as at_bat_id
   ,game_pk || pitcher_id || batter_id || inning as plt_apprnc_pk
   ,case when stand = 'R' and zone in ('1', '4', '7')
         then 'inside'
         when stand = 'R' and zone in ('2', '5', '8')
         then 'middle'
         when stand = 'R' and zone in ('3', '6', '9')
         then 'outside'
         when stand = 'L' and zone in ('1', '4', '7')
         then 'outside'
         when stand = 'L' and zone in ('2', '5', '8')
         then 'middle'
         when stand = 'L' and zone in ('3', '6', '9')
         then 'inside'
         when zone in ('11', '13')
         then 'inner half border'
         when zone in ('12', '14')
         then 'outer half border'
         else 'ball'
         end as horizontal_loc
   ,case when zone in ('1', '2', '3')
         then 'low'
         when zone in ('4', '5', '6')
         then 'middle'
         when zone in ('7', '8', '9')
         then 'high'
         when zone in ('11', '12')
         then 'high border'
         when zone in ('11', '12')
         then 'low border'
         else 'ball'
         end as vertical_loc
   ,case when horizontal_loc = 'ball' and vertical_loc = 'ball'
         then 'ball'
         else vertical_loc || ' and ' || horizontal_loc
         end as precision_location
   ,case when pitch_number = 1
         then pitch_type_cond_lvi
         else null
         end as first_pitch
   ,case when pitch_number = 2
         then pitch_type_cond_lvi
         else null 
         end as second_pitch
   ,case when pitch_number = 3 
         then pitch_type_cond_lvi
         else null
         end as third_pitch
   ,case when pitch_number = 4
         then pitch_type_cond_lvi
         else null
         end as fourth_pitch
   ,case when pitch_number = 5 
         then pitch_type_cond_lvi
         else null
         end as fifth_pitch
   ,case when pitch_number = 6
         then pitch_type_cond_lvi
         else null
         end as sixth_pitch
   ,case when pitch_number = 7
         then pitch_type_cond_lvi
         else null
         end as seventh_pitch
   ,case when pitch_number = 8
         then pitch_type_cond_lvi
         else null
         end as eighth_pitch
   ,case when pitch_number = 9
         then pitch_type_cond_lvi
         else null
         end as ninth_pitch
   ,case when pitch_number = 10
         then pitch_type_cond_lvi
         else null
         end as tenth_pitch
   ,case when pitch_number = 11
         then pitch_type_cond_lvi
         else null
         end as eleventh_pitch
   ,case when pitch_number = 12
         then pitch_type_cond_lvi
         else null
         end as twelfth_pitch
   ,case when pitch_number = 13
         then pitch_type_cond_lvi
         else null
         end as thirteenth_pitch
   ,case when pitch_number = 14
         then pitch_type_cond_lvi
         else null
         end as fourteenth_pitch
   ,case when pitch_number = 15
         then pitch_type_cond_lvi
         else null
         end as fifteenth_pitch
   ,case when pitch_number = 16
         then pitch_type_cond_lvi
         else null
         end as sixteenth_pitch
   ,case when pitch_number = 17
         then pitch_type_cond_lvi
         else null
         end as seventeenth_pitch
   ,case when pitch_number = 18
         then pitch_type_cond_lvi
         else null
         end as eighteenth_pitch
   ,case when pitch_number = 19
         then pitch_type_cond_lvi
         else null
         end as nineteenth_pitch
   ,case when pitch_number = 20
         then pitch_type_cond_lvi
         else null
         end as twentieth_pitch
   ,case when pitch_number = 21
         then pitch_type_cond_lvi
         else null
         end as twenty_first_pitch
   ,case when pitch_number = 22
         then pitch_type_cond_lvi
         else null
         end as twenty_second_pitch
   ,case when pitch_number = 23
         then pitch_type_cond_lvi
         else null
         end as twenty_third_pitch
   ,case when pitch_number = 24
         then pitch_type_cond_lvi
         else null
         end as twenty_fourth_pitch
   ,case when pitch_number = 25
         then pitch_type_cond_lvi
         else null
         end as twenty_fifth_pitch
  from _raw_statcast
  left join _pitch_types
    on _raw_statcast.pitch_type = _pitch_types.pitch_type
),
_final as (
  select * from _base_statcast
)

select * from _final
