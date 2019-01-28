# -*- coding: utf-8 -*-
"""
Created on Wed Jan 23 13:17:38 2019

@author: ALTZ100066
"""
import pandas as pd
from math import sqrt

from connect_to_azure import *

on_premise = True
off_premise = False

if on_premise:
    db_schema = 'zip_analytics_test'
    
    seg_col = "rtlr_party_id,  vip_channel, L3v5_segment, L4v5_segment, \
               L5v5_segment, L5v5_cuisines, vip_channel_mapped"
               
    seg_query = "select {} from {}.segmentation_data_on_prem".format(seg_col,
                                                                     db_schema)
    seg_df = sql_execute(seg_query)
    
      
    cuisines_df = seg_df['L5v5_cuisines'].str.get_dummies(sep=', ')
    
    seg_df = seg_df[["rtlr_party_id", "vip_channel", "L3v5_segment",
                     "L4v5_segment",  "L5v5_segment", "vip_channel_mapped"]]
    
    seg_dummy_df = pd.get_dummies(seg_df)
    
    feature_df = pd.concat([seg_dummy_df, cuisines_df], axis=1)
    feature_df.columns = [col.strip().split('_')[-1] if col!='rtlr_party_id' else 
                          col for col in feature_df.columns]
                          
    feature_df.set_index('rtlr_party_id', inplace=True)
    
    sim_matrix = feature_df.T.corr()

if off_premise:
    pass





