# -*- coding: utf-8 -*-
"""
Created on Wed Dec 05 13:45:16 2018

@author: ALTZ100066
"""

import pandas as pd
import itertools
import os
import random
import numpy as np

filepath = r'D:\suraj.jha\RL\Data\Synthetic Data'
rec_brnd_nbr = 10

yr_mo_df = pd.read_csv(os.path.join(filepath, 'cal_yr_mo_nbr.csv'))
nbr_of_mo = yr_mo_df.shape[0]

brnd_cd_df = pd.read_csv(os.path.join(filepath, 'brand_cd.csv'),
                          header=0,
                          names=['brand_cd'])

brnd_cmb = list(itertools.combinations(brnd_cd_df['brand_cd'], rec_brnd_nbr))
random.shuffle(brnd_cmb)
brnd_cmb = brnd_cmb[:60]

max_occ_recomm = 0.4
p = [max_occ_recomm] + [(1-max_occ_recomm)/(nbr_of_mo-1)]*(nbr_of_mo-1)

recomm_fav = list(brnd_cmb[0])

reco_list = [brnd_cmb[i] for i in np.random.choice(nbr_of_mo, nbr_of_mo, p=p)]
recomm_df = pd.DataFrame(reco_list)
                         
recomm_df = pd.concat([yr_mo_df, recomm_df], axis=1)
recomm_df = pd.melt(recomm_df, id_vars=['﻿cal_yr_mo_nbr']).drop('variable', 
                                                                 axis=1)
                                                            
recomm_df.columns = ['cal_yr_mo_nbr', 'brnd_cd']
recomm_df = recomm_df.sort_values(by=['cal_yr_mo_nbr']).reset_index(drop=True)

#sales reps preferences
sale_rep_fav = ['bdl', 'bud', 'chl', 'mul', 'sta', 'nti', 'bll',
                'mgl', 'gpx', 'est', 'idb', 'hgh', 'kcm', 'rck']
                
recomm_df['sales_exec'] = recomm_df['brnd_cd'].apply(lambda x: 1 if x in sale_rep_fav else 0)

recomm_df.to_csv(os.path.join(filepath, 'recomm_exec_data_v0.csv'), index=False)

#number of times each brand occurs 
print(recomm_df.groupby('brnd_cd')['sales_exec'].count())

#sparsity of execution
print(recomm_df.groupby('cal_yr_mo_nbr')['sales_exec'].sum())

#transposing the data long to wide
recomm_df['Brnd'] = ['B%s' %i for i in range(1, 11)]*60
recomm_df = recomm_df.pivot(index='cal_yr_mo_nbr', columns='Brnd')[['brnd_cd', 'sales_exec']].reset_index()
recomm_df.columns= ['cal_yr_mo_nbr'] + ['B%s' %i for i in range(1, 11)] + ['SB%s' %i for i in range(1, 11)]

recomm_df.to_csv(os.path.join(filepath, 'sales_exec_5yr_dummy_data_v0.csv'), 
                 index=False)









