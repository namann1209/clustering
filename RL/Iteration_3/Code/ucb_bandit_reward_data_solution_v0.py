# -*- coding: utf-8 -*-
"""
Created on Fri Jan 11 13:12:19 2019

@author: ALTZ100066
"""

import numpy as np
import pandas as pd
import os          


#Reward logic using rebuy, first buy and stagnate
rebuy_brands = ['abn', 'bud', 'bly', 'sta',
                'hgh', 'blr', 'ntd', 'bsf']

firstbuy_brands = ['chl', 'bll', 'gpx',
                   'nti', 'bws', 'mul',
                   'mgl', 'ibh', 'bla']
       
stagnate_brands  = ['sam', 'bdl', 'blp','rck', 'bsh', 'bkx']

return_brands = ['swa', 'idb', 'kcm', 'est', 'bhl']           
 
               
def reward_fun(x):
    if x in rebuy_brands:
        return 2
    elif x in firstbuy_brands:
        return 1
    elif x in stagnate_brands:
        return 0.5
    else:
        return 0
        
        
def next_month(x):
    if int(x)%100 == 12:
        return (int(x) + 89)
    else:
        return (int(x) + 1)


def ucb_bandit(num_occur, avg_occur, round_t, num_recomm_pm):
    return (avg_occur + 2*np.sqrt(num_recomm_pm*(np.log(round_t))/num_occur))      
            

def new_recommendation(recom_exec_df, action, reward, round_col):
    global round_t, num_recomm_pm, ucb_df
    
    # number of recommendation per month (k)
    num_recomm_pm = int(recom_exec_df[round_col].value_counts().mean()) - 1
        
    # ith month from starting of recommendation
    round_t = recom_exec_df[round_col].unique().size + 1
    
    # create UCB(Upper Confidence Bound) dataframe
    ucb_df = recom_exec_df.groupby(action)\
                .agg({action:'size', reward:'mean'})\
                .rename(columns={action:'num_occur',
                                 reward:'avg_reward'}).reset_index()         
                
    ucb_df['ucb_index'] = ucb_df.apply(lambda x: ucb_bandit(x['num_occur'],\
                                        x['avg_reward'], round_t,\
                                        num_recomm_pm), axis=1)
    
    # get top k recommendation using UCB Index                                 
    new_recomm = ucb_df.sort_values('ucb_index', ascending=False)\
                    .reset_index(drop=True).loc[:num_recomm_pm][[action]]

    return new_recomm
    
    
def long_to_wide(df, round_col, action, reward):
    df['temp'] = ['B%s' %i for i in range(1, (num_recomm_pm + 2))]*round_t
    df = df.pivot(index=round_col, columns='temp')[[action, reward]]\
                .reset_index()
    df.columns= [round_col] + ['B%s' %i for i in range(1, (num_recomm_pm + 2))]\
                + ['SB%s' %i for i in range(1, (num_recomm_pm + 2))]
    return df


if __name__ == "__main__":
    
    filepath = r'D:\suraj.jha\RL\Data\Synthetic Data'
    
    action = 'brnd_cd'
    reward = 'reward'
    round_col = 'cal_yr_mo_nbr'
    t = 15
    round_t = 0
    num_recomm_pm = 0
    ucb_df = pd.DataFrame()
    
    # read raw data
    recom_exec_df = pd.read_csv(os.path.join(filepath,
                                             'recomm_exec_reward_data_v0.csv'))
    
    for _ in range(t):
        new_recomm = new_recommendation(recom_exec_df, action, reward, round_col)                      
        print("New Recommendation : {}".format(new_recomm[action].tolist())) 
                 
        new_recomm[round_col] = next_month(recom_exec_df[round_col].max())
                     
        new_recomm[reward] = new_recomm[action].apply(reward_fun)
        
        recom_exec_df = recom_exec_df.append(new_recomm, ignore_index=True)
       
    final_df = long_to_wide(recom_exec_df, round_col, action, reward)
    final_df.to_csv(os.path.join(filepath, 'new_reco_reward_data_v0.csv'), 
                    index=False)
     
