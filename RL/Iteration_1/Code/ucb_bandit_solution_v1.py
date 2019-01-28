# -*- coding: utf-8 -*-
"""
Created on Fri Dec 07 12:23:15 2018

@author: ALTZ100066
"""

import numpy as np
import pandas as pd
import os


def sales_rep_exec(x):
    sale_rep_fav = ['abn', 'bud', 'est', 'bhl', 'hgh', 'bll', 'mgl', 
                    'blr', 'ntd', 'bsf', 'rck', 'bsh', 'sta', 'ibh']
    if x in sale_rep_fav:
        p = 0.6
    else:
        p = 0.6   
    return np.random.choice([0, 1], size=1, p=[1-p, p])[0]          


def next_month(x):
    if int(x)%100 == 12:
        return (int(x) + 89)
    else:
        return (int(x) + 1)


def ucb_bandit(num_occur, avg_occur, round_t, num_recomm_pm):
    return (avg_occur + 2*np.sqrt(num_recomm_pm*(np.log(round_t))/num_occur))      
            

def new_recommendation(recom_exec_df, action, reward, round_col):
    global round_t, num_recomm_pm
    
    # number of recommendation per month (k)
    num_recomm_pm = int(recom_exec_df[round_col].value_counts().mean()) - 1
        
    # ith month from starting of recommendation
    round_t = recom_exec_df[round_col].unique().size + 1
    
    # create UCB(Upper Confidence Bound) dataframe
    ucb_df = recom_exec_df.groupby(action)\
                .agg({action:'size', reward:'mean'})\
                .rename(columns={action:'num_occur',
                                 reward:'avg_occur'}).reset_index()         
                
    ucb_df['ucb_index'] = ucb_df.apply(lambda x: ucb_bandit(x['num_occur'],\
                                        x['avg_occur'], round_t,\
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
    reward = 'sales_exec'
    round_col = 'cal_yr_mo_nbr'
    t = 60
    round_t = 0
    num_recomm_pm = 0
    
    # read raw data
    recom_exec_df = pd.read_csv(os.path.join(filepath, 'recomm_exec_data_v5.csv'))
    
    for _ in range(t):
        new_recomm = new_recommendation(recom_exec_df, action, reward, round_col)                      
        print("New Recommendation : {}".format(new_recomm[action].tolist())) 
                 
        new_recomm[round_col] = next_month(recom_exec_df[round_col].max())
                     
        new_recomm[reward] = new_recomm[action].apply(sales_rep_exec)
        
        recom_exec_df = recom_exec_df.append(new_recomm, ignore_index=True)
       
    final_df = long_to_wide(recom_exec_df, round_col, action, reward)
    final_df.to_csv(os.path.join(filepath, 'new_recommendations_v5.csv'), 
                    index=False)
     
