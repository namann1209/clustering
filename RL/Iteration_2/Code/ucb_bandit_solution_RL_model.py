# -*- coding: utf-8 -*-
"""
Created on Fri Dec 14 14:21:37 2018

@author: ALTZ100066
"""
import os
import pandas as pd
import numpy as np


def next_month(x):
    if int(x)%100 == 12:
        return (int(x) + 89)
    else:
        return (int(x) + 1)

def recomm_execution(x):
    if (x['CURRENT_POD'] - x['BASELINE_POD']) > 0:
        if x['ACTION'] == 'INCREASE':
            return 1
        else:
            return 0
    else:
        if x['ACTION'] == 'INCREASE':
            return 0
        else:
            return 1
 

def ucb_bandit(num_occur, avg_reward, round_t, num_recomm_pm):
    return (avg_reward + 2*np.sqrt(num_recomm_pm*(np.log(round_t))/num_occur))      
            

def new_recommendation(df, action, reward, round_col, key):
    global round_t, num_recomm_pm
        
    # ith month from starting of recommendation
    round_t = df[round_col].unique().size + 1
    
    # create UCB(Upper Confidence Bound) dataframe
    ucb_df = df.groupby(action).agg({key:'size', reward:'mean'})\
                .rename(columns={key:'num_occur',
                                 reward:'avg_reward'}).reset_index()         
                                        
    # include all missing combination of brand and action for present brand 
    # with one executed and one not executed
#    all_brnd_action = [(x,y) for x in ucb_df[action[0]].unique()\
#                        for y in ucb_df[action[1]].unique()]
#                                  
#    all_brnd_action_df = pd.DataFrame(all_brnd_action,
#                                      columns=action)
#                                      
#    ucb_df = pd.merge(ucb_df, all_brnd_action_df, on=action, how='outer')
#
#    ucb_df['num_occur'] = ucb_df['num_occur'].fillna(2)                       
#    ucb_df['avg_reward'] = ucb_df['avg_reward'].fillna(0.5) 

    # get UCB index for each action           
    ucb_df['ucb_index'] = ucb_df.apply(lambda x: ucb_bandit(x['num_occur'],\
                                        x['avg_reward'], round_t,\
                                        num_recomm_pm), axis=1)                        
                                      
    ucb_df = ucb_df.sort_values('ucb_index', ascending=False)\
                    .reset_index(drop=True)
    
    ucb_df = ucb_df.drop_duplicates(action[0], keep='first')\
                .reset_index(drop=True)
    
    # get top k recommendation using UCB Index
    if ucb_df.shape[0] >= (num_recomm_pm-1):
#        u_index_thresh = ucb_df.loc[(num_recomm_pm-1)]['ucb_index']
#        new_recomm = ucb_df[ucb_df['ucb_index']>=u_index_thresh][action]
        new_recomm = ucb_df.loc[:(num_recomm_pm-1)][action]
    else:
        new_recomm = ucb_df[action]
    return new_recomm

   
def preprocess_data(df):
    # get all LOLA piloted stores
    df = df[df['PILOT_FLG']==1]
    
    # get whether recommendations executed or not
    df['SALES_EXEC'] = df.apply(recomm_execution, axis=1)
    
    # Filter relevent columns
    df = df[['WSLR_NBR', 'SURV_CWC_WSLR_CUST_PARTY_ID', 'CAL_YR_MO_NBR', 
             'PROD_CD', 'ACTION', 'SALES_EXEC']]
             
    return df
    
    
def long_to_wide(df, round_col, action, reward, key):
    df = pd.pivot_table(df,
                        values=reward,
                        index=[key] + [round_col], 
                        columns=action).reset_index()

    df.columns = [str(s1) + '_' + str(s2) if s2 else str(s1)  for (s1,s2) in df.columns.tolist()]
    return df
    

if __name__ == "__main__":
    
    filepath = r'D:\suraj.jha\RL\Data\Real Data'
    
    action = ['PROD_CD', 'ACTION'] 
    reward = 'SALES_EXEC'
    round_col = 'CAL_YR_MO_NBR'
    key = 'SURV_CWC_WSLR_CUST_PARTY_ID'
    round_t = 0
    
    # number of recommendation per month (k)
    num_recomm_pm = 15
    
    # read raw data
    execution_df = pd.read_csv(os.path.join(filepath, 'execution.csv'))
    
    # preprocess data     
    recom_exec_df = preprocess_data(execution_df)
    
    # save execution data in wide format
    long_to_wide(recom_exec_df, round_col, action, reward, key).to_csv(\
                os.path.join(filepath, 'execution_wide_format.csv'),
                index=False)
    
    # get unique rtlr party ids
    rtlr_list = recom_exec_df[key].unique().tolist()
    
    # create new empty dataframe
    new_reco_df = pd.DataFrame()    
    
    # recommend brand and action for each rtlr
    for rtlr in rtlr_list:
        temp_df = recom_exec_df[recom_exec_df[key]==rtlr]
        
        new_reco = new_recommendation(temp_df, action, reward, round_col, key)                      

        new_reco[round_col] = next_month(temp_df[round_col].max())
        
        new_reco[key] = rtlr

        new_reco_df = new_reco_df.append(new_reco, ignore_index=True)    
    
    wslr_rtlr = recom_exec_df[['WSLR_NBR',
                               'SURV_CWC_WSLR_CUST_PARTY_ID']].drop_duplicates()
                               
    new_reco_df = pd.merge(new_reco_df,
                           wslr_rtlr,
                           how='left',
                           on=key)
                           
    new_reco_df = new_reco_df[['WSLR_NBR', 'SURV_CWC_WSLR_CUST_PARTY_ID',
                               'CAL_YR_MO_NBR', 'PROD_CD', 'ACTION']] 
                           
    new_reco_df.to_csv(os.path.join(filepath, 'new_reco_real_v0.csv'),
                       index=False)
