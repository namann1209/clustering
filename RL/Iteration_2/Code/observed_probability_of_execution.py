# -*- coding: utf-8 -*-
"""
Created on Mon Dec 17 16:27:13 2018

@author: ALTZ100066
"""
import pandas as pd
import numpy as np
import os


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


def observed_proba_of_exec(df):
    pass
    
    
    
if __name__ == "__main__":
    
    filepath = r'D:\suraj.jha\RL\Data\Real Data'
 
    execution_df = pd.read_csv(os.path.join(filepath, 'execution.csv'))

    # get all LOLA piloted stores
    execution_df = execution_df[execution_df['PILOT_FLG']==1]
    
    # get whether recommendations executed or not
    execution_df['SALES_EXEC'] = execution_df.apply(recomm_execution, axis=1)
    
    # Filter relevant columns
    execution_df = execution_df[['SURV_CWC_WSLR_CUST_PARTY_ID',
                                 'CAL_YR_MO_NBR', 'PROD_CD', 'ACTION',
                                 'SALES_EXEC']]

    prob_matrix = execution_df.groupby(['SURV_CWC_WSLR_CUST_PARTY_ID',
                                        'PROD_CD']).agg({'PROD_CD': 'size',\
                                        'SALES_EXEC': 'sum'}).rename(\
                                        columns={'PROD_CD':'num_occur',\
                                        'SALES_EXEC':'num_exec'}).reset_index()                                 
    
    prob_matrix['probability'] = prob_matrix['num_exec']/prob_matrix['num_occur']                        
    
    prob_matrix = prob_matrix.pivot(index='SURV_CWC_WSLR_CUST_PARTY_ID',
                                    columns='PROD_CD', 
                                    values='probability')
    prob_matrix.to_csv(os.path.join(filepath, 'observed_prob_matrix.csv'))                      
    