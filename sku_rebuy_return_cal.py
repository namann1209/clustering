# -*- coding: utf-8 -*-
"""
Created on Fri Jan 25 16:45:41 2019

@author: ALTZ100066
"""

"""
First Buy – execution of a LOLA recommendation by adding a SKU where recommended
Rebuy – Same package was bought again in the 90D period FOLLOWING the initial purchase
Return – Initial SKU registered net negative cases in a rolling 90D period following the First Buy
Stagnation – No selling activity in the 90D period following the most recent purchase of a SKU
"""

import pandas as pd
from connect_to_azure import *


def prev_3_month(x):
    if int(x)%100 == 1:
        return (int(x) - 91)
    else:
        return (int(x) - 3)


db_schema = 'zip_analytics_test'
on_prem_chnl = ('RESTAURANT', 'BAR/TAVERN')
off_prem_chnl = ('CONVENIENCE', 'LIQUOR', 'GROCERY', 'MASS MERCH')

           
lst_mon_qry = "SELECT max([CAL_YR_MO_NBR]) FROM {}.str_sales_extract_all".format(db_schema)
lst_mon = str(int(sql_execute(lst_mon_qry).loc[0]))

lst_3rd_mon = prev_3_month(lst_mon)

str_qry = "SELECT [rtlr_party_id], [CAL_YR_MO_NBR], [BRND_CD], [MKT_LN_CD],\
          [SALES_BBLS], [NET_PRICE] FROM {}.str_sales_extract_all\
          WHERE [CAL_YR_MO_NBR] > {} and [channel] in {}".format(db_schema,
                                                                  lst_3rd_mon,
                                                                  on_prem_chnl)
str_df = sql_execute(str_qry)

lst_6th_mon = prev_3_month(lst_3rd_mon)

rcnt_sales_qry = "SELECT [rtlr_party_id], [CAL_YR_MO_NBR], [BRND_CD],\
                [MKT_LN_CD], [SALES_BBLS] FROM {}.str_sales_extract_all WHERE\
                [CAL_YR_MO_NBR] > {} and [CAL_YR_MO_NBR] <= {} and \
                [channel] in {}".format(db_schema,
                                        lst_6th_mon,
                                        lst_3rd_mon,
                                        on_prem_chnl)

rcnt_sales_df = sql_execute(rcnt_sales_qry)

key_cols = ['rtlr_party_id', 'BRND_CD', 'MKT_LN_CD']

rcnt_sales_df.columns = key_cols + ['recent_sales_bbls']

str_grp = str_df.groupby(key_cols)

str_summ = str_grp.agg({'SALES_BBLS': {'sales_count' : 'count', 
                                       'sales_total' : 'sum' }})
str_summ.columns = [col[1] for col in str_summ.columns]                                       
str_summ = str_summ.reset_index()

str_summ = pd.merge(str_summ, rcnt_sales_df, how='left', on=key_cols)

str_summ['stagnate'] = 0
str_summ.loc[(str_summ.recent_sales_bbls > 0) & (str_summ.sales_total==0),
             'stagnate'] = 1

str_summ['return'] = 0
str_summ.loc[str_summ.sales_total < 0, 'return'] = 1

str_summ['first_buy'] = 0
str_summ.loc[(str_summ.sales_count == 1) & (str_summ.sales_total > 0) &
             ((str_summ.recent_sales_bbls.isnull()) | 
              (str_summ.recent_sales_bbls == 0)), 'first_buy'] = 1
             
str_summ['rebuy'] = 0
str_summ.loc[(str_summ.sales_total > 0) & (str_summ.sales_count > 1),
             'rebuy'] = 1
            
str_summ.loc[(str_summ[['stagnate', 'return', 'first_buy', 
        'rebuy']].sum(axis=1)== 0), 'rebuy'] = 1            
             
             
             
             
             
             
             
             
             
             