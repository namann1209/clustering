# -*- coding: utf-8 -*-
"""
Created on Tue Jun 19 15:19:36 2018

@author: ALTY100048
"""
<<<<<<< HEAD
#LOLA_BRND_PROD_XREF test edit
=======
#LOLA_BRND_PROD_XREF edit test
#what like this?
>>>>>>> d9ea80d38231efdab88e7b4b08dda873219cae1c

from __future__ import print_function
import copy
import time
import pickle
import os, sys
import sklearn
import traceback
import gc
import numpy as np
import pandas as pd
from sklearn.cross_validation import train_test_split
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier

sys.path.extend(['C:\\BlockParty\\SrvApps\\ZipAnalytics_TEST\\Feature_Engineering'])

from column_util import *
from connect_to_azure import *
#from zip_analytics_test_creation_bir import get_bir_zip_analytics_tests
###########################################################################

t1 = time.time()
###########################################################################


channel=(sys.argv[1].split(','))
#channel name
channel=channel[0]
#yes if mpd needed to be executed
require_mpd=channel[1]
#yes if cluster needed to be executed
cluster=channel[2]
#Yes if bud needed to be executed
bud=channel[3]
#refresh end month
latest_month = channel[4]
#first month of the quarter
simulation_start = channel[5]

##Retailer party ID to retailer number
rtlr_map=sql_execute("select rtlr_party_id, rtlr_num, channel from zip_analytics_test.rtlr_geo_lookup where rtlr_party_id=rtlr_num", create_connection())
rtlr_map.loc[~rtlr_map.channel.isnull(),:].to_csv('D:/harsh.vardhana/jan_fe/quick_access_rtlr_ch.csv',index=True)
rtlr_geo_map=sql_execute("select rtlr_num, zip, state_cd, dma_key, wslr_nbr, chain_ind from zip_analytics_test.rtlr_geo_lookup where rtlr_party_id=rtlr_num", create_connection())
ext = sql_execute(
        "select xcyea06v001, cyec17v001, xcya08v002, xcyfem, rtlr_num from zip_analytics_test.rtlr_num_external_data", create_connection())



###Get Lola Group to Segment mapping

seg_map = sql_execute("SELECT distinct brnd_cd ,prod_cd lola_grp, wamp_nm New_Brand_segment from  zip_analytics_test.lola_brnd_prod_xref", create_connection())
seg_map['segment']='Value'
seg_map.loc[seg_map['New_Brand_segment'].str.contains('FMB'),'segment']='FMB'
seg_map.loc[seg_map['New_Brand_segment'].str.contains('Core'),'segment']='Core'
seg_map.loc[seg_map['New_Brand_segment'].str.contains('Prem'),'segment']='Core'
seg_map.loc[seg_map['New_Brand_segment'].str.contains('Alc'),'segment']='NA'
seg_map.loc[seg_map['New_Brand_segment'].str.contains('Super'),'segment']='Premium'

seg_map['lola_grp']=seg_map['lola_grp'].str.lower()

seg_map1=seg_map[['lola_grp','segment','brnd_cd']].groupby(['lola_grp','segment'],as_index=False).count()
seg_map1=seg_map1.sort_values(['lola_grp','brnd_cd'],ascending=[True,False])
seg_map1=seg_map1[['lola_grp','segment']].groupby('lola_grp',as_index=False).first()



#Off Premise##
##########################Volume and Volume Related##############################################
###########################
if channel=='convSF' or channel=='grocLF' or channel=='pkgliqSF':
    
    if channel=='convSF':
        seg_out = sql_execute("select rtlr_party_id, lola_grp, cal_yr_mo_nbr, sum(sales_bbls) as vol_sales, sum(net_price) as price from (select rtlr_party_id, brnd_cd, cal_yr_mo_nbr, sales_bbls, net_price from zip_analytics_test.str_sales_extract_all where channel='CONVENIENCE') as a inner join (select brnd_cd, prod_cd lola_grp from zip_analytics_test.lola_brnd_prod_xref) as b on a.brnd_cd=b.brnd_cd group by rtlr_party_id, lola_grp, cal_yr_mo_nbr", create_connection())
    if channel=='grocLF':
        seg_out = sql_execute("select rtlr_party_id, lola_grp, cal_yr_mo_nbr, sum(sales_bbls) as vol_sales, sum(net_price) as price from (select rtlr_party_id, brnd_cd, cal_yr_mo_nbr, sales_bbls, net_price from zip_analytics_test.str_sales_extract_all where channel='GROCERY' or channel='MASS MERCH') as a inner join (select brnd_cd, prod_cd lola_grp from zip_analytics_test.lola_brnd_prod_xref) as b on a.brnd_cd=b.brnd_cd group by rtlr_party_id, lola_grp, cal_yr_mo_nbr", create_connection())
    if channel=='pkgliqSF':
        seg_out = sql_execute("select rtlr_party_id, lola_grp, cal_yr_mo_nbr, sum(sales_bbls) as vol_sales, sum(net_price) as price from (select rtlr_party_id, brnd_cd, cal_yr_mo_nbr, sales_bbls, net_price from zip_analytics_test.str_sales_extract_all where channel='LIQUOR') as a inner join (select brnd_cd, prod_cd lola_grp from zip_analytics_test.lola_brnd_prod_xref) as b on a.brnd_cd=b.brnd_cd group by rtlr_party_id, lola_grp, cal_yr_mo_nbr", create_connection())    
    
    
    #get rtlr_num
    seg_out=seg_out.merge(rtlr_map[['rtlr_party_id','rtlr_num']].drop_duplicates(),on='rtlr_party_id',how='left')
    seg_out['lola_grp']=seg_out['lola_grp'].str.lower()
    seg_out=seg_out.loc[~seg_out.rtlr_num.isnull(),:]
    
    #PTR Per BBLS
    seg_out['ptr']=seg_out['price']/seg_out['vol_sales']
    
    #year and month
    seg_out['cal_yr_mo_nbr']=seg_out['cal_yr_mo_nbr'].astype(int)
    seg_out['year']=seg_out.cal_yr_mo_nbr/100
    seg_out['year']=seg_out['year'].round(decimals=0)
    seg_out['month']=seg_out['cal_yr_mo_nbr'] - (seg_out['year']*100)
    time_ser=pd.DataFrame(seg_out[['cal_yr_mo_nbr']].drop_duplicates().sort_values(by='cal_yr_mo_nbr',axis=0,ascending=True)).reset_index(drop=True)
    time_ser['time']=range(1,(time_ser.shape[0]+1))
    seg_out=seg_out.merge(time_ser,on='cal_yr_mo_nbr',how='left')    
    
    start_year=seg_out.year.min()
    final_year=seg_out.year.max()

    
    #calculate brand sales of previous year
    long_trend=pd.DataFrame()
    
    for i in range(13,(time_ser.time.max()+2)):
        temp=seg_out.loc[np.logical_and(seg_out.time > (i -13),seg_out.time< (i)) ,['rtlr_num','lola_grp','vol_sales']].groupby(['rtlr_num','lola_grp'],as_index=False).sum()
        temp['time']=i
        long_trend=long_trend.append(temp)
    
    brand_lev=long_trend.merge(seg_out[['time','cal_yr_mo_nbr','month','year']].drop_duplicates(),on='time',how='left')
    brand_lev.loc[brand_lev.cal_yr_mo_nbr.isnull(),'cal_yr_mo_nbr']=brand_lev.cal_yr_mo_nbr.max() + 1
    brand_lev['year']=brand_lev['cal_yr_mo_nbr']/100
    brand_lev['year']=brand_lev['year'].round(decimals=0)
    brand_lev['month']=brand_lev['cal_yr_mo_nbr'] - (brand_lev['year']*100)
    brand_lev.drop(['time','month','year'],axis=1,inplace=True)
    ####
    
    
    
    #dma_key for calculation of regional level dynamics
    seg_out=seg_out.merge(rtlr_geo_map[['rtlr_num','dma_key','wslr_nbr','state_cd']],on='rtlr_num',how='inner')
    seg_out=seg_out.loc[~seg_out.wslr_nbr.isnull(),:]
    
    #fill in missing DMAs
    seg_out.rtlr_num=seg_out.rtlr_num.astype(int)
    seg_out.loc[seg_out.dma_key.isnull(),'dma_key']=seg_out.loc[seg_out.dma_key.isnull(),'wslr_nbr']
    
    
    
    trending=seg_out[['dma_key','year','month','lola_grp','time','vol_sales']].groupby(['dma_key','year','month','time','lola_grp'],as_index=False).sum()
    
    #trending[['year','vol_sales']].groupby('year').sum()
    tr2=trending.copy(deep=True)
    tr2['year']=tr2['year']+1
    trending=trending.merge(tr2[['dma_key','year','month','lola_grp','vol_sales']].rename(columns={'vol_sales':'pre_sale'}),on=['dma_key','year','month','lola_grp'],how='left')
    trending.fillna(0,inplace=True)
    trending=trending.loc[trending.time>12,:]
    
    trending['delta']=(trending['vol_sales'] - trending['pre_sale'])
    tr2=trending.copy(deep=True)
    tr2['month']=tr2['month']+1
    tr2.loc[tr2.month==13,'year']=tr2.loc[tr2.month==13,'year']+1
    tr2.loc[tr2.month==13,'month']=1
    
    trending=trending.merge(tr2[['dma_key','year','month','lola_grp','delta']].rename(columns={'delta':'delta_ll'}),on=['dma_key','year','month','lola_grp'],how='outer')
    trending=trending.loc[trending.time>13,['dma_key','year','month','lola_grp','delta','delta_ll']]
    trending.rename(columns={'delta':'recency_offset','delta_ll':'recency_offset2'},inplace=True)
    #trending['recency_offset']=trending['delta']*0.70 +trending['delta_ll']*0.30
    trending.fillna(0,inplace=True)
    trending['month']=trending['month']+1
    trending.loc[trending.month==13,'year']=trending.loc[trending.month==13,'year']+1
    trending.loc[trending.month==13,'month']=1
    
    
    #store level previous year segment break up
    seg_out= seg_out.merge(seg_map1,on='lola_grp',how='left')
    
    seg_trend=pd.DataFrame()
    
    for i in range(13,(time_ser.time.max()+2)):
        temp=seg_out.loc[np.logical_and(seg_out.time > (i -13),seg_out.time< (i)) ,['rtlr_num','segment','vol_sales']].groupby(['rtlr_num','segment'],as_index=False).sum()
        temp['time']=i
        seg_trend=seg_trend.append(temp)
    
    seg_lev=seg_trend.merge(seg_out[['time','cal_yr_mo_nbr','month','year']].drop_duplicates(),on='time',how='left')
    seg_lev.loc[seg_lev.cal_yr_mo_nbr.isnull(),'cal_yr_mo_nbr']=seg_lev.cal_yr_mo_nbr.max() + 1
    seg_lev.drop(['time','month','year'],axis=1,inplace=True)
    
    #regional offset
    ovr_reg=seg_out[['dma_key','year','vol_sales']].groupby(['dma_key','year'],as_index=False).sum()
    ovr_reg=ovr_reg.merge(seg_out[['year','vol_sales']].groupby('year',as_index=False).sum().rename(columns={'vol_sales':'USA'}),on='year',how='left')
    ovr_reg['over_cont']=ovr_reg['vol_sales']/ovr_reg['USA']
    
    brnd_reg=seg_out[['dma_key','lola_grp','year','vol_sales']].groupby(['dma_key','lola_grp','year'],as_index=False).sum()
    brnd_reg=brnd_reg.merge(seg_out[['lola_grp','year','vol_sales']].groupby(['lola_grp','year'],as_index=False).sum().rename(columns={'vol_sales':'USA'}),on=['lola_grp','year'],how='left')
    brnd_reg['brnd_cont']=brnd_reg['vol_sales']/brnd_reg['USA']
    
    brnd_reg=brnd_reg.merge(ovr_reg.drop(['USA','vol_sales'],axis=1),on=['dma_key','year'],how='left')
    brnd_reg['regional_offset']=brnd_reg['brnd_cont']/brnd_reg['over_cont']
    
    brnd_reg=brnd_reg[['dma_key','year','lola_grp','regional_offset']]
    brnd_reg['year']=brnd_reg['year']+1
    
    #seasonal offset
    brnd_seas=seg_out[['dma_key','month','lola_grp','year','vol_sales']].groupby(['dma_key','month','lola_grp','year'],as_index=False).sum()
    brnd_seas['seasonal_offset']=brnd_seas['vol_sales']    
    brnd_seas=brnd_seas[['dma_key','month','year','lola_grp','seasonal_offset']]
    brnd_seas['year']=brnd_seas['year']+1
    
    
    #new_intro
    intro=seg_out[['state_cd','lola_grp','time']].groupby(['state_cd','lola_grp'],as_index=False).min()
    seg_out=seg_out.merge(intro.rename(columns={'time':'intro_date'}),on=['state_cd','lola_grp'],how='left')
    seg_out['introduction']=0
    seg_out.loc[np.logical_and(seg_out.intro_date>3,np.logical_and(seg_out.time - seg_out.intro_date>0,seg_out.time - seg_out.intro_date>4)),'introduction']=1
    
    #ext_xcyea06v001, ext_cyec17v001, ext_xcya08v002, ext_xcyfem from Altryx
    seg_out=seg_out.merge(rtlr_geo_map[['rtlr_num','zip','chain_ind']],on='rtlr_num',how='left')
    
    #ext_xcyea06v001, ext_cyec17v001, ext_xcya08v002, ext_xcyfem from Altryx
    fin_dat=seg_out.loc[seg_out.time>12,['rtlr_num','cal_yr_mo_nbr','year','month','chain_ind','zip','state_cd','dma_key','lola_grp','segment','introduction','vol_sales','ptr']].merge(ext[['rtlr_num','xcyea06v001', 'cyec17v001', 'xcya08v002', 'xcyfem']],on='rtlr_num',how='left')
    
    fin_dat=fin_dat.merge(brand_lev[['rtlr_num','cal_yr_mo_nbr','lola_grp','vol_sales']].rename(columns={'vol_sales':'store_fixed'}),on=['rtlr_num','lola_grp','cal_yr_mo_nbr'],how='left')
    #fin_dat=fin_dat.merge(trending[['dma_key','year','month','lola_grp','recency_offset','recency_offset2']],on=['dma_key','year','month','lola_grp'],how='left')
    fin_dat=fin_dat.merge(seg_lev[['rtlr_num','cal_yr_mo_nbr','segment','vol_sales']].rename(columns={'vol_sales':'store_pres'}),on=['rtlr_num','segment','cal_yr_mo_nbr'],how='left')
    
	fin_dat['latest_month_available']=latest_month
	fin_dat['simulation_start']=simulation_start
    fin_dat.to_csv('D:/harsh.vardhana/jan_fe/fin_vol_dat_{0}.csv'.format(channel),index=False)    


    offset=trending[['dma_key','year','month','lola_grp']].drop_duplicates()
    offset=offset.append(brnd_seas[['dma_key','year','month','lola_grp']]).drop_duplicates()
    
    offset=offset.merge(trending[['dma_key','year','month','lola_grp','recency_offset','recency_offset2']],on=['dma_key','year','month','lola_grp'],how='left')
    offset=offset.merge(brnd_reg,on=['dma_key','year','lola_grp'],how='outer')
    offset=offset.merge(brnd_seas,on=['dma_key','year','month','lola_grp'],how='left')

    offset.to_csv('D:/harsh.vardhana/jan_fe/offsets_{0}.csv'.format(channel),index=False)    
    
    if channel=='convSF':
        zip_dat = sql_execute("select zip, bi_channel channel, period cal_yr_mo_nbr, chain_ind, bisegmentdesc segment, supplier, sum(bbls) vol from zip_analytics_test.ab_zip_shr_data where period>201700 and bi_channel='CONVENIENCE' group by zip, bi_channel, period, bisegmentdesc, chain_ind, supplier", create_connection())
    if channel=='grocLF':
        zip_dat = sql_execute("select zip, bi_channel channel, period cal_yr_mo_nbr, chain_ind, bisegmentdesc segment, supplier, sum(bbls) vol from zip_analytics_test.ab_zip_shr_data where period>201700 and (bi_channel='GROCERY' or bi_channel='MASS MERCH') group by zip, bi_channel, period, bisegmentdesc, chain_ind, supplier", create_connection())
    if channel=='pkgliqSF':
        zip_dat = sql_execute("select zip, bi_channel channel, period cal_yr_mo_nbr, chain_ind, bisegmentdesc segment, supplier, sum(bbls) vol from zip_analytics_test.ab_zip_shr_data where period>201700 and bi_channel='LIQUOR' group by zip, bi_channel, period, bisegmentdesc, chain_ind, supplier", create_connection())
        
    zip_dat=zip_dat.loc[zip_dat.supplier=='Ind',:].rename(columns={'vol':'ind'}).merge(zip_dat.loc[zip_dat.supplier=='ABI',['zip','cal_yr_mo_nbr','chain_ind','segment','vol']].rename(columns={'vol':'abi'}),on=['zip','cal_yr_mo_nbr','chain_ind','segment'],how='outer')
    zip_dat.fillna(0,inplace=True)
    zip_dat['ind_share']=zip_dat['ind']/(zip_dat['ind'] + zip_dat['abi'])
    
    zip_dat.drop('channel',axis=1).to_csv('D:/harsh.vardhana/jan_fe/zip_share_{0}.csv'.format(channel),index=False)
    
    ################################
    ######MPD Related###############
    
    if require_mpd=='Yes':
        mpd=sql_execute("select rtlr_party_id, lola_grp, sum(pod_count) mpd, cal_yr_mo_nbr from (select surv_cwc_wslr_cust_party_id rtlr_party_id, brnd_cd, off_premise_pod_count pod_count, cal_yr_mo_nbr from zip_analytics_test.distribution_sold where cal_yr_mo_nbr>201700) as a inner join (select brnd_cd, prod_cd lola_grp from zip_analytics_test.lola_brnd_prod_xref) as b on a.brnd_cd=b.brnd_cd group by rtlr_party_id, lola_grp, cal_yr_mo_nbr", create_connection())
        
        
        mpd['lola_grp']=mpd['lola_grp'].str.lower()
        mpd=mpd.merge(rtlr_map,on='rtlr_party_id',how='left')
        mpd=mpd.loc[~mpd['channel'].isnull(),:]
        mpd=mpd.loc[mpd['channel'].isin(['CONVENIENCE','LIQUOR','GROCERY','MASS MERCH']),:]
        mpd=mpd[['rtlr_num','cal_yr_mo_nbr','lola_grp','mpd','channel']]
        
        mpd.to_csv('D:/harsh.vardhana/jan_fe/mpd_month_offpremise.csv',index=False)
    
        latest_mpd=sql_execute("select max(cal_yr_mo_nbr) from zip_analytics_test.distribution_sold", create_connection())
        latest_mpd=str(latest_mpd.iloc[0,0])
        mpd=sql_execute("select rtlr_party_id, lola_grp, iso_yr_wk_nbr, sum(pod_count) mpd from (select surv_cwc_wslr_cust_party_id rtlr_party_id, brnd_cd, iso_yr_wk_nbr, off_premise_pod_count pod_count from zip_analytics_test.distribution_sold where cal_yr_mo_nbr={0}) as a inner join (select brnd_cd, prod_cd lola_grp from zip_analytics_test.lola_brnd_prod_xref) as b on a.brnd_cd=b.brnd_cd group by rtlr_party_id, lola_grp, iso_yr_wk_nbr".format(latest_mpd), create_connection())
        
        
        mpd['lola_grp']=mpd['lola_grp'].str.lower()
        mpd=mpd.merge(rtlr_map,on='rtlr_party_id',how='left')
        mpd=mpd.loc[~mpd['channel'].isnull(),:]
        mpd=mpd.loc[mpd['channel'].isin(['CONVENIENCE','LIQUOR','GROCERY','MASS MERCH']),:]
        
        mpd=mpd.loc[mpd.iso_yr_wk_nbr==mpd.iso_yr_wk_nbr.max(),:]
        mpd=mpd[['rtlr_num','lola_grp','mpd','channel']]
        
        mpd.to_csv('D:/harsh.vardhana/jan_fe/mpd_latest_offpremise.csv',index=False)

###############################################################################
#On Premise##
##########################Volume and Volume Related############################

if channel=='barONP' or channel=='resONP':

    if channel=='barONP':
        seg_out = sql_execute(
                "select rtlr_party_id, lola_grp, cal_yr_mo_nbr, drght_flg, sum(sales_bbls) as vol_sales, sum(net_price) as price from (select rtlr_party_id, brnd_cd, mkt_ln_cd, cal_yr_mo_nbr, net_price, sales_bbls from zip_analytics_test.str_sales_extract_all where channel='BAR/TAVERN') as a inner join (select brnd_cd, prod_cd lola_grp from zip_analytics_test.lola_brnd_prod_xref) as b on a.brnd_cd=b.brnd_cd inner join (select mkt_ln_cd, drght_flg from zip_analytics_test.str_ref_package) as c on a.mkt_ln_cd=c.mkt_ln_cd group by rtlr_party_id, lola_grp, drght_flg, cal_yr_mo_nbr", create_connection())

        
    if channel=='resONP':
        seg_out = sql_execute(
                "select rtlr_party_id, lola_grp, cal_yr_mo_nbr, drght_flg, sum(sales_bbls) as vol_sales, sum(net_price) as price from (select rtlr_party_id, brnd_cd, mkt_ln_cd, cal_yr_mo_nbr, net_price, sales_bbls from zip_analytics_test.str_sales_extract_all where channel='RESTAURANT') as a inner join (select brnd_cd, prod_cd lola_grp from zip_analytics_test.lola_brnd_prod_xref) as b on a.brnd_cd=b.brnd_cd inner join (select mkt_ln_cd, drght_flg from zip_analytics_test.str_ref_package) as c on a.mkt_ln_cd=c.mkt_ln_cd group by rtlr_party_id, lola_grp, drght_flg, cal_yr_mo_nbr", create_connection())
    
    
    #get rtlr_num
    seg_out=seg_out.merge(rtlr_map[['rtlr_party_id','rtlr_num']].drop_duplicates(),on='rtlr_party_id',how='left')
    seg_out['lola_grp']=seg_out['lola_grp'].str.lower()
    
    #PTR Per BBLS
    seg_out['ptr']=seg_out['price']/seg_out['vol_sales']
    
    #year and month
    seg_out['cal_yr_mo_nbr']=seg_out['cal_yr_mo_nbr'].astype(int)
    seg_out['year']=seg_out.cal_yr_mo_nbr/100
    seg_out['year']=seg_out['year'].round(decimals=0)
    seg_out['month']=seg_out['cal_yr_mo_nbr'] - (seg_out['year']*100)
    time_ser=pd.DataFrame(seg_out[['cal_yr_mo_nbr']].drop_duplicates().sort_values(by='cal_yr_mo_nbr',axis=0,ascending=True)).reset_index(drop=True)
    time_ser['time']=range(1,(time_ser.shape[0]+1))
    seg_out=seg_out.merge(time_ser,on='cal_yr_mo_nbr',how='left')    
    
    start_year=seg_out.year.min()
    final_year=seg_out.year.max()
    
    ##concat brand and p_draft
    seg_out.loc[seg_out['drght_flg']!='D','drght_flg']='P'
    seg_out['brand']=seg_out['lola_grp']
    seg_out['lola_grp']=seg_out['drght_flg'].str.lower() + '_' + seg_out['brand'] 
    
    #calculate brand sales of previous year
    long_trend=pd.DataFrame()
    
    for i in range(13,(time_ser.time.max()+2)):
        temp=seg_out.loc[np.logical_and(seg_out.time > (i -13),seg_out.time< (i)) ,['rtlr_num','lola_grp','vol_sales']].groupby(['rtlr_num','lola_grp'],as_index=False).sum()
        temp['time']=i
        long_trend=long_trend.append(temp)
    
    brand_lev=long_trend.merge(seg_out[['time','cal_yr_mo_nbr','month','year']].drop_duplicates(),on='time',how='left')
    brand_lev.loc[brand_lev.cal_yr_mo_nbr.isnull(),'cal_yr_mo_nbr']=brand_lev.cal_yr_mo_nbr.max() + 1
    brand_lev['year']=brand_lev['cal_yr_mo_nbr']/100
    brand_lev['year']=brand_lev['year'].round(decimals=0)
    brand_lev['month']=brand_lev['cal_yr_mo_nbr'] - (brand_lev['year']*100)
    brand_lev.drop(['time','month','year'],axis=1,inplace=True)
    ####
    
    
    
    #dma_key for calculation of regional level dynamics
    seg_out=seg_out.merge(rtlr_geo_map[['rtlr_num','dma_key','wslr_nbr','state_cd']],on='rtlr_num',how='left')
    seg_out=seg_out.loc[~seg_out.wslr_nbr.isnull(),:]
    
    #fill in missing DMAs
    seg_out.rtlr_num=seg_out.rtlr_num.astype(int)
    seg_out.loc[seg_out.dma_key.isnull(),'dma_key']=seg_out.loc[seg_out.dma_key.isnull(),'wslr_nbr']
    
    
    
    trending=seg_out[['dma_key','year','month','lola_grp','time','vol_sales']].groupby(['dma_key','year','month','time','lola_grp'],as_index=False).sum()
    
    #trending[['year','vol_sales']].groupby('year').sum()
    tr2=trending.copy(deep=True)
    tr2['year']=tr2['year']+1
    trending=trending.merge(tr2[['dma_key','year','month','lola_grp','vol_sales']].rename(columns={'vol_sales':'pre_sale'}),on=['dma_key','year','month','lola_grp'],how='left')
    trending.fillna(0,inplace=True)
    trending=trending.loc[trending.time>12,:]
    
    trending['delta']=(trending['vol_sales'] - trending['pre_sale'])
    tr2=trending.copy(deep=True)
    tr2['month']=tr2['month']+1
    tr2.loc[tr2.month==13,'year']=tr2.loc[tr2.month==13,'year']+1
    tr2.loc[tr2.month==13,'month']=1
    
    trending=trending.merge(tr2[['dma_key','year','month','lola_grp','delta']].rename(columns={'delta':'delta_ll'}),on=['dma_key','year','month','lola_grp'],how='outer')
    trending=trending.loc[trending.time>13,['dma_key','year','month','lola_grp','delta','delta_ll']]
    trending.rename(columns={'delta':'recency_offset','delta_ll':'recency_offset2'},inplace=True)
    #trending['recency_offset']=trending['delta']*0.70 +trending['delta_ll']*0.30
    trending.fillna(0,inplace=True)
    trending['month']=trending['month']+1
    trending.loc[trending.month==13,'year']=trending.loc[trending.month==13,'year']+1
    trending.loc[trending.month==13,'month']=1
    
    
    #store level previous year segment break up
    seg_out= seg_out.merge(seg_map1.rename(columns={'lola_grp':'brand'}),on='brand',how='left')
    
    seg_trend=pd.DataFrame()
    
    for i in range(13,(time_ser.time.max()+2)):
        temp=seg_out.loc[np.logical_and(seg_out.time > (i -13),seg_out.time< (i)) ,['rtlr_num','segment','vol_sales']].groupby(['rtlr_num','segment'],as_index=False).sum()
        temp['time']=i
        seg_trend=seg_trend.append(temp)
    
    seg_lev=seg_trend.merge(seg_out[['time','cal_yr_mo_nbr','month','year']].drop_duplicates(),on='time',how='left')
    seg_lev.loc[seg_lev.cal_yr_mo_nbr.isnull(),'cal_yr_mo_nbr']=seg_lev.cal_yr_mo_nbr.max() + 1
    seg_lev.drop(['time','month','year'],axis=1,inplace=True)
    
    #regional offset
    ovr_reg=seg_out[['dma_key','year','vol_sales']].groupby(['dma_key','year'],as_index=False).sum()
    ovr_reg=ovr_reg.merge(seg_out[['year','vol_sales']].groupby('year',as_index=False).sum().rename(columns={'vol_sales':'USA'}),on='year',how='left')
    ovr_reg['over_cont']=ovr_reg['vol_sales']/ovr_reg['USA']
    
    brnd_reg=seg_out[['dma_key','lola_grp','year','vol_sales']].groupby(['dma_key','lola_grp','year'],as_index=False).sum()
    brnd_reg=brnd_reg.merge(seg_out[['lola_grp','year','vol_sales']].groupby(['lola_grp','year'],as_index=False).sum().rename(columns={'vol_sales':'USA'}),on=['lola_grp','year'],how='left')
    brnd_reg['brnd_cont']=brnd_reg['vol_sales']/brnd_reg['USA']
    
    brnd_reg=brnd_reg.merge(ovr_reg.drop(['USA','vol_sales'],axis=1),on=['dma_key','year'],how='left')
    brnd_reg['regional_offset']=brnd_reg['brnd_cont']/brnd_reg['over_cont']
    
    brnd_reg=brnd_reg[['dma_key','year','lola_grp','regional_offset']]
    brnd_reg['year']=brnd_reg['year']+1
    
    #seasonal offset
    brnd_seas=seg_out[['dma_key','month','lola_grp','year','vol_sales']].groupby(['dma_key','month','lola_grp','year'],as_index=False).sum()
    brnd_seas['seasonal_offset']=brnd_seas['vol_sales']    
    brnd_seas=brnd_seas[['dma_key','month','year','lola_grp','seasonal_offset']]
    brnd_seas['year']=brnd_seas['year']+1
    
    
    #new_intro
    intro=seg_out[['state_cd','brand','time']].groupby(['state_cd','brand'],as_index=False).min()
    seg_out=seg_out.merge(intro.rename(columns={'time':'intro_date'}),on=['state_cd','brand'],how='left')
    seg_out['introduction']=0
    seg_out.loc[np.logical_and(seg_out.intro_date>3,np.logical_and(seg_out.time - seg_out.intro_date>0,seg_out.time - seg_out.intro_date>4)),'introduction']=1
    
    
    #zip and chain_ind
    seg_out=seg_out.merge(rtlr_geo_map[['rtlr_num','zip','chain_ind']],on='rtlr_num',how='left')
    
    #ext_xcyea06v001, ext_cyec17v001, ext_xcya08v002, ext_xcyfem from Altryx
    fin_dat=seg_out.loc[seg_out.time>12,['rtlr_num','cal_yr_mo_nbr','year','month','chain_ind','zip','state_cd','dma_key','lola_grp','brand','drght_flg','segment','introduction','vol_sales','ptr']].merge(ext[['rtlr_num','xcyea06v001', 'cyec17v001', 'xcya08v002', 'xcyfem']],on='rtlr_num',how='left')
    
    fin_dat=fin_dat.merge(brand_lev[['rtlr_num','cal_yr_mo_nbr','lola_grp','vol_sales']].rename(columns={'vol_sales':'store_fixed'}),on=['rtlr_num','lola_grp','cal_yr_mo_nbr'],how='left')
    #fin_dat=fin_dat.merge(trending[['dma_key','year','month','lola_grp','recency_offset','recency_offset2']],on=['dma_key','year','month','lola_grp'],how='left')
    fin_dat=fin_dat.merge(seg_lev[['rtlr_num','cal_yr_mo_nbr','segment','vol_sales']].rename(columns={'vol_sales':'store_pres'}),on=['rtlr_num','segment','cal_yr_mo_nbr'],how='left')
    
	
	fin_dat['latest_month_available']=latest_month
	fin_dat['simulation_start']=simulation_start
    fin_dat.to_csv('D:/harsh.vardhana/jan_fe/fin_vol_dat_{0}.csv'.format(channel),index=False)
    

    offset=trending[['dma_key','year','month','lola_grp']].drop_duplicates()
    offset=offset.append(brnd_seas[['dma_key','year','month','lola_grp']]).drop_duplicates()
    
    offset=offset.merge(trending[['dma_key','year','month','lola_grp','recency_offset','recency_offset2']],on=['dma_key','year','month','lola_grp'],how='left')
    offset=offset.merge(brnd_reg,on=['dma_key','year','lola_grp'],how='outer')
    offset=offset.merge(brnd_seas,on=['dma_key','year','month','lola_grp'],how='left')
    
    offset.to_csv('D:/harsh.vardhana/jan_fe/offsets_{0}.csv'.format(channel),index=False)


    
    if channel=='resONP':
        zip_dat = sql_execute("select zip, bi_channel channel, period cal_yr_mo_nbr, chain_ind, bisegmentdesc segment, supplier, sum(bbls) vol from zip_analytics_test.ab_zip_shr_data where period>201700 and bi_channel='RESTAURANT' group by zip, bi_channel, period, bisegmentdesc, chain_ind, supplier", create_connection())
    if channel=='barONP':
        zip_dat = sql_execute("select zip, bi_channel channel, period cal_yr_mo_nbr, chain_ind, bisegmentdesc segment, supplier, sum(bbls) vol from zip_analytics_test.ab_zip_shr_data where period>201700 and bi_channel='BAR/TAVERN' group by zip, bi_channel, period, bisegmentdesc, chain_ind, supplier", create_connection())
   
    zip_dat=zip_dat.loc[zip_dat.supplier=='Ind',:].rename(columns={'vol':'ind'}).merge(zip_dat.loc[zip_dat.supplier=='ABI',['zip','cal_yr_mo_nbr','chain_ind','segment','vol']].rename(columns={'vol':'abi'}),on=['zip','cal_yr_mo_nbr','chain_ind','segment'],how='outer')
    zip_dat.fillna(0,inplace=True)
    zip_dat['ind_share']=zip_dat['ind']/(zip_dat['ind'] + zip_dat['abi'])
    
    zip_dat.drop('channel',axis=1).to_csv('D:/harsh.vardhana/jan_fe/zip_share_{0}.csv'.format(channel),index=False)
    
    
    ##################
    if require_mpd=='Yes':
        mpd=sql_execute("select rtlr_party_id, lola_grp, cal_yr_mo_nbr, sum(pod_count) mpd from (select surv_cwc_wslr_cust_party_id rtlr_party_id, brnd_cd, cal_yr_mo_nbr, package_pod_count pod_count from zip_analytics_test.distribution_sold where cal_yr_mo_nbr>201700 and package_pod_count>0) as a inner join (select brnd_cd, prod_cd lola_grp from zip_analytics_test.lola_brnd_prod_xref) as b on a.brnd_cd=b.brnd_cd group by rtlr_party_id, cal_yr_mo_nbr, lola_grp", create_connection())
        mpd['draft_or_package']='p'
        mpd2=sql_execute("select rtlr_party_id, lola_grp, cal_yr_mo_nbr, sum(pod_count) mpd from (select surv_cwc_wslr_cust_party_id rtlr_party_id, brnd_cd, cal_yr_mo_nbr, draught_pod_count pod_count from zip_analytics_test.distribution_sold where cal_yr_mo_nbr>201700 and draught_pod_count>0) as a inner join (select brnd_cd, prod_cd lola_grp from zip_analytics_test.lola_brnd_prod_xref) as b on a.brnd_cd=b.brnd_cd group by rtlr_party_id, cal_yr_mo_nbr, lola_grp", create_connection())
        mpd2['draft_or_package']='d'
        
        mpd=mpd.append(mpd2)
        del(mpd2)
        gc.collect()
        
        mpd['lola_grp']=mpd['lola_grp'].str.lower()
        mpd=mpd.merge(rtlr_map,on='rtlr_party_id',how='left')
        mpd=mpd.loc[~mpd['channel'].isnull(),:]
        mpd=mpd.loc[mpd['channel'].isin(['RESTAURANT','BAR/TAVERN']),:]
        mpd['lola_grp']=mpd['lola_grp'].astype(str)
        mpd['draft_or_package']=mpd['draft_or_package'].astype(str)
        mpd['lola_grp']=mpd['draft_or_package']+ str('_') + mpd['lola_grp']
        
        
        mpd=mpd[['rtlr_num','lola_grp','cal_yr_mo_nbr','mpd','channel']]
        mpd.to_csv('D:/harsh.vardhana/jan_fe/mpd_month_onpremise.csv',index=False)


        latest_mpd=sql_execute("select max(cal_yr_mo_nbr) from zip_analytics_test.distribution_sold", create_connection())
        latest_mpd=str(latest_mpd.iloc[0,0])
        mpd=sql_execute("select rtlr_party_id, lola_grp, iso_yr_wk_nbr, sum(pod_count) mpd from (select surv_cwc_wslr_cust_party_id rtlr_party_id, brnd_cd, iso_yr_wk_nbr, package_pod_count pod_count from zip_analytics_test.distribution_sold where cal_yr_mo_nbr={0} and package_pod_count>0) as a inner join (select brnd_cd, prod_cd lola_grp from zip_analytics_test.lola_brnd_prod_xref) as b on a.brnd_cd=b.brnd_cd group by rtlr_party_id, iso_yr_wk_nbr, lola_grp".format(latest_mpd), create_connection())
        mpd['draft_or_package']='p'
        mpd2=sql_execute("select rtlr_party_id, lola_grp, iso_yr_wk_nbr, sum(pod_count) mpd from (select surv_cwc_wslr_cust_party_id rtlr_party_id, brnd_cd, iso_yr_wk_nbr, draught_pod_count pod_count from zip_analytics_test.distribution_sold where cal_yr_mo_nbr={0} and draught_pod_count>0) as a inner join (select brnd_cd, prod_cd lola_grp from zip_analytics_test.lola_brnd_prod_xref) as b on a.brnd_cd=b.brnd_cd group by rtlr_party_id, iso_yr_wk_nbr, lola_grp".format(latest_mpd), create_connection())
        mpd2['draft_or_package']='d'

        mpd=mpd.append(mpd2)
        del(mpd2)
        gc.collect()
        
        mpd['lola_grp']=mpd['lola_grp'].str.lower()
        mpd=mpd.merge(rtlr_map,on='rtlr_party_id',how='left')
        mpd=mpd.loc[~mpd['channel'].isnull(),:]
        mpd=mpd.loc[mpd['channel'].isin(['RESTAURANT','BAR/TAVERN']),:]
        mpd['lola_grp']=mpd['lola_grp'].astype(str)
        mpd['draft_or_package']=mpd['draft_or_package'].astype(str)
        mpd['lola_grp']=mpd['draft_or_package']+ str('_') + mpd['lola_grp']
        mpd=mpd.loc[mpd.iso_yr_wk_nbr==mpd.iso_yr_wk_nbr.max(),:]        
        mpd=mpd[['rtlr_num','lola_grp','mpd','channel']]
        mpd.to_csv('D:/harsh.vardhana/jan_fe/mpd_latest_onpremise.csv',index=False)

if bud=='Yes':
    wod_name_bud = sql_execute("SELECT r.wslr_nbr, rtlr_num ,max( pkg_rte_drvr_nbr) pkg_rte_drvr_nbr, coalesce(max(case when substring(indus_vol_class_cd,1,1) = '1' then 'B' when substring(indus_vol_class_cd,1,1) = '2' then 'U' else 'D' end),'D') bud_class FROM zip_analytics_test.acct_dtl_extract r  left join  zip_analytics_test.rtlr_geo_lookup g on g.rtlr_party_id=r.rtlr_party_id group by r.wslr_nbr, rtlr_num", create_connection())
    wod_name_bud= wod_name_bud.loc[~wod_name_bud.rtlr_num.isnull(),['rtlr_num','bud_class']].drop_duplicates()
    wod_name_bud= wod_name_bud.merge(wod_name_bud.groupby('rtlr_num',as_index=False).count().rename(columns={'bud_class':'number'}),on='rtlr_num')
    wod_name_bud.loc[wod_name_bud.number==1,['rtlr_num','bud_class']].to_csv('D:/harsh.vardhana/jan_fe/bud_df.csv',index=False)
    
if cluster=='Yes':
    cl=sql_execute("SELECT * from zip_analytics_test.rtlr_num_clusters_without_adjustment_for_coli",create_connection())
    cl[['rtlr_num','segment']].to_csv('D:/harsh.vardhana/jan_fe/cluster_df.csv')

if req_3_2=='Yes':
    rtlr_lic=sql_execute("SELECT b.rtlr_num from (SELECT surv_cwc_wslr_cust_party_id rtlr_party_id from lola_data_source.surv_cwc where alchl_lic_cd=20) as a inner join (select rtlr_num, rtlr_party_id from zip_analytics_test.rtlr_geo_lookup where rtlr_num=rtlr_party_id) as b on a.rtlr_party_id=b.rtlr_party_id",create_connection())
    rtlr_lic['licence']=1
    rtlr_lic.to_csv('D:/harsh.vardhana/jan_fe/rtlr_licence_mapping.csv',index=False)
    
    brnd_list=sql_execute("SELECT b.lola_grp, a.mkt_ln_nm ,a.alchl_strength_cd from (SELECT brnd_cd, mkt_ln_nm,alchl_strength_cd from lola_data_source.pdcn_dm) as a inner join (select brnd_cd, prod_cd lola_grp from zip_analytics_test.lola_brnd_prod_xref) as b on a.brnd_cd=b.brnd_cd",create_connection())
    brnd_list['alchl_strength_cd']=brnd_list['alchl_strength_cd'].astype(int)
    brnd_list=brnd_list.loc[brnd_list.alchl_strength_cd<5,:]
    brnd_list=brnd_list[['lola_grp','alchl_strength_cd']].groupby('lola_grp',as_index=False).count()
    brnd_list.rename(columns={'alchl_strength_cd':'cnt'}).to_csv('D:/harsh.vardhana/jan_fe/licence_brnd_mapping.csv',index=False)

act_8= sql_execute("select RTLR_PARTY_ID, CAL_YR_MO_NBR, PRIORITY, EXECUTED, PACK_DRAFT, CORE_CRAFT_LED, SMALL_LARGE_PK, ACTIVE8_CHANNEL, NON_HEAVY_TRADE, HEAVY_TRADE from zip_analytics_test.activ8",create_connection())

def dummy_creator(dat=pd.DataFrame(),var=[],full=True,drop=False):
    for i in var:
        lev=dat[i].drop_duplicates().tolist()
        lev=[x for x in lev if str(x)!='nan']
        if full==False:
            lev=lev[1:]
        k=0
        for j in lev:
            k=k+1
            dat[i + str(k)]=0
            dat.loc[dat[i]==j,i + str(k)]=1
        if drop==True:
            dat=dat.drop(var,axis=1)
    return dat

for i in ['PACK_DRAFT','CORE_CRAFT_LED','SMALL_LARGE_PK','ACTIVE8_CHANNEL','HEAVY_TRADE']:
    act_8[i]=act_8[i].str.lower()

act_8=dummy_creator(dat=act_8,var=['PACK_DRAFT','CORE_CRAFT_LED','SMALL_LARGE_PK','ACTIVE8_CHANNEL','HEAVY_TRADE'])
act_8=act_8.rename(columns={'RTLR_PARTY_ID':'rtlr_party_id'}).merge(rtlr_map.rename(columns={'channel':'alt_channel'}).drop_duplicates(),on='rtlr_party_id',how='left')
act_8_1=act_8.loc[act_8.alt_channel.isin(['CONVENIENCE','BAR/TAVERN','LIQUOR','GROCERY','MASS MERCH','RESTAURANT']),:]
act_8_1.to_csv('D:/harsh.vardhana/jan_fe/act_8_dat.csv',index=False)

brand_grp=sql_execute("select brnd_cd, prod_cd lola_grp from zip_analytics_test.lola_brnd_prod_xref",create_connection())

f1 = sql_execute("select WSLR_NBR, BRND_CD, MKT_LN_CD, BBL_EQUIV_QTY from zip_analytics_test.CONSTRAINT_WSLR_CMBND",create_connection())
f1.columns = ['wslr_nbr','brnd_cd','packs','volume'])
f1=f1.merge(brand_grp,on='brnd_cd',how='left')

f1=f1[['wslr_nbr','lola_grp','volume','packs']].groupby(['wslr_nbr','lola_grp'],as_index=False).agg({'volume':'sum'})
f1=f1[['wslr_nbr','lola_grp','volume','packs']].groupby(['wslr_nbr','lola_grp'],as_index=False).agg({'packs':'count'})

f1.fillna(0,inplace=True)

f1['PKG_COUNT']=f1['packs']
f1[['wslr_nbr','lola_grp','PKG_COUNT']].to_csv('D:/harsh.vardhana/jan_fe/wslr_carried_brands.csv',index=False)
	
space = sql_execute("select WSLR_ST_CD,RTL_SEG_NM,RTLR_SURV_WC_PARTY_ID,SHLF_TOTAL,SHLF_PREM_PCT,AVG_PREM,STD_PREM,SHLF_CLASS from zip_analytics_test.CONSTRAINT_SHELF_SPACE",create_connection())
space=space.rename(columns={'RTLR_SURV_WC_PARTY_ID':'rtlr_party_id'}).merge(rtlr_map[['rtlr_num','rtlr_party_id']],on='rtlr_party_id',how='left')

space=space[['rtlr_num','SHLF_TOTAL']].groupby('rtlr_num',as_index=False).sum()
space.to_csv('D:/harsh.vardhana/jan_fe/space_const.csv',index=False)


#wlsr_brand=wlsr_brand.loc[~wlsr_brand.TOTAL_VOL.isnull(),:]

rtlr_geo_map.loc[~rtlr_geo_map.wslr_nbr.isnull(),['rtlr_num','wslr_nbr']].to_csv('D:/harsh.vardhana/jan_fe/rtlr_wslr.csv',index=False)
