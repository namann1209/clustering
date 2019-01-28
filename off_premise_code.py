
####Defining paths
input_path='D:\\Clustering_MuSigma\\Clustering Final\\Input Files\\'
output_path='D:\\Clustering_MuSigma\\Clustering Final\\Output Files\\'

####Importing required packages
import numpy as np
import pandas as pd  
from scipy import mean
from sklearn.preprocessing import MinMaxScaler
import pyodbc
from sklearn.ensemble import RandomForestRegressor
import matplotlib.pyplot as plt
import matplotlib.pylab as pylab
from scipy.stats.stats import pearsonr
import seaborn as sns
import os as os


bucket='Business_Sector'
keyword_to_filter_columns = ["Business Sector"]
off_channels = ['LIQUOR','CONVENIENCE','GROCERY','MASS MERCH']
columns_to_be_dropped = ["CYA03V001", "CYA03V004", "CYA03V005", "CYA03VBASE"]
sc = MinMaxScaler(feature_range = [0,1])

##function declarations
def create_connection():
    """
    Establishes connection to azure (assumes the same username and password)
    to use --
    from connect_to_azure import create_connection
    connection_to_azure = create_connection()
    Returns a connection object and a cursor to the connection object wrapped in a dictionary
    :return:
    """
    # Establishing SQL Connection 
    con = pyodbc.connect('DSN=ZipAnalyticsADW;UID=zipcode_analytics_app;PWD=DECZr91@cF')
    return con


def sql_execute(sql_query, create_con_obj=None, n_row=0):
    """
    Checks if a open connection is inputted and uses it to return data from sql query (also accepted as a parameter)
    :param n_row: Number of rows of data to fetch from sql query; default - 0 denotes all rows
    :param sql_query: string containing sql query to be executed
    :param create_con_obj: Connection object returned from create_connection method
    :return: pandas dataframe object
    """
    
    if create_con_obj is None:
        create_con_obj = create_connection()
    print (sql_query)
    df = pd.read_sql(sql_query, create_con_obj)
    print (df.head(2))
    
    return df


def data_ingestion(x):
    if (x=='fetch'):
        global demographics_raw
        demographics_raw = sql_execute("select * from zip_analytics_test.rtlr_party_id_external_data_2018")
        demographics_raw = demographics_raw.loc[demographics_raw['CHANNEL'].isin(off_channels),:]
        demographics_raw['CHANNEL'] = demographics_raw.CHANNEL.replace('MASS MERCH', 'GROCERY')
        
        #data_description
        global var_names
        var_names = sql_execute("Select * from [MuSigma_DEV].[demographics_description]")
        var_names = var_names[['Name', 'Description']]
        var_names = var_names.dropna(axis = 0)
        
        
        ##abi rtlr volume
        global active_retailer_vol
        active_retailer_vol = sql_execute(""" with cte1 as (SELECT a.rtlr_party_id,c.zip,b.dma_name, c.channel, cal_yr_mo_nbr, sales_bbls,
        case 
        when cal_yr_mo_nbr > 201710 then 'present'
        when cal_yr_mo_nbr > 201610 then 'past'
        else 'Not Needed'
        end as time_frame
        FROM [zip_analytics_test].[str_sales_extract_all] a 
        left join zip_analytics_test.rtlr_geo_lookup c
        on a.rtlr_party_id = c.rtlr_party_id
        left join zip_analytics_test.zip_dma b 
        on c.zip=b.zip)
        select rtlr_party_id, zip,dma_name, channel, sum(sales_bbls) as sales_bbls, time_frame from cte1 
        where cal_yr_mo_nbr > 201610
        group by rtlr_party_id,zip,dma_name, channel, time_frame""")
        active_retailer_vol = active_retailer_vol.loc[active_retailer_vol['channel'].isin(off_channels),:]
        active_retailer_vol['channel'] = active_retailer_vol.channel.replace('MASS MERCH', 'GROCERY')
        
        ##industry vol
        global industry_data
        industry_data = sql_execute("""SELECT A.zip, bi_channel, A.period,  B.dma_name, sum(A.bbls) As Volume
        FROM [zip_analytics_test].[ab_zip_shr_data] A
        LEFT JOIN [zip_analytics_test].[zip_dma] B
        ON A.zip = B.zip
        WHERE A.bi_channel IN ('LIQUOR', 'GROCERY', 'CONVENIENCE','MASS MERCH') AND A.supplier = 'IND' 
        GROUP BY A.zip, A.bi_channel, A.period, B.dma_name ORDER BY A.zip, A.period""")

        global rtlr_cluster_df
        rtlr_cluster_df= sql_execute("select * from zip_analytics_test.rtlr_num_clusters_without_adjustment_for_coli")
        
        global result        
        result=pd.read_csv(input_path+'result.csv')
        
        global buckets        
        buckets=pd.read_csv(input_path+'Feature_Buckets.csv')

def demographics_cleaning(x):
    global demographics_off        
    demographics_off = x
    demo_cols = list(demographics_off.columns)
    demo_cols[0] = 'rtlr_party_id'
    demo_cols[3] = 'zip'
    demographics_off.columns = demo_cols
    demographics_off['rtlr_party_id'] = demographics_off['rtlr_party_id'].fillna(0).astype(int)
    demographics_off.drop(columns_to_be_dropped, axis = 1, inplace = True)


def str_sales_cleaning(active_retailer_vol):
    global vol_df_present, vol_df_past
    active_retailer_vol_comb = pd.pivot_table(active_retailer_vol, index = ['rtlr_party_id','zip','dma_name'], columns= 'time_frame', values='sales_bbls'  ).reset_index()
    active_retailer_vol_comb['zip'] = active_retailer_vol_comb['zip'].astype(int) 
    vol_df_present = active_retailer_vol_comb.groupby(['rtlr_party_id','zip','dma_name']).agg({'present':['sum']}).reset_index()
    vol_df_past = active_retailer_vol_comb.groupby(['rtlr_party_id','zip','dma_name']).agg({'past':['sum']}).reset_index()
    vol_df_present.columns = ['rtlr_party_id','zip','dma_name','volume']
    vol_df_past.columns = ['rtlr_party_id','zip','dma_name','volume']
    
def segmentation (Key):
    global feature_bucket_off_all
    global columns_to_select
    global bucketed_column_df
    data_dictionary_desc = var_names[var_names['Description'].str.contains(Key)]
    columns_to_select = data_dictionary_desc['Name'].tolist()
    if Key == "Race and Ethnicity":    
          var_new = var_names[var_names['Description'].str.contains("Income Race and Ethnicity")]
          col_to_select = var_new['Name'].tolist()
          columns_to_select = list(set(columns_to_select).difference(set(col_to_select)))
    bucketed_column_df = combined_data[columns_to_select]
    return bucketed_column_df

def ftr_combine(bucketed_column_df):
    bucketed_column_comb=pd.DataFrame()
    ftr_col=var_names[var_names['Description'].str.contains(keyword_to_filter_columns[0])]
    if keyword_to_filter_columns[0]=='Sports Events':
        ftr_col['Description']=ftr_col['Description'].str.split('% ').str[-1].str.replace('Sports Events - Attend ','')
        ftr_col['Description']=ftr_col['Description'].str.split('% ').str[-1].str.replace('Sports Events - Listen On Radio ','')
    else:
        ftr_col['Description']=ftr_col['Description'].str.split('% ').str[-1].str.split('(').str[0]
    unique_desc=ftr_col['Description'].unique()    
    for i in unique_desc:
        cols=ftr_col['Name'][ftr_col['Description']==i].tolist()
        if keyword_to_filter_columns[0]=='Sports Events':
            bucketed_column_comb[i]=bucketed_column_df[cols].mean(axis=1)
        else:
            bucketed_column_comb[i]=bucketed_column_df[cols].sum(axis=1)
    bucketed_colnames=bucketed_column_comb.columns
    bucketed_column_comb=pd.DataFrame(sc.fit_transform(bucketed_column_comb))
    bucketed_column_comb.columns=bucketed_colnames
    return bucketed_column_comb
    
   
def feature_vol(combined_data, bucketed_column_df):
    global feature_bucket_off_all
    global tot_rtlr    
    global tot_vol
    feature_bucket_off_all = pd.concat([combined_data[['rtlr_party_id','zip','dma_name']],
                                                  bucketed_column_df,combined_data['volume']],axis=1)
    feature_bucket_off_all.fillna(0, inplace=True)
    tot_rtlr = feature_bucket_off_all.rtlr_party_id.count()
    tot_vol = feature_bucket_off_all.volume.sum()
    
def dma_random_forest(i):
    feature_bucket_off = feature_bucket_off_all.loc[feature_bucket_off_all['dma_name']==i]
    feature_bucket_off_to_scale = feature_bucket_off.drop(['rtlr_party_id','zip','dma_name','volume'],axis=1)
    feature_bucket_off_scaled_array = sc.fit_transform(feature_bucket_off_to_scale)
    X = feature_bucket_off_scaled_array
    y = feature_bucket_off['volume']
    regressor = RandomForestRegressor(n_jobs=30, random_state=13)  
    model = regressor.fit(X,y)
    dma_best_ft = pd.DataFrame()
    dma_best_ft['colnames'] = feature_bucket_off_to_scale.columns
    dma_best_ft['Feature Importance'] = model.feature_importances_
    dma_best_ft.sort_values(by = 'Feature Importance', inplace = True, ascending = False)
    dma_best_ft = dma_best_ft.merge(var_names, how = 'left', left_on ='colnames', right_on = 'Name').drop('Name',axis=1)
    
    for j in range(0,len(dma_best_ft)):
            dma_best_ft['Description'][j] = str(dma_best_ft['Description'][j])[str(dma_best_ft['Description'][j]).find("%"):len(str(dma_best_ft['Description'][j]))-5]  
    
    dma_best_ft['dma_name'],dma_best_ft['rtlr_count'],dma_best_ft['present_volume'] = i,len(feature_bucket_off.index), y.sum()
    if (dma_best_ft['Feature Importance'][0]>mean(dma_best_ft['Feature Importance'])*2):
        return pd.DataFrame(dma_best_ft.iloc[0,:]).transpose()

def feature_matrix(feature_mat):
    feature = feature_mat.groupby(['colnames'], as_index=False, sort=False).\
      agg({'dma_name':[':'.join], 'Description':['unique'],'rtlr_count':['sum'],'present_volume':['sum']}) 
    feature.columns = feature.columns.get_level_values(0)
    feature.rename(columns={'colnames':'colnames','dma_name':'dma_list','rtlr_count':'%_rtlr','present_volume':'%_present_volume','Description':'description'},inplace=True)
    feature['no_of_dma'] = ''
    for i in feature.index:
        feature.dma_list[i] = feature.dma_list[i].split(':')    
        feature['no_of_dma'][i] = len(feature.dma_list[i])
    feature['mean_rtlr_vol'] = feature['%_present_volume']/feature['%_rtlr']
    feature['%_rtlr'] = (feature['%_rtlr']/tot_rtlr)*100
    feature['%_present_volume'] = (feature['%_present_volume']/tot_vol)*100
    return feature                

def dma_list_random_forest(i):
    feature_bucket_off = feature_bucket_off_all.loc[feature_bucket_off_all['dma_name'].isin(i)]
    feature_bucket_off_to_scale = feature_bucket_off.drop(['rtlr_party_id','zip','dma_name','volume'],axis=1)
    feature_bucket_off_scaled_array = sc.fit_transform(feature_bucket_off_to_scale)
    X = feature_bucket_off_scaled_array
    y = feature_bucket_off['volume']
    regressor = RandomForestRegressor(n_jobs=30, random_state=13)  
    model = regressor.fit(X,y)
    dma_best_ft = pd.DataFrame()
    dma_best_ft['colnames'] = feature_bucket_off_to_scale.columns
    dma_best_ft['Cumulative Feature Importance'] = model.feature_importances_
    feature_mat_cum = pd.DataFrame(dma_best_ft[dma_best_ft['colnames']==feature.colnames[list_of_dma.index(i)]])
    return feature_mat_cum

def scatter(feature_bucket_rtlr_vol,feature_nm):
    plt.scatter(feature_bucket_rtlr_vol[[feature_nm]],feature_bucket_rtlr_vol[['volume']],c=feature_bucket_rtlr_vol[['flag']], cmap = 'binary')
    plt.xlim(0,30)
    plt.ylim(0,6000)
    plt.ylabel('Volume')
    plt.xlabel(feature_nm+' (%pop)')
    plt.show
    file_name = 'Scatterplot_'+feature_nm+'.png'
    pylab.savefig(file_name)


def growth_dist_and_corr(i):
    x= pd.DataFrame(feature.iloc[i,:]).transpose()
    feature_nm = x['colnames'][i]
    dma_list_ft = x['dma_list'][i]
    feature_bucket_rtlr_vol = feature_bucket_off_all_growth[['rtlr_party_id','dma_name',feature_nm,'volume', 'flag']]
    feature_bucket_rtlr_vol = feature_bucket_rtlr_vol.loc[feature_bucket_rtlr_vol.dma_name.isin(dma_list_ft)]
    within_ft_dist = feature_bucket_rtlr_vol.flag.value_counts().reset_index().transpose().drop('index', axis=0)
    within_ft_dist.index = [feature_nm]
    within_ft_dist['correlation'], within_ft_dist['p-value'] = pearsonr(feature_bucket_rtlr_vol['volume'],feature_bucket_rtlr_vol[feature_nm])
    return within_ft_dist    


def impact_index(feature):
    retailer_growth=pd.DataFrame()
    retailer_growth[['rtlr_party_id', 'zip', 'dma_name']]=vol_df_past[['rtlr_party_id', 'zip', 'dma_name']]
    
    retailer_growth['growth']=(vol_df_present["volume"]-vol_df_past["volume"])/vol_df_past["volume"]
    retailer_growth['growth']=retailer_growth['growth'].replace(np.inf,0)
    retailer_growth=pd.merge(retailer_growth, feature_mat[['colnames', 'Feature Importance', 'Description', 'dma_name']], how = 'left',on = 'dma_name')
    retailer_growth.dropna(inplace=True)
    feature_growth=retailer_growth.groupby('colnames', as_index=False)['growth'].count()
    
    feature_growth['growing_rtlr_cnt']=retailer_growth[retailer_growth['growth']>0].groupby('colnames', as_index=False)['growth'].count()['growth']
    feature_growth.columns=['colnames', 'tot_rtlr_cnt', 'growing_rtlr_cnt']
    feature_growth['%growing_rtlr']=feature_growth['growing_rtlr_cnt']/feature_growth['tot_rtlr_cnt']
    
    
    feature= feature.merge(cumulative_ft_imp, how = 'left', on = 'colnames')
    feature=pd.merge(feature, feature_growth[['colnames','%growing_rtlr']], how = 'left',on = 'colnames')
    
    
    feature['Impact_index'] = feature['%growing_rtlr']*feature['mean_rtlr_vol']*feature['Cumulative Feature Importance']* feature['%_rtlr']
    feature = feature.sort_values(by='Impact_index', ascending = False).reset_index(drop=True)
    return feature


def growth_flag(x):
    if (x == 'growth'):
        industry_data['time'] = np.where(pd.to_numeric(industry_data['period']) >= 201711, "Present", "Past")
        industry_data_pivot = pd.pivot_table(industry_data, values = 'Volume', index = ['zip', 'dma_name'], columns = 'time', aggfunc = np.sum).reset_index()
        industry_data_pivot['zip_growth'] = (industry_data_pivot['Present']- industry_data_pivot['Past'])/industry_data_pivot['Past']
        industry_data_pivot_dma = pd.pivot_table(industry_data, values = 'Volume', index = 'dma_name', columns = 'time', aggfunc = np.sum).reset_index()
        industry_data_pivot_dma['dma_growth'] = (industry_data_pivot_dma['Present'] - industry_data_pivot_dma['Past'])/industry_data_pivot_dma['Past']
        rtlr_ind_growth = pd.merge(industry_data_pivot, industry_data_pivot_dma, how = 'left', on = 'dma_name').loc[:,['zip','zip_growth','dma_growth']]
        rtlr_ind_growth['flag'] = np.where(((1+rtlr_ind_growth['zip_growth'])/(1+rtlr_ind_growth['dma_growth'])) > 1, 1, 0)
        occupatuion_off_all_growth = feature_bucket_off_all.merge(rtlr_ind_growth, on='zip', how='inner')
        return occupatuion_off_all_growth

def threshold_cal(feature_bucket_off_all_growth,feature):
    feature['threshold'] = ''
    for i in feature.index:
        x= pd.DataFrame(feature.iloc[i,:]).transpose()
        feature_nm = x['colnames'][i]
        dma_list_ft = x['dma_list'][i]
        feature_bucket_rtlr_vol = feature_bucket_off_all_growth[['rtlr_party_id','dma_name',feature_nm,'volume', 'flag']]
        feature_bucket_rtlr_vol = feature_bucket_rtlr_vol.loc[feature_bucket_rtlr_vol.dma_name.isin(dma_list_ft)]
        feature_bucket_rtlr_vol['flag'] = np.where(feature_bucket_rtlr_vol['flag']==1,'Grew','Shrunk')
        optimal_rtlr_vol = feature_bucket_rtlr_vol[(feature_bucket_rtlr_vol['volume']>(mean(feature_bucket_rtlr_vol['volume'])))&(feature_bucket_rtlr_vol['volume']<(mean(feature_bucket_rtlr_vol['volume'])+2*np.std(feature_bucket_rtlr_vol['volume'])))&(feature_bucket_rtlr_vol['flag']=='Grew')]
        feature['threshold'][i] = mean(optimal_rtlr_vol[optimal_rtlr_vol[feature_nm]>0][feature_nm])
    return feature


def rtlr_ftr_bkt(feature_bucket_off_all_growth,feature):
    feature_transpose=feature.set_index('colnames').T
    threshold_check=pd.DataFrame()
    for i in feature_transpose.columns:
        colname1='Deviation-'+i
        colname2='Deviation*Impact_Index-'+i
        colname3='Threshold_Check-'+i
        threshold_check[colname1]=feature_bucket_off_all_growth[i]-feature_transpose.loc['threshold'][i]
        threshold_check[colname2]=threshold_check[colname1]*feature_transpose.loc['Impact_index'][i]
        threshold_check[colname3]=np.where(threshold_check[colname2]<0,'Low',np.where(threshold_check[colname2]>0,'High','Match'))
        
    rtlr_feature_bkt=pd.DataFrame()
    rtlr_feature_bkt[['rtlr_party_id','dma_name','zip','flag','volume']]=feature_bucket_off_all_growth[['rtlr_party_id','dma_name','zip','flag','volume']]
    threshold_check_cols = threshold_check.filter(regex='Impact_Index-')
    rtlr_feature_bkt['feature_bkt'] = threshold_check_cols.apply(lambda x: x.argmax(), axis=1)
    rtlr_feature_bkt['feature_bkt']=rtlr_feature_bkt['feature_bkt'].str.split('-', 1).str[-1]
    return rtlr_feature_bkt
 

##data ingestion
data_ingestion('fetch')

##cleaning demographics data
demographics_cleaning(demographics_raw)

## cleaning store volume data
str_sales_cleaning(active_retailer_vol)

##mapping demographics to the volume
vol_df_present['zip'] = vol_df_present['zip'].astype(int)
demographics_off['zip'] = demographics_off['zip'].astype(int) 
combined_data = demographics_off.merge(vol_df_present,how='inner',on = ['rtlr_party_id','zip'])


##subsetting only required columns
bucketed_column_df = pd.concat(map(segmentation,keyword_to_filter_columns),axis=1)

##Combining Feature columns related to same Category
bucketed_column_df=ftr_combine(bucketed_column_df)


##generating needed dataframes and values
feature_vol(combined_data, bucketed_column_df)


##generation of the feature Matrix    

unique_dma = list(combined_data['dma_name'].unique())
feature_mat = pd.concat(map(dma_random_forest,unique_dma),axis=0).reset_index(drop=True)

feature = feature_matrix(feature_mat)

list_of_dma = list(feature.dma_list)
cumulative_ft_imp = pd.concat(map(dma_list_random_forest, list_of_dma))


####Creates impact index column with other required columns in feature dataframe
feature=impact_index(feature)

####Generating the growth flag from industry data
feature_bucket_off_all_growth = growth_flag('growth')

####generation of correlation, p-value and grew/decline for each feature: 1 = grew
feature_index = list(feature.index)
feature_growth_dist = pd.concat(map(growth_dist_and_corr,feature_index), axis=0).reset_index()


####adding threshold column to feature df
feature=threshold_cal(feature_bucket_off_all_growth,feature)
feature.to_csv(output_path+bucket+'_offprem_feature_details_new_latest.csv', index=False)

####Profiling the stores into feature buckets   
retailer_feature_bucket=rtlr_ftr_bkt(feature_bucket_off_all_growth,feature)

#### Volume distribution among feature_bucket features
feature_dist=pd.merge(retailer_feature_bucket[['rtlr_party_id','dma_name', 'zip',
                                          'feature_bkt']],
       vol_df_present[['rtlr_party_id','volume']], how = 'left',on = 'rtlr_party_id')


feature_dist.to_csv(output_path+bucket+"_offprem_Profiled_retailers.csv", index=False)


vol_feature_dist=feature_dist.groupby('feature_bkt', as_index=False)['volume'].sum()
vol_feature_dist['%volume']=vol_feature_dist['volume']*100/vol_feature_dist['volume'].sum()
vol_feature_dist.to_csv(output_path+bucket+'_offprem_Feature_Volume_dist.csv')

#### Combining all the retailer clusters
rtlr_cluster_off=pd.merge(feature_dist,rtlr_cluster_df[['rtlr_num', 'segment', 'age_segment']],
                         how = 'left',left_on = 'rtlr_party_id',right_on = 'rtlr_num')
cluster_column='feature_bkt'
if bucket=='Business_Sector':
    rtlr_cluster_off['feature_bkt']=rtlr_cluster_off['feature_bkt'].map(lambda x: x.strip())
    rtlr_cluster_off=pd.merge(rtlr_cluster_off,buckets[['Collar Categories','Business_Sector']],
                         how = 'left',left_on = 'feature_bkt',right_on = 'Business_Sector')
    cluster_column='Collar Categories'


#### Combining clusters
cluster_scatter=pd.DataFrame()
cluster_scatter[['rtlr_party_id','dma_name','volume','segment',bucket+'_cluster']]=rtlr_cluster_off[['rtlr_party_id','dma_name','volume','segment',cluster_column]]
cluster_scatter['age_cluster']=rtlr_cluster_off['segment']+' '+rtlr_cluster_off['age_segment']
cluster_scatter['final_cluster']=rtlr_cluster_off['segment']+' '+rtlr_cluster_off['age_segment']+' '+rtlr_cluster_off[cluster_column]


cluster_scatter_inc=pd.merge(cluster_scatter,result[['rtlr_party_id','geo_cluster_income']],how='left',on='rtlr_party_id')
cluster_scatter_inc['income_cluster']=cluster_scatter_inc['geo_cluster_income'].str.split(' ',1).str[-1]
cluster_scatter_inc['inc_occ_cluster']=cluster_scatter_inc['income_cluster']+' '+cluster_scatter_inc[bucket+'_cluster']

cluster_scatter_inc.to_csv(output_path+bucket+"_offprem_Profiled_retailers_with_clusters.csv", index=False)

