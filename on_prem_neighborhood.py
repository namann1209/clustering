####Defining paths
input_path='D:\\Clustering_MuSigma\\Clustering Final\\Input Files\\'
output_path='D:\\Clustering_MuSigma\\Clustering Final\\Output Files\\'

####Importing required packages
import pandas as pd  
import pyodbc
import os as os
import numpy as np

#os.chdir('D:\\Piyush Rai\\On-Premise\\')

columns_to_be_dropped = ["CYA03V001", "CYA03V004", "CYA03V005", "CYA03VBASE"]

###Defining channel type
channel_value = ['BAR/TAVERN','RESTAURANT']

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
        demographics_raw['CHANNEL'] = demographics_raw.CHANNEL.replace('MASS MERCH', 'GROCERY')
        demographics_raw = demographics_raw.loc[demographics_raw['CHANNEL'].isin(channel_value),:]
        
        global rtlr_city_mapping        
        rtlr_city_mapping = sql_execute("SELECT [RTLR_PARTY_ID], [CRCTD_CITY_NM],[CHANNEL], [ZIP] FROM [zip_analytics_prod].[acct_dtl_extract]")
        rtlr_city_mapping = rtlr_city_mapping.loc[rtlr_city_mapping['CHANNEL'].isin(channel_value),:]
        rtlr_city_mapping.columns = ['rtlr_party_id', 'city','channel', 'zip']
       
        global popular_city
        popular_city = pd.read_excel(input_path+"Updated_Cluster_2000k_modeled.xlsx")
        popular_city = set(list(popular_city['CITY'].str.upper()))
        
        global rtlr_geo_cluster        
        rtlr_geo_cluster=pd.read_csv(input_path+'result.csv')
        
        global neigh_zip_mapping        
        neigh_zip_mapping=pd.read_csv(input_path+'neigh_int.csv')
        
        
def demographics_cleaning(x):
    global demographics        
    demographics= x
    demo_cols = list(demographics.columns)
    demo_cols[0] = 'rtlr_party_id'
    demo_cols[3] = 'zip'
    demographics.columns = demo_cols
    demographics['rtlr_party_id'] = demographics['rtlr_party_id'].fillna(0).astype(int)
    demographics.drop(columns_to_be_dropped, axis = 1, inplace = True)




###Fetching datasets
data_ingestion('fetch')

##Cleaning data
demographics_cleaning(demographics_raw)
demographics['rtlr_party_id'] = demographics['rtlr_party_id'].astype(int)
demographics['zip'] = demographics['zip'].astype(int)

#####Finding City retailers
city_zip_rtlr = rtlr_city_mapping[rtlr_city_mapping['city'].isin(popular_city)][['rtlr_party_id','city','zip']]
city_zip_rtlr['rtlr_party_id'] = city_zip_rtlr['rtlr_party_id'].astype(int)
city_zip_rtlr.columns.values[1]='geo_cluster'
city_zip_rtlr['geo_label']='City'


####Finding Rural retailers

rtlr_geo_cluster['zip'] = rtlr_geo_cluster['zip'].astype(int)
rtlr_geo_cluster=rtlr_geo_cluster[~rtlr_geo_cluster['zip'].isin(city_zip_rtlr['zip'].astype(int))].dropna()
zip_geo_cluster=rtlr_geo_cluster.groupby('zip',as_index=False)['rtlr_party_id'].count()
zip_rural_cluster=rtlr_geo_cluster[rtlr_geo_cluster['geo_cluster']=='Rural'].groupby('zip',as_index=False)['rtlr_party_id'].count()
zip_geo_cluster.columns.values[1]='total_count'
zip_rural_cluster.columns.values[1]='rural_count'
zip_geo_cluster=pd.merge(zip_geo_cluster,zip_rural_cluster,how='inner',on='zip')
zip_geo_cluster['rural_flag']=np.where(zip_geo_cluster['rural_count']>(zip_geo_cluster['total_count']/2),1,0)

rural_zip=zip_geo_cluster['zip'][(zip_geo_cluster['rural_flag']==1)&(zip_geo_cluster['total_count']>49) & (~zip_geo_cluster['zip'].isin(city_zip_rtlr['zip']))].astype(int).unique()
rural_zip_rtlr=demographics[['rtlr_party_id','zip']][demographics['zip'].isin(rural_zip)].dropna()
rural_zip_rtlr['rtlr_party_id'] = rural_zip_rtlr['rtlr_party_id'].astype(int)
rural_zip_rtlr['geo_cluster']=rural_zip_rtlr['zip']
rural_zip_rtlr['geo_label']='Rural'

####Finding Sub-Urban retailers

unique_zip=neigh_zip_mapping.groupby(['zip'],as_index=False).count()
unique_zip['neigh']=unique_zip['zip']
neigh_zip_mapping=pd.concat([neigh_zip_mapping[['zip','neigh']],unique_zip[['zip','neigh']]])
neigh_zip_mapping = neigh_zip_mapping.applymap(str)
neigh_zip=neigh_zip_mapping.groupby('zip')['neigh'].apply(list).sort_values().apply(lambda x: sorted(x)).reset_index(False)

neigh_zip['zip'] = neigh_zip['zip'].astype(int)
neigh_zip=neigh_zip[~neigh_zip['zip'].isin(city_zip_rtlr['zip'].astype(int).unique())].dropna()
neigh_zip=neigh_zip[~neigh_zip['zip'].isin(rural_zip)].dropna()
neigh_zip_rtlr = pd.merge(neigh_zip,demographics[['rtlr_party_id','zip']], how = 'inner', on = 'zip' )
neigh_zip_rtlr['rtlr_party_id'] = neigh_zip_rtlr['rtlr_party_id'].astype(int)
neigh_zip_rtlr.columns.values[1]='geo_cluster'
neigh_zip_rtlr['geo_label']='Sub-Urban'
neigh_zip_rtlr['geo_cluster']=neigh_zip_rtlr['geo_cluster'].apply(lambda x: list(map(str, x)), 1).str.join(',')

####Combining all the geo-classified retailers
rtlr_geolevel_cluster=pd.concat([city_zip_rtlr,neigh_zip_rtlr],ignore_index=True)
rtlr_geolevel_cluster=pd.concat([rtlr_geolevel_cluster,rural_zip_rtlr],ignore_index=True)

rtlr_geolevel_cluster=rtlr_geolevel_cluster.drop_duplicates(subset=['rtlr_party_id'])
rtlr_geolevel_cluster.to_csv(output_path+'Retailer_Geo_Cluster.csv',index=False)


