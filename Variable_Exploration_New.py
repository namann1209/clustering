# -*- coding: utf-8 -*-
"""
Created on Wed Dec 19 16:02:58 2018

@author: C887843
"""

############################Importing basic modules and libraries###########################
import pandas as pd  
import numpy as np 
from sklearn.preprocessing import MinMaxScaler
from sklearn.decomposition import PCA
import pyodbc
from sklearn.ensemble import RandomForestRegressor

#Configurations
scaling_range = [0,100]
no_of_processors = 30
explained_variance = 0.5

#User input 
keyword_to_filter_column_description = ['Race and Ethnicity',
 'Income Race and Ethnicity',
 'Business Sector',
 'Average Expenditure Alcoholic',
 'Percent Tenure',
 'Percent Labor Force',
 'Sports Events - Attend',
 'Sports Events - Listen',
 'Dining']  
off_premise_channels = ['LIQUOR','CONVENIENCE','GROCERY', 'MASS MERCH']
columns_to_be_dropped = []
#Prepared on top of the results of correaltion matrix to remove variables based on business sense
correlation_matrix_columns = ['XCYA10V001',
 'XCYA12V009',
 'CYEC17V001',
 'PYCYPOPGRW',
 'CYB26VV01',
 'CYPOPDENS',
 'CYA01V001',
 'XCYB01V002',
 'XCYFEM',
 'XCYMALE']
#Columns for which no bucketing is required
column_no_bucketing_required = ['XCYFEM',
 'XCYMALE',
 'XCYA10V001',
 'XCYA12V009',
 'CYEC17V001',
 'PYCYPOPGRW',
 'CYB26VV01',
 'CYPOPDENS',
 'CYB03V001',
 'CYB02V001',
 'CYA01V001',
 'XCX02V194',
 'XCX02V160',
 'CYB17MED',
 'CYB21VBASE',
 'XCYB01V002',
 'XCYB01V003',
 'SMFAM007P_PCT']
#Description columns
descriptive_columns = ['rtlr_party_id',
 'WSLR_NBR',
 'WSLR_ASGND_CUST_NBR',
 'ZIP',
 'CHANNEL',
 'Urbanicity']
#########################################Function Declaration##############################



set(column_no_bucketing_required).difference(set(correlation_matrix_columns))
def create_connection():
    con = pyodbc.connect('DSN=ZipAnalyticsADW;UID=zipcode_analytics_app;PWD=DECZr91@cF')
    return con
    
def sql_execute(sql_query, create_con_obj=None, n_row=0):
    if create_con_obj is None:
        create_con_obj = create_connection()
    print (sql_query)
    df = pd.read_sql(sql_query, create_con_obj)
    print (df.head(2))
    return df
    
#Function for generating PCA for 9 buckets
def segmentation (Key):
    global data_dictionary
    data_dictionary_desc = data_dictionary[data_dictionary['Description'].str.contains(Key)]
    columns_to_select = data_dictionary_desc['Name'].tolist()
    bucketed_column_df = demo_off_scaled[demo_off_scaled.columns.intersection(columns_to_select)]   
    ###Removing column containing Income as Keyword only for Race and Ethnicity
    if Key == "Race and Ethnicity":    
          var_new = data_dictionary[data_dictionary['Description'].str.contains("Income Race and Ethnicity")]
          col_to_select = var_new['Name'].tolist()
          for column in col_to_select:
             del bucketed_column_df[column]
    #PCA part
    pca = PCA(n_components=None)
    principalComponents = pca.fit_transform(bucketed_column_df)
    pca = PCA(n_components=np.argmax(np.cumsum(pca.explained_variance_ratio_)>explained_variance)+1)
    principalComponents = pca.fit_transform(bucketed_column_df)
    cols=[]
    for i in range(np.argmax(np.cumsum(pca.explained_variance_ratio_)>explained_variance)+1):
        column_name = Key.replace(" ", "_")
        cols.append(column_name+'_'+str(i+1))
    dataframe_PCA  = pd.DataFrame(data = principalComponents
                 , columns = cols)
    return dataframe_PCA


#Data Ingestion
def data_ingestion():
    #Pulling demographics data
    global demographics_raw  
    demographics_raw = sql_execute("select * from zip_analytics_test.rtlr_party_id_external_data_2018")
    
    #Active retailer volume
    global active_retailer_vol 
    active_retailer_vol = sql_execute("with cte1 as (SELECT a.rtlr_party_id,b.zip, b.channel, a.brnd_cd, cal_yr_mo_nbr, sales_bbls \
        FROM [zip_analytics_test].[str_sales_extract_all] a \
        left join zip_analytics_test.rtlr_geo_lookup b \
        on a.rtlr_party_id = b.rtlr_party_id) \
        select rtlr_party_id, zip, channel, brnd_cd, sum(sales_bbls) as sales_bbls from cte1 \
        where cal_yr_mo_nbr > 201709 \
        group by rtlr_party_id, zip, channel, brnd_cd") 
    
    #Reading the data dictionary file
    global data_dictionary    
    data_dictionary = pd.read_csv("D:/Nikunj/Variable names 2018.csv")
    data_dictionary = data_dictionary[['Name', 'Description']]
    ##Removing rows containing NAs in description
    data_dictionary = data_dictionary.dropna(axis = 0)
    
    return (demographics_raw, active_retailer_vol, data_dictionary)

def Random_Forest_Regressor(combined_data, no_of_processors):
    global feature_importance
    independent_variable = combined_data.drop(['rtlr_party_id', 'zip', 'volume'], axis =1) 
    dependent_var = combined_data[['volume']].reset_index(drop =True)
    independent_variable[np.isnan(independent_variable)] = 0
    dependent_var[np.isnan(dependent_var)] = 0
    regressor = RandomForestRegressor(verbose=2,oob_score=True, n_jobs = no_of_processors, random_state =13)  
    model = regressor.fit(independent_variable, dependent_var)  
    
    feature_importance = pd.DataFrame()
    feature_importance['colnames'] = combined_data.drop(['rtlr_party_id', 'zip', 'volume'], axis =1).columns
    feature_importance['Feature Importance'] = model.feature_importances_
    feature_importance.sort_values(by = 'Feature Importance', inplace = True, ascending = False)
    feature_importance = feature_importance.merge(data_dictionary, how = 'left', left_on ='colnames', right_on = 'Name').drop('Name', axis = 1)

def Correlation_of_demo_columns_description(dataframe_PCA_non_PCA):
    global corr_df        
    correlation_matrix = dataframe_PCA_non_PCA.drop('rtlr_party_id', axis=1).corr()
    correlation_matrix_long = correlation_matrix.stack().reset_index()
    correlation_matrix_long.columns = ['V1','V2','correlation']
    corr_df = correlation_matrix_long.loc[(abs(correlation_matrix_long['correlation'])>correlation_matrix_long.correlation.quantile(0.75)) & (abs(correlation_matrix_long['correlation'])!=1)]
    corr_df = corr_df.merge(data_dictionary,how='left',left_on='V1',right_on='Name')
    corr_df = corr_df.merge(data_dictionary,how='left',left_on='V2',right_on='Name')
    corr_df = corr_df.drop(['Name_x','Name_y'], axis = 1)
    corr_df.columns = ['V1','V2','Correlation','Desc_V1','Desc_V2']
 
def scaling(scaling_range, demographics_off):
    sc = MinMaxScaler(feature_range = scaling_range)
    demo_to_scale = demographics_off.drop(descriptive_columns, axis = 1)
    demo_off_scaled_array = sc.fit_transform(demo_to_scale)
    global demo_off_scaled        
    demo_off_scaled = pd.DataFrame(demo_off_scaled_array, columns=demo_to_scale.columns.values)
    return demo_off_scaled
   
        
def PCR(x):
    if (str(x)=='Alteryx'):
        
        global feature_ipmortance, corr_df
        data_ingestion()
        ################Data Cleaning and Wrangling################
        demographics = demographics_raw
        demo_cols = list(demographics.columns)
        demo_cols[0] = 'rtlr_party_id'
        demographics.columns = demo_cols
        demographics_off = demographics.loc[demographics['CHANNEL'].isin(off_premise_channels),:].reset_index(drop = True)
        demographics_off.CHANNEL.replace(to_replace = 'MASS MERCH', value = 'GROCERY', inplace = True)        
        demographics_off['rtlr_party_id'] = demographics_off['rtlr_party_id'].fillna(0).astype(int)
        demographics_off = demographics_off.reset_index(drop = True)
        #Removing descriptive columns
        demographics_temp = demographics_off.drop(descriptive_columns, axis = 1)
        #Removing columns with NAs in them        
        demographics_off = demographics_off.drop(demographics_temp.columns[demographics_temp.isnull().sum() != 0],axis=1)        
        #Dropping columns containing Age as keyword
        columns_age_description = data_dictionary[data_dictionary['Description'].str.contains("Age")]
        columns_to_select_in_description = columns_age_description['Name'].tolist()
        for column in columns_to_select_in_description:
            del demographics_off[column]
        
        #Scaling the dataframe
        scaling(scaling_range, demographics_off)
        ####################Principal Component Analysis###################
        #Creating final dataframe after running PCA
        PCA_Dataframes = pd.concat(map(segmentation, keyword_to_filter_column_description),axis=1).reset_index(drop=True)
        Dataframe_Non_PCA = demo_off_scaled[column_no_bucketing_required]          
        #Collating the Final dataframe required for correlation matrix
        dataframe_PCA_non_PCA = pd.concat([demographics_off[['rtlr_party_id']].reset_index(drop=True), Dataframe_Non_PCA, PCA_Dataframes], axis = 1).reset_index(drop = True)
        
        Correlation_of_demo_columns_description(dataframe_PCA_non_PCA) 
        
        columns_selected_after_correlation = demo_off_scaled[correlation_matrix_columns]          
        demo_to_reg = pd.concat([demographics_off[['rtlr_party_id']].reset_index(drop=True),columns_selected_after_correlation, PCA_Dataframes], axis = 1)     
        
        #########################Random Forest###########################
        global active_retailer_vol
        active_retailer_vol = active_retailer_vol.loc[active_retailer_vol['channel'].isin(off_premise_channels),:]
        vol_df = active_retailer_vol.groupby(['rtlr_party_id','zip']).agg({'sales_bbls':['sum']}).reset_index()
        
        #Considering only required columns
        vol_df.columns = ['rtlr_party_id','zip','volume']
        combined_data = demo_to_reg.merge(vol_df,how='inner',on = 'rtlr_party_id')
        
        Random_Forest_Regressor(combined_data, no_of_processors)
        print("Check out 'corr_df' and 'feature_importnace' in Variable Explorer!")
        return feature_importance, corr_df
    else:
        print("Error: Only supports Alteryx data currently")

######Calling PCR function
if '__name_'== '__main__':
    
    feature_importance, corr_df = PCR('Alteryx')
    
#############################################End of code####################################    
    