
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






#date_div, rf var names,no_of_processor,
off_channels = ['LIQUOR','CONVENIENCE','GROCERY','MASS MERCH']
columns_to_be_dropped = ["CYA03V001", "CYA03V004", "CYA03V005", "CYA03VBASE"]
keyword_to_filter_columns = ["Business Sector"]  
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
        var_names = pd.read_csv("Varibale names 2018.csv")
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
        group by rtlr_party_id, zip,dma_name, channel, time_frame""")
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
#    active_retailer_vol_comb['present'].fillna(0,inplace=True)
#    active_retailer_vol_comb = active_retailer_vol_comb[active_retailer_vol_comb['present']>0]
    vol_df_present = active_retailer_vol_comb.groupby(['rtlr_party_id','zip','dma_name']).agg({'present':['sum']}).reset_index()
    vol_df_past = active_retailer_vol_comb.groupby(['rtlr_party_id','zip','dma_name']).agg({'past':['sum']}).reset_index()
    vol_df_present.columns = ['rtlr_party_id','zip','dma_name','volume']
    vol_df_past.columns = ['rtlr_party_id','zip','dma_name','volume']
    
def segmentation (Key):
    global occupation_off_all
    global columns_to_select
    global bucketed_column_df
    data_dictionary_desc = var_names[var_names['Description'].str.contains(Key)]
    columns_to_select = data_dictionary_desc['Name'].tolist()
    ###Removing column containing Income as Keyword only for Race and Ethnicity
    if Key == "Race and Ethnicity":    
          var_new = var_names[var_names['Description'].str.contains("Income Race and Ethnicity")]
          col_to_select = var_new['Name'].tolist()
          columns_to_select = list(set(columns_to_select).difference(set(col_to_select)))
    bucketed_column_df = combined_data[columns_to_select]
    return bucketed_column_df
    
def feature_vol(combined_data, bucketed_column_df):
    global occupation_off_all
    global tot_rtlr    
    global tot_vol
    occupation_off_all = pd.concat([combined_data[['rtlr_party_id','zip','dma_name']],
                                                  bucketed_column_df,combined_data['volume']],axis=1)
    occupation_off_all.fillna(0, inplace=True)
    tot_rtlr = occupation_off_all.rtlr_party_id.count()
    tot_vol = occupation_off_all.volume.sum()
    
def dma_random_forest(i):
    occupation_off = occupation_off_all.loc[occupation_off_all['dma_name']==i]
    occupation_off_to_scale = occupation_off.drop(['rtlr_party_id','zip','dma_name','volume'],axis=1)
    occupation_off_scaled_array = sc.fit_transform(occupation_off_to_scale)
    X = occupation_off_scaled_array
    y = occupation_off['volume']
    regressor = RandomForestRegressor(n_jobs=30, random_state=13)  
    model = regressor.fit(X,y)
    dma_best_ft = pd.DataFrame()
    dma_best_ft['colnames'] = occupation_off_to_scale.columns
    dma_best_ft['Feature Importance'] = model.feature_importances_
    dma_best_ft.sort_values(by = 'Feature Importance', inplace = True, ascending = False)
    dma_best_ft = dma_best_ft.merge(var_names, how = 'left', left_on ='colnames', right_on = 'Name').drop('Name',axis=1)
    
    for j in range(0,len(dma_best_ft)):
            dma_best_ft['Description'][j] = str(dma_best_ft['Description'][j])[str(dma_best_ft['Description'][j]).find("%"):len(str(dma_best_ft['Description'][j]))-5]  
    
    dma_best_ft['dma_name'],dma_best_ft['rtlr_count'],dma_best_ft['present_volume'] = i,len(occupation_off.index), y.sum()
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
    occupation_off = occupation_off_all.loc[occupation_off_all['dma_name'].isin(i)]
    occupation_off_to_scale = occupation_off.drop(['rtlr_party_id','zip','dma_name','volume'],axis=1)
    occupation_off_scaled_array = sc.fit_transform(occupation_off_to_scale)
    X = occupation_off_scaled_array
    y = occupation_off['volume']
    regressor = RandomForestRegressor(n_jobs=30, random_state=13)  
    model = regressor.fit(X,y)
    dma_best_ft = pd.DataFrame()
    dma_best_ft['colnames'] = occupation_off_to_scale.columns
    dma_best_ft['Cumulative Feature Importance'] = model.feature_importances_
    feature_mat_cum = pd.DataFrame(dma_best_ft[dma_best_ft['colnames']==feature.colnames[list_of_dma.index(i)]])
    return feature_mat_cum

def scatter(occupation_rtlr_vol,feature_nm):
    plt.scatter(occupation_rtlr_vol[[feature_nm]],occupation_rtlr_vol[['volume']],c=occupation_rtlr_vol[['flag']], cmap = 'binary')
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
    occupation_rtlr_vol = occupation_off_all_growth[['rtlr_party_id','dma_name',feature_nm,'volume', 'flag']]
    occupation_rtlr_vol = occupation_rtlr_vol.loc[occupation_rtlr_vol.dma_name.isin(dma_list_ft)]
    within_ft_dist = occupation_rtlr_vol.flag.value_counts().reset_index().transpose().drop('index', axis=0)
    within_ft_dist.index = [feature_nm]
    within_ft_dist['correlation'], within_ft_dist['p-value'] = pearsonr(occupation_rtlr_vol['volume'],occupation_rtlr_vol[feature_nm])
    return within_ft_dist    
    scatter(occupation_rtlr_vol,feature_nm)




def growth_flag(x):
    if (x == 'growth'):
        industry_data['time'] = np.where(pd.to_numeric(industry_data['period']) >= 201711, "Present", "Past")
        industry_data_pivot = pd.pivot_table(industry_data, values = 'Volume', index = ['zip', 'dma_name'], columns = 'time', aggfunc = np.sum).reset_index()
        industry_data_pivot['zip_growth'] = (industry_data_pivot['Present']- industry_data_pivot['Past'])/industry_data_pivot['Past']
        industry_data_pivot_dma = pd.pivot_table(industry_data, values = 'Volume', index = 'dma_name', columns = 'time', aggfunc = np.sum).reset_index()
        industry_data_pivot_dma['dma_growth'] = (industry_data_pivot_dma['Present'] - industry_data_pivot_dma['Past'])/industry_data_pivot_dma['Past']
        rtlr_ind_growth = pd.merge(industry_data_pivot, industry_data_pivot_dma, how = 'left', on = 'dma_name').loc[:,['zip','zip_growth','dma_growth']]
        rtlr_ind_growth['flag'] = np.where(((1+rtlr_ind_growth['zip_growth'])/(1+rtlr_ind_growth['dma_growth'])) > 1, 1, 0)
        occupatuion_off_all_growth = occupation_off_all.merge(rtlr_ind_growth, on='zip', how='inner')
        return occupatuion_off_all_growth

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
##generating needed dataframes and values
feature_vol(combined_data, bucketed_column_df)

##generation of the feature Matrix    

unique_dma = list(combined_data['dma_name'].unique())
feature_mat = pd.concat(map(dma_random_forest,unique_dma),axis=0).reset_index(drop=True)

feature = feature_matrix(feature_mat)

list_of_dma = list(feature.dma_list)
cumulative_ft_imp = pd.concat(map(dma_list_random_forest, list_of_dma))

feature= feature.merge(cumulative_ft_imp, how = 'left', on = 'colnames')
feature['Impact_index'] = feature['mean_rtlr_vol']*feature['Cumulative Feature Importance']* feature['%_rtlr']
feature = feature.sort_values(by='Impact_index', ascending = False).reset_index(drop=True)


####Generating the growth flag from industry data
occupation_off_all_growth = growth_flag('growth')

####generation of correlation, p-value and grew/decline for each feature: 1 = grew
feature_index = list(feature.index)
feature_growth_dist = pd.concat(map(growth_dist_and_corr,feature_index), axis=0).reset_index()
#feature_growth_dist.to_csv('corr and p-value.csv', index = True)




feature['threshold'] = ''

for i in feature.index:
    x= pd.DataFrame(feature.iloc[i,:]).transpose()
    feature_nm = x['colnames'][i]
    dma_list_ft = x['dma_list'][i]
    occupation_rtlr_vol = occupation_off_all_growth[['rtlr_party_id','dma_name',feature_nm,'volume', 'flag']]
    occupation_rtlr_vol = occupation_rtlr_vol.loc[occupation_rtlr_vol.dma_name.isin(dma_list_ft)]
    occupation_rtlr_vol['flag'] = np.where(occupation_rtlr_vol['flag']==1,'Grew','Shrunk')
    optimal_rtlr_vol = occupation_rtlr_vol[(occupation_rtlr_vol['volume']>(mean(occupation_rtlr_vol['volume'])))&(occupation_rtlr_vol['volume']<(mean(occupation_rtlr_vol['volume'])+2*np.std(occupation_rtlr_vol['volume'])))]
    feature['threshold'][i] = mean(optimal_rtlr_vol[optimal_rtlr_vol[feature_nm]>0][feature_nm])





    
feature.to_csv('feature_details_new_1.csv', index=False)

##scatterplots
for i in feature.index:
    x= pd.DataFrame(feature.iloc[i,:]).transpose()
    feature_nm = x['colnames'][i]
    dma_list_ft = x['dma_list'][i]
    occupation_rtlr_vol = occupation_off_all_growth[['rtlr_party_id','dma_name',feature_nm,'volume', 'flag']]
    occupation_rtlr_vol = occupation_rtlr_vol.loc[occupation_rtlr_vol.dma_name.isin(dma_list_ft)]
    occupation_rtlr_vol['flag'] = np.where(occupation_rtlr_vol['flag']==1,'Grew','Shrunk')
    optimal_rtlr_vol = occupation_rtlr_vol[(occupation_rtlr_vol['volume']>(mean(occupation_rtlr_vol['volume'])))&(occupation_rtlr_vol['volume']<(mean(occupation_rtlr_vol['volume'])+2*np.std(occupation_rtlr_vol['volume'])))]
    feature['threshold'][i] = mean(optimal_rtlr_vol[optimal_rtlr_vol[feature_nm]>0][feature_nm])
    
    plot = sns.scatterplot(x=feature_nm, y="volume", alpha=0.2, legend = 'brief', data= occupation_rtlr_vol)
    plot.set(xlim = (0, max(occupation_rtlr_vol[feature_nm])), ylim = (0,max(occupation_rtlr_vol['volume'])))
    plt.axhline(y=mean(occupation_rtlr_vol['volume']), color='red')    
    plt.axhline(y=mean(occupation_rtlr_vol['volume'])+2*np.std(occupation_rtlr_vol['volume']), color='r')    
    plt.axvline(x=mean(optimal_rtlr_vol[feature_nm]))    
    plt.xlabel(feature.description[i][0])    
    plt.ylabel("Volume (sales_bbls)")
    plt.show()
    test = plot.get_figure()
    file_name = 'Scatterplot_'+feature.description[i][0]+'.png'
    test.savefig(file_name)
