####Defining paths
input_path='D:\\Clustering_MuSigma\\Clustering Final\\Input Files\\'
output_path='D:\\Clustering_MuSigma\\Clustering Final\\Output Files\\'

####Importing required packages
import os
import pandas as pd
import numpy as np
from scipy.cluster.hierarchy import linkage, fcluster

###Total population on zip level
demographics_data = pd.read_csv(input_path+"ACS_16_5YR_B01003_with_ann.csv")

demographics_data = demographics_data[['GEO.id2', 'HD01_VD01']]

###Removing first row
demographics_data = demographics_data.iloc[1:]

##Renaming column names
demographics_data = demographics_data.rename(columns = {'GEO.id2':'zip', 'HD01_VD01':'total_population'})
demographics_data['zip'] = demographics_data['zip'].astype(int)
demographics_data = demographics_data[demographics_data['total_population'] > '0']

###Employee count on zip level
zip_emp_data = pd.read_csv(input_path+"zbp16totals.txt")
zip_emp_data = zip_emp_data[['zip', 'emp']]
zip_emp_data = zip_emp_data.rename(columns = {'emp':'employee_count'})


###Merging dataset
master_dataset = pd.merge(demographics_data, zip_emp_data, how = 'inner', on = 'zip')

###Changing datatype of columns
master_dataset['total_population'] = master_dataset['total_population'].astype(int)
master_dataset['ratio_emp_to_population'] = (master_dataset['employee_count'])/(master_dataset['total_population'])

test_a = master_dataset[master_dataset.columns[3:4]]

data = np.array(test_a)


######For testing ### 1000 rows

result_test = linkage(data, method = 'ward')


def bucket(x):
    bucket = str(x.min_value) + " - " + str(x.max_value)
    return (bucket)

cluster_list = list(np.linspace(3,30,10))

ratio_buck = []


def bucketing(x):
    global ratio_buck
    global test_a
    global result_test
    test_a_sp_hcl_3=test_a
    
    
    ls_sp_hcl = fcluster(result_test, x, criterion="maxclust")
    test_a_sp_hcl_3['clusters'] = ls_sp_hcl
    
    
    group_sp_hcl_3=pd.DataFrame()
    group_sp_hcl_3['max']=test_a_sp_hcl_3.groupby('clusters')['ratio_emp_to_population'].max()
    group_sp_hcl_3['min']=(test_a_sp_hcl_3.groupby('clusters')['ratio_emp_to_population'].min())
    group_sp_hcl_3=group_sp_hcl_3.reset_index()
    group_sp_hcl_3
    
    
    
    zip_no_sp_hcl_3 = pd.DataFrame(test_a_sp_hcl_3.clusters.value_counts()).reset_index()
    
    zip_no_sp_hcl_3.columns = ['clusters','counts']
    
    clusters_sp_hcl_3 = group_sp_hcl_3.merge(zip_no_sp_hcl_3, how='left', on = 'clusters')
    clusters_sp_hcl_3 = clusters_sp_hcl_3[['clusters','min','max','counts']]
    
    clusters_sp_hcl_3.columns= ['clusters','min_value','max_value','counts']
    
    a ="Clusters "
    clusters_graph_sp_hcl_3 = clusters_sp_hcl_3
    clusters_graph_sp_hcl_3
    clusters_graph_sp_hcl_3['cluster'] = clusters_graph_sp_hcl_3['clusters'].apply(lambda x: a + x.astype('str'))
    
    clusters_graph_sp_hcl_3 = clusters_graph_sp_hcl_3.sort_values(by='min_value')
    
    ratio_buck.append(clusters_graph_sp_hcl_3.min_value.values)
    
    clusters_graph_sp_hcl_3['bucket'] = clusters_graph_sp_hcl_3.apply(bucket, axis=1)
    clusters_graph_sp_hcl_3.plot.bar(y="counts", x="bucket")
    cluster_counts = clusters_graph_sp_hcl_3[['bucket','counts']]
    cluster_counts['k'] = x
    return cluster_counts

demo = pd.concat(map(bucketing,cluster_list), axis=0)


##Entropy maximisation

data_merge = master_dataset[['zip','ratio_emp_to_population']]
zip_seg_vol = pd.read_csv(input_path+'zip_seg_vol.csv')

temp_df = pd.merge(zip_seg_vol, 
                   data_merge,
                   how='left', 
                   left_on='zip_code', 
                   right_on='zip')


temp_df['total_abi_vol'] = temp_df[['CORE', 'CORE Plus & Premium',
                                    'FMB', 'H.E', 'N.A', 'VALUE']].sum(axis=1)

def ratio_bucket(x, ratio_buck):
    if x >= max(ratio_buck):
            return 'Above ' + str(max(ratio_buck))
    for i in ratio_buck:
        if x >= i:
            continue
        else:
            return i
        
        
def entropy_cal(ratio_buck):
    final_df = temp_df[['zip_code', 'ratio_emp_to_population', 'total_abi_vol']]
      
    
    final_df['ratio_group'] = final_df['ratio_emp_to_population'].apply(lambda x: ratio_bucket(x, ratio_buck))
    total_vol = final_df['total_abi_vol'].sum()
    entropy_df = final_df.groupby('ratio_group')['total_abi_vol'].sum().reset_index()
    entropy_df['propotion'] = entropy_df['total_abi_vol']/total_vol
    
    print(entropy_df[['ratio_group', 'propotion']])
    
    entropy = -sum(entropy_df['propotion']*np.log(entropy_df['propotion']))
    
    return(entropy)


    
entropy_dict = {}
for a in ratio_buck:
    entropy = entropy_cal(a)
    entropy_dict[str(a)] =  entropy
    print('Entropy for %s: %s' %(a, entropy))

max_entropy = (max(entropy_dict, key=entropy_dict.get))
print('\nMaximum Entropy is %s for %s which corresponds to %s clusters\n' %(entropy_dict[max_entropy], 
                                           max_entropy, len(max_entropy.split(' '))))
                                           
                                           


test_a_sp_hcl_3=test_a


ls_sp_hcl = fcluster(result_test, len(max_entropy.split(' ')), criterion="maxclust")
test_a_sp_hcl_3['clusters'] = ls_sp_hcl


group_sp_hcl_3=pd.DataFrame()
group_sp_hcl_3['max']=test_a_sp_hcl_3.groupby('clusters')['ratio_emp_to_population'].max()
group_sp_hcl_3['min']=(test_a_sp_hcl_3.groupby('clusters')['ratio_emp_to_population'].min())
group_sp_hcl_3=group_sp_hcl_3.reset_index()
group_sp_hcl_3



zip_no_sp_hcl_3 = pd.DataFrame(test_a_sp_hcl_3.clusters.value_counts()).reset_index()

zip_no_sp_hcl_3.columns = ['clusters','counts']

clusters_sp_hcl_3 = group_sp_hcl_3.merge(zip_no_sp_hcl_3, how='left', on = 'clusters')
clusters_sp_hcl_3 = clusters_sp_hcl_3[['clusters','min','max','counts']]

clusters_sp_hcl_3.columns= ['clusters','min_value','max_value','counts']
clusters_sp_hcl_3
a ="Clusters "
clusters_graph_sp_hcl_3 = clusters_sp_hcl_3
clusters_graph_sp_hcl_3
clusters_graph_sp_hcl_3['cluster'] = clusters_graph_sp_hcl_3['clusters'].apply(lambda x: a + x.astype('str'))

clusters_graph_sp_hcl_3 = clusters_graph_sp_hcl_3.sort_values(by='min_value')

clusters_graph_sp_hcl_3 = pd.read_csv(input_path+'clusters_graph_sp_hcl_3.csv')

ratio_limits = [0.130830, 0.297091]

master_dataset['area_type'] =""

master_dataset['ratio_emp_to_population'] = master_dataset['ratio_emp_to_population'].astype(float)

def area_type():
    global master_dataset
    global ratio_limits
    master_dataset['area_type'] = np.where(((master_dataset['ratio_emp_to_population'] < ratio_limits[0])), 'Residential', 
                                             np.where(((master_dataset['ratio_emp_to_population'] >= ratio_limits[0]) & (master_dataset['ratio_emp_to_population'] <= ratio_limits[1])), 'Mixed', 'Commercial'))

area_type()

master_dataset.to_csv(input_path+'master_dataset_clustering_resid_commer.csv', index=False)
