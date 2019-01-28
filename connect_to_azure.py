# coding=utf-8

import pyodbc
import pandas as pd
import ConfigParser

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


def get_sql (sql_key):
    config = ConfigParser.RawConfigParser()
    config.read('feature_sql.properties')
    return config.get('featureSQL', sql_key)