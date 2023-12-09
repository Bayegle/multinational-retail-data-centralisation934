""" script aiming at extracting data from different data sources:
    1. CVs files
    2. API
    3. S3 Bucket
"""
import pandas as pd
import yaml
# import tabula
from tabula import read_pdf
from tabulate import tabulate
import jpype
import requests
import boto3

# from database_utils import DatabaseConnector

class DataExtractor:

    @staticmethod
    def read_rds_table(engine, table_name):
        """ read tables from database"""

        #engine = DatabaseConnector.init_db_engine()
        #table_list = DatabaseConnector.list_db_tables()
        
        # table chosen by the user
        # table_name = table_list[0]
        df = pd.read_sql_table(table_name, engine)

        return df

    def retrieve_pdf_data(link_url):
        """ takes in a link as an argument and returns a pandas DataFrame"""
        df = read_pdf(link_url, pages="all")
        
        # concat all the individual lists into a dataframe
        df = pd.concat(df)   

        return df


    def list_number_of_stores(api_header, number_store_endpoint):

        response = requests.get(number_store_endpoint, headers=api_header)
        number_of_stores = response.json()['number_stores']
        
        return number_of_stores


    def retrieve_stores_data(retrieve_store_endpoint):
        response = requests.get(retrieve_store_endpoint, headers = api_header)
        

        return response


    # def extract_from_s3(https:s3.//data-handling-public/products.csv):
    #     #s3 = boto3.client('s3')
    #     #s3.download_file()

    #     return df





if __name__ == '__main__':

    's3://data-handling-public/products.csv'
    #print(stores)
    retrieve_store_endpoint =  'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/1'
    api_header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    response = DataExtractor.retrieve_stores_data(retrieve_store_endpoint)
    data = response.json()
    print(data)
    # print(list(data.keys()))
    # print(list(data.values()))
    df = pd.DataFrame([list(data.keys()), list(data.values())]).T
    # df = df.pivot(columns='0',values = '1')

    # df.columns = df.iloc[1]
    # df = pd.concat((pd.read_json(d) for d in data), axis=0)

    
    # df = pd.DataFrame(list(data.keys())).T
    # df.columns = df.iloc[0]
    print(df.reset_index())
    # DataExtractor.list_number_of_stores(number_of_stores_endpoint)
