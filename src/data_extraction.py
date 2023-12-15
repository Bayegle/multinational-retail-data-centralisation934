""" script aiming at extracting data from different data sources:
    1. CVs files
    2. API
    3. S3 Bucket
"""
import pandas as pd
import yaml
# import tabula
import tabula
from sqlalchemy import create_engine, inspect
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
        
        #concat all the individual lists into a dataframe
        df = pd.concat(df)   

        return df


    def list_number_of_stores(api_header, number_store_endpoint):

        response = requests.get(number_store_endpoint, headers=api_header)
        number_of_stores = response.json()['number_stores']
        
        return number_of_stores


    def retrieve_stores_data(api_header):

        all_store_data = []

        for store_number in range(1, 451):
            # Make a request to the API for each store number
            retrieve_store_endpoint =  f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
        
            # print(endpoint_url)
            response = requests.get(retrieve_store_endpoint, headers=api_header)

            if response.status_code == 200:
                # If the request is successful, append the data to the list
                store_data = response.json()  # Assuming the response is in JSON format
                all_store_data.append(store_data)
            else:
                print(f"Failed to retrieve data for store {store_number}. Status code: {response.status_code}")

        # Convert the list of dictionaries into a DataFrame
        df = pd.DataFrame(all_store_data)      

        return df


    # def extract_from_s3(https:s3.//data-handling-public/products.csv):
    #     #s3 = boto3.client('s3')
    #     #s3.download_file()

    #     return df





if __name__ == '__main__':

    's3://data-handling-public/products.csv'
    #print(stores)
    # retrieve_store_endpoint =  'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/1'
    api_header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    print(DataExtractor.retrieve_stores_data(api_header))
    # response = DataExtractor.retrieve_stores_data(retrieve_store_endpoint)
    # data = response.json()

    # data = {'index': [1],
    #     'address': ['Flat 72W\nSally isle\nEast Deantown\nE7B 8EB, High Wycombe'],
    #     'longitude': ['51.62907'],
    #     'lat': [None],
    #     'locality': ['High Wycombe'],
    #     'store_code': ['HI-9B97EE4E'],
    #     'staff_numbers': ['34'],
    #     'opening_date': ['1996-10-25'],
    #     'store_type': ['Local'],
    #     'latitude': ['-0.74934'],
    #     'country_code': ['GB'],
    #     'continent': ['Europe']}

    # df = pd.DataFrame(data)

    # all_store_data = []

    # for store_number in range(1,20):
        # print(store_number)
    #     # Make a request to the API for each store number
    #     endpoint_url =  f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
    
    #     # print(endpoint_url)
    #     response = requests.get(endpoint_url, headers=api_header)

    #     if response.status_code == 200:
    #         # If the request is successful, append the data to the list
    #         store_data = response.json()  # Assuming the response is in JSON format
    #         all_store_data.append(store_data)
    #     else:
    #         print(f"Failed to retrieve data for store {store_number}. Status code: {response.status_code}")

    # # Convert the list of dictionaries into a DataFrame
    # df = pd.DataFrame(all_store_data)

    
    # print(df.head(5))
    # DataExtractor.list_number_of_stores(number_of_stores_endpoint)
