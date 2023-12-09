from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


if __name__ == '__main__':
    import pandas as pd
    import sqlite3


    #################### Task 3 start #############
    # from sqlalchemy import create_engine
    # engine = create_engine('sqlite://', echo=False)
    df = pd.DataFrame({'name' : ['User 1', 'User 2', 'User 3']})

    from sqlalchemy import text
    engine = DatabaseConnector.init_db_engine()
    # # print(engine)
    # conn = sqlite3.connect('sales_data')
    # c = conn.cursor()
    # c.execute('CREATE TABLE IF NOT EXISTS products (product_name text, price number)')
    # conn.commit()
    df.to_sql(name='products', con=engine, if_exists='replace', index = False)

    # table_list = DatabaseConnector.list_db_tables(engine)
    #print(table_list)

    ##################### process user table data #########################
    # user_table = table_list[1] 
    # print(user_table)
    # df = DataExtractor.read_rds_table(engine, user_table)
    # df = DataCleaning.clean_user_data(df) # il faut bien faire du data cleaning: null values, dates etc..
    # # print(df.columns)
    # user_table_name = 'dim_users'
    # df.to_sql(name= 'dim_usersS', con=engine, if_exists='replace')
    # DatabaseConnector.upload_to_db(df, user_table_name, engine)
    #################### Task 3 end #############

    #################### Task 4 start #############
    ############## Process card details data and store it. #######################
    # read pdf file.
    # link_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    # card_detail_df = DataExtractor.retrieve_pdf_data(link_url)
    # print(card_detail_df.head(10))

    # retrieve clean data
    # card_details_df = DataCleaning.clean_card_data(card_detail_df)

    # save card_details_df
    # card_details_table_name = 'dim_card_details'
    # DatabaseConnector.upload_to_db(card_detail_df, card_details_table_name, engine)

    #################### Task 4 end #############


    ########### TASK 5 START ##############

    # retrieve the number of stores
    # api_header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    # number_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    # number_stores = DataExtractor.list_number_of_stores(api_header, number_store_endpoint)

    # extract all data from stores
    # retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_numbers}'
    # response = requests.get(retrieve_store_endpoint, headers = api_header)



    # clean and store
    # clean_store_df = DataCleaning.called_clean_store_data(df)
    # store_details_table_name = 'dim_store_details'
    # DatabaseConnector.upload_to_db(clean_store_df, store_details_table_name, engine)

    ################# Task 6 Start
    # engine = create_engine("postgresql+psycopg2://scott:tiger@localhost:5432/mydatabase")
    # Engine(postgresql+psycopg2://aicore_admin:***@data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com:5432/sales_data)
