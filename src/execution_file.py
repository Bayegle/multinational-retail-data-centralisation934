from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning




if __name__ == '__main__':

    #################### Task 3 start #############
    engine = DatabaseConnector.init_db_engine()
    print(engine)
    table_list = DatabaseConnector.list_db_tables(engine)
    user_table = table_list[1] 
    users_df = DataExtractor.read_rds_table(engine, user_table)
    users_df = DataCleaning.clean_user_data(users_df) # More data cleaning to be done: null values, dates etc..
    user_table_name = 'dim_users'
    # DatabaseConnector.upload_to_db(users_df, user_table_name, engine) # this line doesn't work. issue with sales_data database does not exist.

    #################### Task 3 end #############

    #################### Task 4 start #############
    ############## Process card details data and store it. #######################
    # read pdf file.
    link_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    card_detail_df = DataExtractor.retrieve_pdf_data(link_url)

    # retrieve clean data
    card_details_df = DataCleaning.clean_card_data(card_detail_df)

    # save card_details_df
    card_details_table_name = 'dim_card_details'
    # DatabaseConnector.upload_to_db(card_detail_df, card_details_table_name, engine)  # this line doesn't work. issue with sales_data database does not exist.

    #################### Task 4 end #############

    ########### TASK 5 START ##############
    # retrieve the number of stores
    api_header = {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    number_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    number_stores = DataExtractor.list_number_of_stores(api_header, number_store_endpoint)

    # extract all data from stores
    store_df = DataExtractor.retrieve_stores_data(api_header)
    store_df = DataCleaning.called_clean_store_data(store_df)

    # clean and store
    clean_store_df = DataCleaning.called_clean_store_data(store_df)
    store_details_table_name = 'dim_store_details'
    # DatabaseConnector.upload_to_db(clean_store_df, store_details_table_name, engine)  # this line doesn't work. issue with sales_data database does not exist.

    ################# Task 5 END  #######################

    ################# Task 6 START #######################
   