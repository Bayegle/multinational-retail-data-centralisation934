""" script aiming at connecting and upload data to the database """
import yaml
from sqlalchemy import create_engine, inspect
import psycopg2
#from data_cleaning import DataCleaning


class DatabaseConnector:

    @staticmethod
    def read_db_creds():
        """ read credentials from yaml file """
        with open(yaml_path, 'r') as f:
            dic = yaml.safe_load(f)
        return dic

    @staticmethod
    def init_db_engine():
        """
        takes dic from read_db_creds and return  sqlalchemy database engine
        """

       # DATABASE_TYPE = 'postgresql'
        # DBAPI = 'psycopg2'
        USER = 'aicore_admin'
        PASSWORD = 'AiCore2022'
        HOST = 'data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com'
        PORT = 5432
        DATABASE = 'sales_data'

        engine_url = "postgresql+psycopg2://aicore_admin:AiCore2022@data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com:5432/postgres"
        engine = create_engine(engine_url)
        
        return engine

    @staticmethod
    def list_db_tables(engine):
        """ list all tables stored in the database
        takes output from init_db_engine
        returns a list of tables

        """
        inspector = inspect(engine)
        table_list = inspector.get_table_names()
        print(table_list)
        
        return table_list

    # @staticmethod
    # def upload_to_db(df, table_name: str, engine):
    #def upload_to_db(df, table_name, engine_url):
     # engine = create_engine(engine_url)
        #df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

        #data_extractor = DataExtractor("C:\Users\Patrice\Downloads\card_details.pdf")
        #pdf_data = data_extractor.retrieve_pdf_data()

        #data_cleaning = DataCleaning()
        #cleaned_data = data_cleaning.clean_card_data(pdf_data)

       # upload_to_db(cleaned_data, 'dim_card_details', engine_url)





    #     """
    #     uploads data: arguments: dataframe and table name
    #     store data in sales data database in a table named dim_users
    #     """
    #     # conn = engine.connect('sales_data')
    #     # c = conn.cursor()

    #     # # create df with columns.
    #     # c.execute('CREATE TABLE IF NOT EXISTS dim_users (index, first_name, last_name, date_of_birth, company, email_address, address, country, country_code, phone_number,join_date, user_uuid)')
    #     # conn.commit()

    #     # logic to save df in dim_users
    #     df.to_sql(name = table_name, con = engine, if_exists='replace', index=False)

