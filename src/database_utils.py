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
        

        # user = 'root'
        # password = 'password'
        # host = '127.0.0.1'
        # port = 5432
        # database = 'postgres'
        # engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        return create_engine(
            url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
                USER, PASSWORD, HOST, PORT, DATABASE
            )
        )
        
        # return engine

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

    @staticmethod
    def upload_to_db(df, engine):

        """
        uploads data: arguments: dataframe and table name
        store data in sales data database in a table named dim_users
        """
        # conn = engine.connect('sales_data')
        # c = conn.cursor()

        # # create df with columns.
        # c.execute('CREATE TABLE IF NOT EXISTS dim_users (index, first_name, last_name, date_of_birth, company, email_address, address, country, country_code, phone_number,join_date, user_uuid)')
        # conn.commit()

        # logic to save df in dim_users
        df.to_sql(name = 'dim_users', con = engine, if_exists='replace')


if __name__ == '__main__':
    engine = DatabaseConnector.init_db_engine()
    print(f"Connection to the database sales_data has been created successfully.")
    print(engine)

