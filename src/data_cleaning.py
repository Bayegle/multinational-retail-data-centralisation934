# """ script aiming at cleaning data from each of the data sources """

import pandas as pd


class DataCleaning:

    @staticmethod
    def clean_user_data(df):
        """ perform the cleaning of the user data."""

        # data cleansing
        # missing values, duplicates, incorrect data, wrong date format
        df.dropna()
        # Convert the date column to date format
        # df["opening_date"] = pd.to_datetime(df["opening_date"])

        return df

    def clean_card_data(df):
        """ clean data and returns a dataframe"""
        df.dropna()

        return df


    def called_clean_store_data(df):
        # clean data 
        df.dropna()
        return df
















































































