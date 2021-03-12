
import pandas as pd
import logging
import os

from sqlalchemy.engine import create_engine

DATA_PATH = 'Data'

class DataETLManager:
    def __init__(self, root_dir: str, csv_file: str):
        if os.path.exists(root_dir):
            if csv_file.endswith('.csv'):
                self.csv_file = os.path.join(root_dir, csv_file)
            else:
                logging.error('The file is not in csv format')
                exit(1)
        else:
            logging.error('The root dir path does not exist')
            exit(1)

        self.retail_df = pd.read_csv(self.csv_file, sep=',', encoding='ISO-8859-1')


    def extract_data(self):
        return self.retail_df

    def fetch_columns(self):
        return self.retail_df.columns.tolist()

    def data_description(self):
        return self.retail_df.describe()

    def fetch_categorical(self, categorical=False):
        if categorical:
            categorical_columns = list(set(self.retail_df.columns) - set(self.retail_df._get_numerical_data().columns))
            categorical_df = self.retail_df[categorical_columns]
            return categorical_df
        else:
            non_categorical = list(set(self.retail_df._get_numerical_data().columns))
            return self.retail_df[non_categorical]

    def transform_data(self):
        data = self.retail_df

        # Checking and eliminating redundant information:
        data.drop_duplicates(keep='last', inplace=True)

        # Fill null Values:
        data['InvoiceNo'].fillna(value=0, inplace=True)
        data['Description'].fillna(value='No Description', inplace=True)
        data['StockCode'].fillna(value='----', inplace=True)
        data['Quantity'].fillna(value=0, inplace=True)
        data['InvoiceDate'].fillna(value='00/00/0000 00:00', inplace=True)
        data['UnitPrice'].fillna(value=0.00, inplace=True)

        data['CustomerID'].fillna(value=0, inplace=True)
        data['Country'].fillna(value='None', inplace=True)

        # Format value columns:
        data['InvoiceDate'] = pd.to_datetime(data['InvoiceDate'])

        self.data_transfomed = data


    def load_data(self):
        database_config = {
            'username': 'aymane_hachcham',
            'password': 'Aymane_Pass@96',
            'host': '127.0.0.1',
            'port':'3306',
            'database':'spotbusiness'
        }

        # create database connection using engine:
        engine = create_engine('mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(
            database_config['username'],
            database_config['password'],
            database_config['host'],
            database_config['port'],
            database_config['database']
        ))

        data_to_load = type(pd.DataFrame())(self.data_transfomed)
        try:
            data_to_load.to_sql('Retail', con=engine, if_exists='append', index=False)
        except Exception as err:
            print(err)
