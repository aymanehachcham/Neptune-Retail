
import pandas as pd
from etl_process import DataETLManager

etl_manager = DataETLManager(root_dir='./Data', csv_file='OnlineRetail.csv')
etl_manager.extract_data()
etl_manager.transform_data()


by_customer = dataset.groupby(['CustomerID'])
by_customer.head()
