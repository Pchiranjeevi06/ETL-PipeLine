import json
import pandas as pd

class DataReader:
    def __init__(self, customers_file, transactions_file):
        self.customers_file = customers_file
        self.transactions_file = transactions_file

    def read_json(self):
        with open(self.customers_file, 'r') as f:
            customers_data = json.load(f)
        with open(self.transactions_file, 'r') as f:
            transactions_data = json.load(f)

        customers_df = pd.DataFrame(customers_data)
        transactions_df = pd.DataFrame(transactions_data)
        return customers_df, transactions_df
