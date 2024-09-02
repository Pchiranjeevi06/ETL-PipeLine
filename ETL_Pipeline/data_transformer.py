import pandas as pd

class DataTransformer:
    def __init__(self, customer_data, transaction_data):
        self.customers_df = customer_data
        self.transactions_df = transaction_data

    def transform_customers_data(self):
        # Ensure columns match exactly
        self.customers_df.columns = self.customers_df.columns.str.strip().str.lower().str.replace(' ', '_')
        self.customers_df['country'] = self.customers_df['country'].fillna('Unknown')
        self.customers_df['city'] = self.customers_df['city'].fillna('Unknown')
        self.customers_df['customer_name'] = self.customers_df['customer_name'].fillna('Unknown Customer')

        self.customers_df['customer_id'] = self.customers_df['customer_id'].astype(int)
        self.customers_df['customer_name'] = self.customers_df['customer_name'].astype(str)
        self.customers_df['country'] = self.customers_df['country'].astype(str)
        self.customers_df['city'] = self.customers_df['city'].astype(str)

        return self.customers_df

    def transform_orders_data(self):
        try:
            orders_df = self.transactions_df[['order_id', 'customer_id', 'datetime', 'ecommerce_website_name']].copy()

            orders_df['order_id'] = orders_df['order_id'].astype(int)
            orders_df['customer_id'] = orders_df['customer_id'].astype(int)
            orders_df['datetime'] = pd.to_datetime(orders_df['datetime'])
            orders_df['ecommerce_website_name'] = orders_df['ecommerce_website_name'].astype(str)

            return orders_df
        except KeyError as e:
            print(f"KeyError: {e}")
            print("Please check the column names in your DataFrame and adjust the code accordingly.")
            return None

    def transform_products_data(self):
        try:
            products_df = self.transactions_df[['product_id', 'product_name', 'product_category', 'price']].drop_duplicates().copy()

            products_df['product_name'] = products_df['product_name'].fillna('Unknown Product')
            products_df['product_category'] = products_df['product_category'].fillna('Miscellaneous')
            products_df['price'] = products_df['price'].fillna(0)

            products_df['product_id'] = products_df['product_id'].astype(int)
            products_df['product_name'] = products_df['product_name'].astype(str)
            products_df['product_category'] = products_df['product_category'].astype(str)
            products_df['price'] = products_df['price'].astype(float)

            return products_df
        except KeyError as e:
            print(f"KeyError: {e}")
            print("Please check the column names in your DataFrame and adjust the code accordingly.")
            return None

    def transform_order_items_data(self):
        try:
            order_items_df = self.transactions_df[['order_id', 'product_id', 'qty', 'price']].copy()

            order_items_df['qty'] = order_items_df['qty'].fillna(1)
            order_items_df['price'] = order_items_df['price'].fillna(0)

            order_items_df['order_id'] = order_items_df['order_id'].astype(int)
            order_items_df['product_id'] = order_items_df['product_id'].astype(int)
            order_items_df['qty'] = order_items_df['qty'].astype(int)
            order_items_df['price'] = order_items_df['price'].astype(float)

            return order_items_df
        except KeyError as e:
            print(f"KeyError: {e}")
            print("Please check the column names in your DataFrame and adjust the code accordingly.")
            return None

    def transform_payments_data(self):
        try:
            payments_df = self.transactions_df[['payment_txn_id', 'order_id', 'payment_type', 'payment_txn_success', 'failure_reason']].copy()

            payments_df['failure_reason'] = payments_df['failure_reason'].fillna('None')

            payments_df['payment_txn_id'] = payments_df['payment_txn_id'].astype(str)
            payments_df['order_id'] = payments_df['order_id'].astype(int)
            payments_df['payment_type'] = payments_df['payment_type'].astype(str)
            payments_df['payment_txn_success'] = payments_df['payment_txn_success'].astype(str)
            payments_df['failure_reason'] = payments_df['failure_reason'].astype(str)

            return payments_df
        except KeyError as e:
            print(f"KeyError: {e}")
            print("Please check the column names in your DataFrame and adjust the code accordingly.")
            return None
