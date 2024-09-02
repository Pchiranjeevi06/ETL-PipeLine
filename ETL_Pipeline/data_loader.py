class DataLoader:
    def __init__(self, cursor):
        self.cursor = cursor

    def load_customers(self, customers_df):
        for index, row in customers_df.iterrows():
            self.cursor.execute("""
                SELECT 1 FROM Customers WHERE customer_id = %s
            """, (row['customer_id'],))
            if not self.cursor.fetchone():
                self.cursor.execute("""
                    INSERT INTO Customers (customer_id, customer_name, country, city)
                    VALUES (%s, %s, %s, %s)
                """, (row['customer_id'], row['customer_name'], row['country'], row['city']))

    def load_orders(self, orders_df):
        for index, row in orders_df.iterrows():
            self.cursor.execute("""
                SELECT 1 FROM Orders WHERE order_id = %s
            """, (row['order_id'],))
            if not self.cursor.fetchone():
                self.cursor.execute("""
                    INSERT INTO Orders (order_id, customer_id, datetime, ecommerce_website_name)
                    VALUES (%s, %s, %s, %s)
                """, (row['order_id'], row['customer_id'], row['datetime'], row['ecommerce_website_name']))

    def load_products(self, products_df):
        for index, row in products_df.iterrows():
            self.cursor.execute("""
                INSERT INTO Products (product_id, product_name, product_category, price)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    product_name = VALUES(product_name),
                    product_category = VALUES(product_category),
                    price = VALUES(price)
            """, (row['product_id'], row['product_name'], row['product_category'], row['price']))

    def load_order_items(self, order_items_df):
        for index, row in order_items_df.iterrows():
            self.cursor.execute("""
                INSERT INTO Order_Items (order_id, product_id, qty, price)
                VALUES (%s, %s, %s, %s)
            """, (row['order_id'], row['product_id'], row['qty'], row['price']))

    def load_payments(self, payments_df):
        for index, row in payments_df.iterrows():
            self.cursor.execute("""
                INSERT INTO Payments (payment_txn_id, order_id, payment_type, payment_txn_success, failure_reason)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    order_id = VALUES(order_id),
                    payment_type = VALUES(payment_type),
                    payment_txn_success = VALUES(payment_txn_success),
                    failure_reason = VALUES(failure_reason)
            """, (row['payment_txn_id'], row['order_id'], row['payment_type'], row['payment_txn_success'], row['failure_reason']))
