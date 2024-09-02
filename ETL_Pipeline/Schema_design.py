import mysql.connector

class SchemaDesign:
    def __init__(self, cursor):
        self.cursor = cursor

    # Products Table
    def create_products_table(self):
        self.cursor.execute("SHOW TABLES LIKE 'Products'")
        if not self.cursor.fetchall():
            self.cursor.execute("""
                CREATE TABLE Products (
                    product_id INT PRIMARY KEY,
                    product_name VARCHAR(100) NOT NULL,
                    product_category VARCHAR(50) NOT NULL,
                    price DECIMAL(10, 2) NOT NULL
                );
            """)
            print("Table 'Products' created successfully")
        else:
            print("Table 'Products' already exists")

    # Customers Table
    def create_customers_table(self):
        self.cursor.execute("SHOW TABLES LIKE 'Customers'")
        if not self.cursor.fetchall():
            self.cursor.execute("""
                CREATE TABLE Customers (
                    customer_id INT PRIMARY KEY,
                    customer_name VARCHAR(100) NOT NULL,
                    country VARCHAR(100) NOT NULL,
                    city VARCHAR(50) NOT NULL
                );
            """)
            print("Table 'Customers' created successfully")
        else:
            print("Table 'Customers' already exists")

    # Orders Table
    def create_orders_table(self):
        self.cursor.execute("SHOW TABLES LIKE 'Orders'")
        if not self.cursor.fetchall():
            self.cursor.execute("""
                CREATE TABLE Orders (
                    order_id INT PRIMARY KEY,
                    customer_id INT NOT NULL,
                    datetime DATETIME NOT NULL,
                    ecommerce_website_name VARCHAR(100) NOT NULL,
                    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
                );
            """)
            print("Table 'Orders' created successfully")
        else:
            print("Table 'Orders' already exists")

    # Order Items Table
    def create_order_items_table(self):
        self.cursor.execute("SHOW TABLES LIKE 'Order_Items'")
        if not self.cursor.fetchall():
            self.cursor.execute("""
                CREATE TABLE Order_Items (
                    order_item_id INT AUTO_INCREMENT PRIMARY KEY,
                    order_id INT NOT NULL,
                    product_id INT NOT NULL,
                    qty INT NOT NULL,
                    price DECIMAL(10, 2) NOT NULL,
                    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
                    FOREIGN KEY (product_id) REFERENCES Products(product_id)
                );
            """)
            print("Table 'Order_Items' created successfully")
        else:
            print("Table 'Order_Items' already exists")

    # Payments Table
    def create_payments_table(self):
        self.cursor.execute("SHOW TABLES LIKE 'Payments'")
        if not self.cursor.fetchall():
            self.cursor.execute("""
                CREATE TABLE Payments (
                    payment_txn_id VARCHAR(50) PRIMARY KEY,
                    order_id INT NOT NULL,
                    payment_type VARCHAR(20) NOT NULL,
                    payment_txn_success CHAR(1) NOT NULL,
                    failure_reason VARCHAR(100),
                    FOREIGN KEY (order_id) REFERENCES Orders(order_id)
                );
            """)
            print("Table 'Payments' created successfully")
        else:
            print("Table 'Payments' already exists")
