import mysql.connector

def fetch_data(cursor):
    # Fetch data from the Customers table
    cursor.execute("SELECT * FROM Customers")
    customers_data = cursor.fetchall()
    print("Customers Data:")
    for row in customers_data:
        print(row)
    
    # Fetch data from the Orders table
    cursor.execute("SELECT * FROM Orders")
    orders_data = cursor.fetchall()
    print("\nOrders Data:")
    for row in orders_data:
        print(row)
    
    # Fetch data from the Products table
    cursor.execute("SELECT * FROM Products")
    products_data = cursor.fetchall()
    print("\nProducts Data:")
    for row in products_data:
        print(row)
    
    # Fetch data from the Order_Items table
    cursor.execute("SELECT * FROM Order_Items")
    order_items_data = cursor.fetchall()
    print("\nOrder Items Data:")
    for row in order_items_data:
        print(row)
    
    # Fetch data from the Payments table
    cursor.execute("SELECT * FROM Payments")
    payments_data = cursor.fetchall()
    print("\nPayments Data:")
    for row in payments_data:
        print(row)


def perform_aggregations(cursor):
    # Total sales per product
    cursor.execute("""
        SELECT p.product_name, SUM(oi.qty * oi.price) as total_sales
        FROM Order_Items oi
        JOIN Products p ON oi.product_id = p.product_id
        GROUP BY p.product_name
    """)
    sales_per_product = cursor.fetchall()
    print("\nTotal Sales Per Product:")
    for row in sales_per_product:
        print(row)
    
    # Total number of orders per customer
    cursor.execute("""
        SELECT c.customer_name, COUNT(o.order_id) as total_orders
        FROM Orders o
        JOIN Customers c ON o.customer_id = c.customer_id
        GROUP BY c.customer_name
    """)
    orders_per_customer = cursor.fetchall()
    print("\nTotal Orders Per Customer:")
    for row in orders_per_customer:
        print(row)

def validate_data(cursor):
    # Check for orders with no corresponding customer
    cursor.execute("""
        SELECT o.order_id
        FROM Orders o
        LEFT JOIN Customers c ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """)
    missing_customers = cursor.fetchall()
    print("\nOrders with No Corresponding Customer:")
    if missing_customers:
        for row in missing_customers:
            print(row)
    else:
        print("All orders have corresponding customers.")

    # Check for order items with no corresponding product
    cursor.execute("""
        SELECT oi.order_id, oi.product_id
        FROM Order_Items oi
        LEFT JOIN Products p ON oi.product_id = p.product_id
        WHERE p.product_id IS NULL
    """)
    missing_products = cursor.fetchall()
    print("\nOrder Items with No Corresponding Product:")
    if missing_products:
        for row in missing_products:
            print(row)
    else:
        print("All order items have corresponding products.")

def update_data(cursor):
    # Update city name for customers based on certain criteria
    cursor.execute("""
        UPDATE Customers
        SET city = 'Unknown'
        WHERE city IS NULL OR city = ''
    """)
    print("\nCity name updated to 'Unknown' for customers with no city specified.")


def top_customers_by_spend(cursor):
    cursor.execute("""
        SELECT c.customer_name, SUM(oi.qty * oi.price) as total_spent
        FROM Order_Items oi
        JOIN Orders o ON oi.order_id = o.order_id
        JOIN Customers c ON o.customer_id = c.customer_id
        GROUP BY c.customer_name
        ORDER BY total_spent DESC
        LIMIT 5
    """)
    top_customers = cursor.fetchall()
    print("\nTop 5 Customers by Total Spend:")
    for row in top_customers:
        print(row)



try:
    MYdb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Chiru@2002',
        database='RevStore'
    )
    cursor = MYdb.cursor()
    
    fetch_data(cursor)
    perform_aggregations(cursor)
    validate_data(cursor)
    update_data(cursor)
    top_customers_by_spend(cursor)
except Exception as e:
    print(f"Error during operations: {e}")
