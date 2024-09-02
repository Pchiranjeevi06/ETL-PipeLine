import mysql.connector
import pandas as pd
from ETL_Pipeline.data_Extraction import DataReader
from ETL_Pipeline.eda import EDA
from ETL_Pipeline.Schema_design import SchemaDesign
from ETL_Pipeline.data_transformer import DataTransformer
from ETL_Pipeline.data_loader import DataLoader
from ETL_Pipeline.logger import Logger

def main():
    
    logger = Logger(log_file="p01/etl_pipeline.log")
    # Example log messages
    try:
        logger.log("Starting the ETL process...", level="info")
        # Simulate some ETL process
        logger.log("Processing data...", level="debug")
        # Simulate a warning scenario
        logger.log("Missing values found in the dataset", level="warning")
        # Simulate an error scenario
        logger.log("Failed to connect to the database", level="error")
        
    except Exception as e:
        logger.log_exception(f"Exception: {str(e)}")
        logger.log("ETL process terminated due to an error.", level="critical")
    else:
       logger.log("ETL process completed successfully!", level="info")
    
    
    try :
    # Establish database connection
        MYdb = mysql.connector.connect(
            host='localhost',
            user='root',
            password='Chiru@2002',
            database='RevStore'
        )
        cursor = MYdb.cursor()

    # Read JSON files
        data_reader = DataReader('D:\Python\P01\Data\customers.json', 'D:\Python\P01\Data\\transaction_logs.json')
        customers_df, transactions_df = data_reader.read_json()

        # Perform EDA on the read data
        # eda = EDA()
        # eda.perform_eda(customers_df, 'Customers')
        # eda.perform_eda(transactions_df, 'Transactions')

        # Schema Design
        schema_design = SchemaDesign(cursor)
        schema_design.create_products_table()
        schema_design.create_customers_table()
        schema_design.create_orders_table()
        schema_design.create_order_items_table()
        schema_design.create_payments_table()

        #  Data Transformation
        data_transformer = DataTransformer(customers_df, transactions_df)
        transformed_customers_df = data_transformer.transform_customers_data()
        transformed_orders_df = data_transformer.transform_orders_data()
        transformed_products_df = data_transformer.transform_products_data()
        transformed_order_items_df = data_transformer.transform_order_items_data()
        transformed_payments_df = data_transformer.transform_payments_data()


   #     Data Loading
        data_loader = DataLoader(cursor)

        if transformed_customers_df is not None:
            data_loader.load_customers(transformed_customers_df)

        if transformed_orders_df is not None:
            data_loader.load_orders(transformed_orders_df)

        if transformed_products_df is not None:
            data_loader.load_products(transformed_products_df)

        if transformed_order_items_df is not None:
            data_loader.load_order_items(transformed_order_items_df)

        if transformed_payments_df is not None:
            data_loader.load_payments(transformed_payments_df)

        # Commit the transaction to the database
        MYdb.commit()

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        cursor.close()
        MYdb.close()

        # Log the completion of the ETL process

   

if __name__ == "__main__":
    main()