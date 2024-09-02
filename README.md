# Building an ETL PipeLine
Objectives
The primary objective of this project is to develop a robust ETL (Extract, Transform, Load) pipeline that processes and normalizes JSON data files, transforming them into a structured format suitable for integration into a MySQL database. The specific goals are as follows:
1. Data Extraction: Load and aggregate JSON data from multiple files.
2. Data Normalization: Flatten and normalize nested JSON structures.
3. Data Transformation: Apply business logic and ensure data integrity.
4. Data Loading: Insert the normalized data into MySQL tables.
5. Data Testing: Test the data after loaded into Db.

Scope of the Project
This project encompasses the entire process of JSON data normalization and ETL pipeline implementation, from data extraction to database integration. The scope includes:
1. Reading and processing JSON files containing customer transaction data.
2. Normalizing nested JSON structures into a flat table format.
3. Creating separate DataFrames/tables for different entities (Customers, Orders, Products, etc.).
4. Transforming and cleaning the data as per the business requirements.
5. Loading the cleaned and normalized data into a MySQL database.
