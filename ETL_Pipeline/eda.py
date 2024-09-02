import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

class EDA:
    def perform_eda(self, df, table_name):
        print(f"--- EDA for {table_name} Table ---")
        
        # Step 1: Data Overview
        print("\n1. Data Overview:")
        print(f"Shape of {table_name} dataset: {df.shape}")
        print(f"Columns in {table_name} dataset: {df.columns.tolist()}")
        print(f"Data types:\n{df.dtypes}")
        
        print("\nFirst few rows:\n", df.head())
        print("\nLast few rows:\n", df.tail())

        # Step 2: Missing Values
        print("\n2. Missing Values:")
        missing_values = df.isnull().sum()
        missing_percentage = (missing_values / len(df)) * 100
        print(pd.DataFrame({'Missing Values': missing_values, 'Percentage': missing_percentage}))

        # Step 3: Statistical Summary
        print("\n3. Statistical Summary:")
        print(df.describe(include='all'))

        # Step 4: Distribution Analysis
        print("\n4. Distribution Analysis:")
        self.plot_distributions(df, table_name)

        # Step 5: Correlation Analysis
        print("\n5. Correlation Analysis:")
        self.plot_correlation_matrix(df, table_name)

        # Step 6: Outlier Detection
        print("\n6. Outlier Detection:")
        self.plot_outliers(df, table_name)
    
    def plot_distributions(self, df, table_name):
        # Plotting distribution of numerical columns
        num_cols = df.select_dtypes(include=['float64', 'int64']).columns
        df[num_cols].hist(bins=15, figsize=(15, 10), layout=(len(num_cols) // 3 + 1, 3))
        plt.suptitle(f"Distribution of Numerical Features in {table_name}")
        plt.show()

        # Plotting distribution of categorical columns
        cat_cols = df.select_dtypes(include=['object']).columns
        for col in cat_cols:
            plt.figure(figsize=(8, 4))
            df[col].value_counts().plot(kind='bar')
            plt.title(f"Distribution of {col} in {table_name}")
            plt.show()

    def plot_correlation_matrix(self, df, table_name):
        num_cols = df.select_dtypes(include=['float64', 'int64']).columns
        corr_matrix = df[num_cols].corr()
        plt.figure(figsize=(10, 8))
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1)
        plt.title(f"Correlation Matrix for {table_name}")
        plt.show()

    def plot_outliers(self, df, table_name):
        num_cols = df.select_dtypes(include=['float64', 'int64']).columns
        plt.figure(figsize=(15, 10))
        df[num_cols].boxplot()
        plt.title(f"Outliers in Numerical Features of {table_name}")
        plt.show()


