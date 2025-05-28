'''
FastAPI: creates the API app.

HTTPException: to send error responses.

uvicorn: server used to run the FastAPI app.

nest_asyncio: lets you run uvicorn in notebooks or scripts without async issues.

CORSMiddleware: allows API to be accessed from any frontend (like JS apps).

Thread: allows the API to run in the background.

requests: used to fetch API data from endpoints.

time: to delay execution a bit.
'''

import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import urllib
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
import pandas as pd
import os
import datetime
from collections import Counter
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import nest_asyncio
import pandas as pd
from threading import Thread
import requests
import time


class StartAPI:
    def __init__(self, api_data, host="127.0.0.1", port=8000):
        self.app = FastAPI()
        self.api_data = api_data
        self.host = host
        self.port = port
        self._load_data()
        self._define_routes()
        

    def _load_data(self):
        '''
        This function reads the data from the given path
        for only 2 specific tables (order_payments, order_reviews)
        '''
        try:
            self.payments_df = pd.read_csv(self.api_data['payments'])
            self.reviews_df = pd.read_csv(self.api_data['reviews'])
        except Exception as e:
            print("Error loading CSVs:", str(e))
            raise

    def _define_routes(self):
        @self.app.get("/order_payments")
        def get_payments(offset: int = 0, limit: int = 10000):
            try:
                data = self.payments_df.iloc[offset:offset + limit].to_dict(orient="records")
                return {
                    "data": data,
                    "total_records": len(self.payments_df),
                    "offset": offset,
                    "limit": limit
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/order_reviews")
        def get_order_reviews(offset: int = 0, limit: int = 10000):
            try:
                cleaned_data = self.reviews_df.where(pd.notnull(self.reviews_df), None)
                data = cleaned_data.iloc[offset:offset + limit].to_dict(orient="records")
                return {
                    "data": data,
                    "total_records": len(cleaned_data),
                    "offset": offset,
                    "limit": limit
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/test")
        def test():
            return {"message": "Server is running!"}

    def run(self):
        nest_asyncio.apply()
        thread = Thread(target=self._start_server, daemon=True)
        thread.start()
        time.sleep(2)

    def _start_server(self):
        uvicorn.run(self.app, host=self.host, port=self.port)

class SQLConnection:
    def __init__(self, config):
        self.config = config
        self.engine = None
    
    def connect(self):
        try:
            conn_str = f"DRIVER={self.config['driver']};SERVER={self.config['server']};DATABASE={self.config['database']};Trusted_Connection=yes;"
            self.engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")
            
            # Test connection immediately
            with self.engine.connect() as connection:
                print("Successfully connected to SQL Server!")
                
        except SQLAlchemyError as e:
            self.engine = None
            print(f"Connection failed: {e}")
            raise RuntimeError("Database connection failed") from e
    
    def disconnect(self):
        if self.engine:
            self.engine.dispose()
            print("Connection closed")

class CreateDataBaseTables:
    def __init__(self, connection):
        self.engine = connection.engine

    def table_exists(self, table_name):
        with self.engine.connect() as conn:
            return conn.execute(
                text("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :name"),
                {"name": table_name}
            ).scalar() is not None

    def create_tables(self, script_path):
        with open(script_path, 'r') as file:
            olist_ddl_script = file.read()

        statements = olist_ddl_script.split(';')
        with self.engine.begin() as conn:
            for statement in statements:
                statement = statement.strip()
                if statement.upper().startswith("CREATE TABLE"):
                    try:
                        # Get table name
                        words = statement.split()
                        table_index = words.index("TABLE") + 1
                        table_name = words[table_index].replace('[','').replace(']','')
                        
                        if not self.table_exists(table_name):
                            print(f"Creating table: {table_name}")
                            conn.execute(text(statement))
                        else:
                            print(f"Table already exists: {table_name}")
                    except Exception as e:
                        print(f"Skipping problematic statement: {e}")
                else:
                    continue

#ETL: extract, transform, load
class Extract:  

    # This function can remain outside the class to fetch data
    def fetch_all_data_api(self, endpoint: str, chunk_size: int = 10000, base_url: str = "http://localhost:8000"):
        all_data = []
        offset = 0
        total_records = None

        try:
            initial_response = requests.get(f"{base_url}/{endpoint}", params={"limit": 1})
            initial_response.raise_for_status()
            total_records = initial_response.json()["total_records"]

            while offset < total_records:
                params = {
                    "offset": offset,
                    "limit": chunk_size
                }
                response = requests.get(f"{base_url}/{endpoint}", params=params)
                response.raise_for_status()

                chunk_data = response.json()["data"]
                all_data.extend(chunk_data)

                offset += len(chunk_data)
                print(f"Fetched {len(chunk_data)} records (total: {offset}/{total_records})", end="\r")

            print(f"\n Successfully fetched all {total_records} records from {endpoint}")
            return pd.DataFrame(all_data)

        except Exception as e:
            print(f"\n Error at offset {offset}: {str(e)}")
            return pd.DataFrame(all_data)

    def extract_from_csvs(self, folder_path):
        actual_dfs = {}
        for file in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file)
                if file.startswith('olist_') and '_dataset.csv' in file:
                    df_key = file.replace('olist_', '').replace('_dataset.csv', '')
                else:
                    df_key = file.replace('.csv', '')

                actual_dfs[df_key] = pd.read_csv(file_path)
        #update actual_dfs
        geo = actual_dfs['geolocation'] 
        geo = geo.drop_duplicates()
        actual_dfs['geolocation']  = geo
        actual_dfs.pop('order_payments')
        actual_dfs.pop('order_reviews')
        actual_dfs['order_payments'] = self.fetch_all_data_api('order_payments')
        actual_dfs['order_reviews'] = self.fetch_all_data_api('order_reviews')
        print(actual_dfs.keys())

        #self.extract_from_api(actual_dfs)
        return actual_dfs

class LogsAndErrors:
    def __init__(self, connection):
        self.engine = connection.engine

    def invalid_pks_log(table_name, invalid_rows, pk_cols, pk_log_file_path):

        if invalid_rows.empty:
            return  # No errors to log

        # Step 1: Create error messages
        error_messages = [
            f"NULL values in PK columns: {pk_cols}"
            for _, row in invalid_rows.iterrows()
        ]
        
        # Step 2: Count duplicates
        error_counts = Counter(error_messages)
        
        # Step 3: Write to CSV
        log_entries = []
        for error_detail, count in error_counts.items():
            log_entries.append({
                "Table Name": table_name,
                "Error Type": "Invalid PK",
                "Affected Records Count": count,
                "Error Details": error_detail,
                "Timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # Convert to DataFrame
        log_df = pd.DataFrame(log_entries)

        # Append to CSV
        log_df.to_csv(pk_log_file_path, mode='a', index=False, header=not os.path.exists(pk_log_file_path))

    
    def invalid_fks_log(child_table_name, parent_table_name, fk_column, invalid_rows, fk_log_file_path):
        """
        Logs summarized FK errors (with counts) into a CSV file.

        Parameters:
            child_table_name (str): Table where FK is defined
            parent_table_name (str): Referenced table
            fk_column (str): FK column name
            invalid_rows (DataFrame): DataFrame of invalid FK rows
        """

        if invalid_rows.empty:
            return  # No errors to log

        # Step 1: Create error messages
        error_messages = [
            f"{fk_column}={row[fk_column]} not found in {parent_table_name}.{fk_column}"
            for _, row in invalid_rows.iterrows()
        ]
        
        # Step 2: Count duplicates
        error_counts = Counter(error_messages)
        
        # Step 3: Write to CSV
        log_entries = []
        for error_detail, count in error_counts.items():
            log_entries.append({
                "Table Name": child_table_name,
                "Error Type": "Invalid FK",
                "Affected Records Count": count,
                "Error Details": error_detail,
                "Timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # Convert to DataFrame
        log_df = pd.DataFrame(log_entries)
        
        # Append to CSV
        log_df.to_csv(fk_log_file_path, mode='a', index=False, header=not os.path.exists(fk_log_file_path))

    
    def log_counts(self, actual_dfs,  log_file_path):
        '''
        Compares expected vs. actual row count in the database and logs results to a CSV file.
        
        Parameters:
            actual_dfs (dict): Dictionary of table names and their corresponding DataFrames.
            engine (SQLAlchemy Engine): Database engine to connect and run queries.
            log_file_path (str): Path to the CSV log file.
        '''
        log_entries = []

        for table_name, df in actual_dfs.items():
            print(f"Checking record count for '{table_name}'...")
            expected_count = len(df)
            
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                actual_count = result.scalar()
            
            log_entries.append([
                table_name,
                expected_count,
                actual_count,
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ])

        # Convert to DataFrame
        log_df = pd.DataFrame(log_entries, columns=["Table Name", "Expected Records", "Actual Records", "Timestamp"])
        
        # Save to CSV (append mode, write header only if file doesn't exist)
        log_df.to_csv(log_file_path, mode='a', header=not os.path.exists(log_file_path), index=False)

    def log_data_quality_issues(table_name, bad_rows, error_msg, data_issues_path):

        if bad_rows.empty:
            return  # No errors to log

        # Step 1: Create error messages
        error_messages = [
            error_msg
            for _, row in bad_rows.iterrows()
        ]
        
        # Step 2: Count duplicates
        error_counts = Counter(error_messages)
        
        # Step 3: Write to CSV
        log_entries = []
        for error_detail, count in error_counts.items():
            log_entries.append({
                "Table Name": table_name,
                "Affected Records Count": count,
                "Error Details": error_detail,
                "Timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # Convert to DataFrame
        log_df = pd.DataFrame(log_entries)
        
        # Append to CSV
        log_df.to_csv(data_issues_path, mode='a', index=False, header=not os.path.exists(data_issues_path))

class TransformPrimaryKey:

    def __init__(self, connection):
          self.engine = connection.engine


    def detect_primary_keys(self):
        
        """Automatically find all table PKs (single or composite)"""
        pk_dict = {}
        
        # Query to get PK information from SQL Server
        pk_query = text("""
            SELECT 
                tc.TABLE_NAME, 
                ccu.COLUMN_NAME
            FROM 
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN 
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE ccu 
                ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME 
            WHERE 
                tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY 
                tc.TABLE_NAME, 
                ccu.ORDINAL_POSITION
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(pk_query)
            for table, column in result:
                if table not in pk_dict:
                    pk_dict[table] = []
                pk_dict[table].append(column)
        
        return pk_dict

    def validate_pk_values(self, df, table_name,pk_log_file_path):

        # Get all PKs 
        table_pks = self.detect_primary_keys()
        
        """Remove rows with NULLs in PK columns and log errors"""
        if table_name not in table_pks:
            return df  # No PK defined
        
        pk_cols = table_pks[table_name] #table_pks['customers'] --> 'customer_id'
        
        # Find rows with missing PK values
        null_rows = df[pk_cols].isnull().any(axis=1)
        
        if null_rows.any():
            print(f"Removing {null_rows.sum()} rows with NULL PK values from {table_name}")
            
            # Log errors for removed rows
            invalid_rows = df[null_rows]
            for _, row in invalid_rows.iterrows():
                LogsAndErrors.invalid_pks_log(table_name, invalid_rows, pk_cols, pk_log_file_path)

            return df[~null_rows]
        
        return df
    
    def update_dfs(self, pk_log_file_path):
        cleaned_pk_dfs = {}
        for table_name, df in actual_dfs.items():
            cleaned_pk_dfs[table_name] = self.validate_pk_values(df, table_name, pk_log_file_path)
        return cleaned_pk_dfs

class TransformForiegnKey:

    def check_invalid_fks(self,parent_df, child_df, fk_column, parent_table_name, child_table_name, fk_log_file_path):
        """
        Validates foreign keys and logs errors for invalid FK values.
        
        Parameters:
            parent_df (DataFrame): Referenced table (has PK)
            child_df (DataFrame): Table with the FK
            fk_column (str): The FK column name
            parent_table_name (str): Name of the referenced table (for logging)
            child_table_name (str): Name of the table containing the FK
            
        Returns:
            DataFrame: The cleaned ref_df with only valid FK values
        """
        invalid_fk_rows = child_df[
            (~child_df[fk_column].isin(parent_df[fk_column])) & (~child_df[fk_column].isna())
        ] 

        LogsAndErrors.invalid_fks_log(
            child_table_name=child_table_name,
            parent_table_name=parent_table_name,
            fk_column=fk_column,
            invalid_rows=invalid_fk_rows,
            fk_log_file_path = fk_log_file_path
        )
        
        # Return only valid rows
        valid_df = child_df[child_df[fk_column].isin(parent_df[fk_column]) | (child_df[fk_column].isna())
        ]
        return valid_df

        
    
    def transform_fks(self, cleaned_pk_dfs, fk_log_file_path):

        #orders.customer_id → customers.customer_id
        clean_orders = self.check_invalid_fks(
            parent_df = cleaned_pk_dfs['customers'], #pk
            child_df= cleaned_pk_dfs['orders'], #fk
            fk_column='customer_id',
            parent_table_name='customers',
            child_table_name='orders',
            fk_log_file_path=fk_log_file_path
        )

        #order_payments.order_id → orders.order_id
        clean_order_payments = self.check_invalid_fks(
            parent_df= clean_orders, #dfs['orders'], #pk
            child_df= cleaned_pk_dfs['order_payments'], #fk
            fk_column='order_id',
            parent_table_name='orders',
            child_table_name='order_payments',
            fk_log_file_path=fk_log_file_path
        )

        #products.product_category_name → product_category_name_translation.product_category_name
        clean_products = self.check_invalid_fks(
            parent_df= cleaned_pk_dfs['product_category_name_translation'], #pk
            child_df= cleaned_pk_dfs['products'], #fk
            fk_column='product_category_name',
            parent_table_name='product_category_name_translation',
            child_table_name='products',
            fk_log_file_path=fk_log_file_path
        )

        #order_items.order_id → orders.order_id
        clean_order_items = self.check_invalid_fks(
            parent_df= clean_orders,#dfs['orders'], #pk
            child_df= cleaned_pk_dfs['order_items'], #fk
            fk_column='order_id',
            parent_table_name='orders',
            child_table_name='order_items',
            fk_log_file_path=fk_log_file_path
        )

        #order_items.seller_id -> sellers.seller_id
        clean_order_items = self.check_invalid_fks(
        parent_df=cleaned_pk_dfs['sellers'], #pk
        child_df= clean_order_items, #cleaned_pk_dfs['order_items'], #fk
        fk_column='seller_id',
        parent_table_name='sellers',
        child_table_name='order_items',
        fk_log_file_path=fk_log_file_path
        )

        #order_items.product_id → products.product_id
        clean_order_items = self.check_invalid_fks(
        parent_df= clean_products, #pk
        child_df= clean_order_items, #cleaned_pk_dfs['order_items'], #fk
        fk_column='product_id',
        parent_table_name='products',
        child_table_name='order_items',
        fk_log_file_path=fk_log_file_path
        )

        #order_reviews.order_id → orders.order_id

        clean_order_reviews = self.check_invalid_fks(
        parent_df=clean_orders, #dfs['orders'], #pk
        child_df= cleaned_pk_dfs['order_reviews'], #fk
        fk_column='order_id',
        parent_table_name='orders',
        child_table_name='order_reviews',
        fk_log_file_path=fk_log_file_path
        )

        clean_dfs = {
        'orders': clean_orders,
        'products': clean_products,
        'order_items': clean_order_items,
        'order_reviews': clean_order_reviews,
        'order_payments': clean_order_payments,
        'customers': cleaned_pk_dfs['customers'],
        'sellers': cleaned_pk_dfs['sellers'],
        'geolocation': cleaned_pk_dfs['geolocation'],
        'product_category_name_translation': cleaned_pk_dfs['product_category_name_translation']
        
        }
        return clean_dfs

class DataQualityIssues:

    def check_all_missing_values(self, clean_dfs, data_issues_path):
        for table_name, df in clean_dfs.items():
            print(f"Checking missing values in '{table_name}'...")
            missing_data = df.isnull()
            for col in df.columns:
                missing_rows = df[missing_data[col]]
                LogsAndErrors.log_data_quality_issues(table_name, missing_rows, f"Null value in column '{col}'", data_issues_path )

    def check_all_duplicates(self, clean_dfs, data_issues_path):
        for table_name, df in clean_dfs.items():
            print(f"Checking duplicates in '{table_name}'...")
            duplicate_rows = df[df.duplicated(keep=False)]  # Detect all duplicate rows
            LogsAndErrors.log_data_quality_issues(table_name, duplicate_rows, "Entire row is duplicated", data_issues_path)

    # Convert date columns to datetime, coercing invalid dates to NaT
    def check_order_dates(self, df, table_name, data_issues_path):
        date_cols = [
            'order_purchase_timestamp',  # Adjusted name
            'order_approved_at',
            'order_delivered_carrier_date',
            'order_delivered_customer_date'
        ]
        
        for col in date_cols:
            #df[col] = pd.to_datetime(df[col], errors='coerce')
            df.loc[:, col] = pd.to_datetime(df[col], errors='coerce')

        
        # Define conditions for date logic errors
        # 1. Check order where dates exist
        purchase_before_approved = df['order_purchase_timestamp'] > df['order_approved_at']
        approved_before_carrier = df['order_approved_at'] > df['order_delivered_carrier_date']
        carrier_before_customer = df['order_delivered_carrier_date'] > df['order_delivered_customer_date']
        
        all_conditions = (
            purchase_before_approved | approved_before_carrier | carrier_before_customer)
        
        # Filter bad rows
        bad_rows = df[all_conditions]
        
        # Optional: Reset index if needed
        bad_rows = bad_rows.reset_index(drop=True)
        #return bad_rows
        LogsAndErrors.log_data_quality_issues(table_name, bad_rows, 'Date logic error', data_issues_path)
       
class CheckBeforeInsertion:
    def __init__(self, connection):
        self.engine = connection.engine
        self.TPK = TransformPrimaryKey(connection=self.engine)

    def check_insertion(self, df, table_name): 
        table_pks = self.TPK.detect_primary_keys()
        try:    
            with self.engine.connect() as conn:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                if count == 0:
                    # Initial load - insert everything
                    df.to_sql(table_name, self.engine, index=False, if_exists='append')
                    print(f"Initial insert: {len(df)} rows to {table_name}")
                    return

            # Step 4: Get existing PKs
            with self.engine.connect() as conn:
                pk_cols = table_pks[table_name] 
                existing_pks= pd.read_sql(f"SELECT {', '.join(pk_cols)} FROM {table_name}", conn) #select cusid, orderid from customers :
                existing_pks = existing_pks.astype(str).apply(tuple, axis=1) 
                
            # Step 5: Find new records
            new_pks = df[pk_cols].astype(str).apply(tuple, axis=1)
            new_records = df[~new_pks.isin(existing_pks)]
            
            if new_records.empty:
                print(f"No new records for {table_name}")
                return

            # Step 6: Insert new records
            new_records.to_sql(table_name, self.engine, index=False, if_exists='append')
            print(f"Inserted {len(new_records)} new rows to {table_name}")

        except Exception as e:
            print(f"Error in {table_name}: {str(e)}")
    
class Load:
    def __init__(self, connection):
        self.engine = connection.engine
        self.CBI = CheckBeforeInsertion(connection=self.engine)

    def load_clean_data(self,clean_dfs):
        load_order = ['sellers', 'customers', 'product_category_name_translation', 'orders', 'products', 'order_payments', 'order_reviews', 'order_items', 'geolocation']
        # Load tables in dependency order
        print("\n Starting data load process...")
        for table_name in load_order:
            if table_name in clean_dfs:
                print(f"\nProcessing {table_name}...")
                self.CBI.check_insertion(clean_dfs[table_name], table_name)
            else:
                print(f"\n Skipping {table_name} - no data to load")
            


if __name__ == "__main__":

    #in this dict, chnage the paths to yours.
    #these paths for data that will be accecced by API
    api_data = {
            "payments" :  r'your_path',
            "reviews" :  r'your_path'
          }
    
    '''
        in this dict change the paths to yours
        Note: olist_ddl is a file to be downloaded from github repo
        olist_raw: olist raw data path

        --> these 4 below files, you just need to add the path, do not change file name, it will be created when running the script
        record_log: log file path (#of records in raw data csvs, and #of records inserted in SQL server database)
        pk_log: invalid_pks file path
        fk_log: invalid_fks file path
        issues_log: data quality issues file path
    '''
    log_paths = {
        "olist_raw" : r'your_path',
        "olist_ddl" : r'your_path\olist_ddl.txt',
        "record_log" :  r'your_path'log_file.csv",
        "pk_log" : r'your_path'\invalid_pks.csv", 
        "fk_log" : r'your_path'\invalid_fks.csv",
        "issues_log" : r'your_path'\data_quality_issues.csv",
        "test_products" : r'your_path'\test_products.csv"
         }
    
    # change to your connection settings
    config = {
        "driver": "your driver",
        "server": "your server",
        "database": "your database name"
    }

    #start api
    API_starter = StartAPI(api_data)
    API_starter.run()

    # Create connection instance
    connector = SQLConnection(config)

    try:
        connector.connect()
        # Your database operations would go here
        
    except RuntimeError as e:
        print(f"Operation failed: {e}")
        
    finally:
        connector.disconnect()

    #create DB tables
    creator = CreateDataBaseTables(connector)
    creator.create_tables(log_paths['olist_ddl'])

    #extract data from csvs and api
    extractor = Extract()
    actual_dfs =  extractor.extract_from_csvs(log_paths['olist_raw'])

    #check and clean pks
    pk_transformer = TransformPrimaryKey(connector)
    cleaned_pk_dfs = pk_transformer.update_dfs(log_paths['pk_log'])

    #check and clean fks
    fk_transformer = TransformForiegnKey()
    clean_dfs = fk_transformer.transform_fks(cleaned_pk_dfs, log_paths['fk_log'])

    #Load data to sql server
    Loader = Load(connector)
    Loader.load_clean_data(clean_dfs)

    #save to log file 
    count_logger = LogsAndErrors(connector)
    count_logger.log_counts(actual_dfs, log_paths['record_log'])

    #save data quality issues
    issues_logger = DataQualityIssues()
    issues_logger.check_all_missing_values(clean_dfs, log_paths['issues_log'])
    issues_logger.check_all_duplicates(clean_dfs, log_paths['issues_log'])
    issues_logger.check_order_dates(clean_dfs['orders'],'orders', log_paths['issues_log'] )

   # delta = DeltaAndHistory(connector)
   # delta.apply_delta_type2(log_paths['test_products'], clean_dfs, 'products', 'product_id')
    


    

    
    
    
