import pandas as pd
import sqlalchemy as db


'''
 Simulates the transform step in an ETL job
'''

def extract_table_from_mysql(table_name, my_sql_connection):
    # Extract data from mysql table
    extraction_query = 'select * from ' + table_name
    df_table_data = pd.read_sql(extraction_query,my_sql_connection)
    return df_table_data


'''
 Simulates the transform step in an ETL job
'''
def transform_data_from_table(df_table_data):
    """Converts DataFrame columns to appropriate types for BigQuery upload.

    Args:
        df_table_data (pd.DataFrame): The DataFrame to transform.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """

    # Identify columns to convert
    object_cols = df_table_data.select_dtypes(include=['object', 'datetime64[ns]']).columns

    # Define appropriate conversion functions
    def to_str(x):
        if isinstance(x, (pd.Timestamp, pd.DateOffset)):
            return x.isoformat()  # Consistent ISO format for timestamps/date offsets
        elif isinstance(x, pd.Interval):
            return f"{x.left.isoformat()}/{x.right.isoformat()}"  # Interval as start/end
        else:
            return str(x)  # Default string conversion

    def to_numeric(x):
        # Handle potential numeric conversions as needed
        if pd.api.types.is_numeric_dtype(x):
            return x
        else:
            raise ValueError(f"Value '{x}' cannot be converted to a numeric type")

    for column in object_cols:
        # Apply appropriate conversion based on column dtype
        dtype = df_table_data[column].dtype
        if dtype == 'object':
            df_table_data[column] = df_table_data[column].apply(to_str)
        elif dtype in ['datetime64[ns]', 'timedelta64[ns]']:
            df_table_data[column] = df_table_data[column].dt.strftime('%Y-%m-%d %H:%M:%S.%f')  # Consistent datetime format
        else:
            try:
                df_table_data[column] = df_table_data[column].apply(to_numeric)
            except ValueError as e:
                print(f"Error converting column '{column}': {e}")

    return df_table_data



'''
 Simulate the load step in an ETL job
'''
def load_data_into_bigquery(bq_project_id, dataset,table_name,df_table_data):
    import pandas_gbq as pdbq
    full_table_name_bg = "{}.{}".format(dataset,table_name)
    pdbq.to_gbq(df_table_data,full_table_name_bg,project_id=bq_project_id,
    if_exists='replace')

def data_pipeline_mysql_to_bq(**kwargs):
    mysql_host = kwargs.get('mysql_host')
    mysql_database = kwargs.get('mysql_database')
    mysql_user = kwargs.get('mysql_user')
    mysql_password = kwargs.get('mysql_password')
    bq_project_id = kwargs.get('bq_project_id')
    dataset = kwargs.get('dataset')

    engine = db.create_engine(f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}")
    try:
        connection = engine.connect()
        
        all_tables = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{}'
        AND table_type = 'BASE TABLE'
        """.format(mysql_database)
        df_tables = pd.read_sql(all_tables,connection,
        parse_dates={'Date': {'format': '%Y-%m-%d'}})

        for table in df_tables.TABLE_NAME:
            table_name = table
            # Extract table data from MySQL
            df_table_data = extract_table_from_mysql(table_name, connection)
            # Transform table data from MySQL
            df_table_data = transform_data_from_table(df_table_data)
            # Load data to BigQuery
            load_data_into_bigquery(bq_project_id,
            dataset,table_name,df_table_data)
            # Show confirmation message
            print("Ingested table {}".format(table_name))
            print(df_table_data)
        connection.close() #close the connection
    except Exception as e:

        print(str(e))
    finally:
        if connection:
            connection.close()


kwargs = {
 # BigQuery connection details
 'bq_project_id': "adventure-works-bq",
 'dataset': 'awdb_raw',
 # MySQL connection details
 'mysql_host': "localhost",
 'mysql_user': "root",
 'mysql_password': "pluto",
 'mysql_database': "awdb"
}
data_pipeline_mysql_to_bq(**kwargs)
