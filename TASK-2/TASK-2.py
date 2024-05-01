import pandas as pd
from sqlalchemy import create_engine

def get_file_path():
    path = 'dataset/yellow_tripdata_2023-01.parquet'
    return path

def get_dateframe(path):
    df = pd.read_parquet(path, engine='fastparquet')
    return df

def transform_data(df):
    df['passenger_count'] = df['passenger_count'].astype('Int64')
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df.fillna(0, inplace=True)
    
    return df

def get_postgres_connection():
    from sqlalchemy import create_engine 
    user = "postgres"
    host = "localhost"
    database = "postgres"
    port = 5432
    conn_string = f"postgresql://{user}@{host}:{port}/{database}"
    engine = create_engine(conn_string)
    
    return engine

def load_to_postgres(conn,clean_data):
    df.to_sql(name='TASK-2', con =conn, if_exists = 'replace', schema = 'public')
    return clean_data

path = get_file_path()
df = get_dateframe(path)
clean_data = transform_data(df)
conn = get_postgres_connection()
to_postgres = load_to_postgres(conn,clean_data)
print(to_postgres)