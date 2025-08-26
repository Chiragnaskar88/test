import pandas as pd
from sqlalchemy import create_engine

def validate_data():


    engine = create_engine('postgresql+psycopg2://concourse_user:concourse_pass@172.21.0.2:5432/concourse')
    query = "select * from concourse_rawtable"
    df = pd.read_sql_query(query, con = engine)
    
    df_len = len(df)
    df_info = df.info
    df_dtypes = df.dtypes
    
    print(f"Length of the data after fetching is {df_len}")
    print(f"Schema of the data is : {df_info}")
    print(f"Datatype of the Column in the data {df_dtypes}")
validate_data()