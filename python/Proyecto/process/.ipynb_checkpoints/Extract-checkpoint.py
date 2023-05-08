import os
from io import StringIO
import sqlalchemy as db
from sqlalchemy import text
import pandas as pd
from pandas import DataFrame

from utils import utilitarios as u



class Extract():
    
    def __init__(self) -> None:
        self.process = 'Extract Process'
    
    def read_mysql(self, database_name, table_name):
        
        mysql_credentials = u.get_mysql_credentials()
        user = mysql_credentials["user"]
        password = mysql_credentials["password"]
        port = mysql_credentials["port"]
        engine = db.create_engine(f"mysql://{user}:{password}@192.168.1.62:{port}/{database_name}")
        conn = engine.connect()
        df = pd.read_sql_query(text(f'SELECT * FROM {table_name}'), con=conn)
        
        return df

    def read_cloud_storage(self, bucketName, fileName):

        client = u.get_cliente_cloud_storage()
        bucket = client.get_bucket(bucketName)
        blob = bucket.get_blob(fileName)
        downloaded_file = blob.download_as_text(encoding="utf-8")
        df = pd.read_csv(StringIO(downloaded_file))
        
        return df
