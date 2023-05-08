import pandas as pd
from utils import utilitarios as u

class Load():
    
    def __init__(self) -> None:
        self.process = 'Load Process'    
    
    def load_to_cloud_storage(self, df, bucketName, fileName): 
        
        try:
            client = u.get_cliente_cloud_storage()
            bucket = client.get_bucket(bucketName)
            bucket.blob(fileName).upload_from_string(df.to_csv(index=False), 'text/csv',)
        except Exception as e:
            print(e)

 