import os
import yaml
from io import StringIO
from google.cloud.storage import Client

with open('/user/app/Proyecto/config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

def get_cliente_cloud_storage():
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["cloud_storage"]["credentials"]
    client = Client()
    
    return client


def get_mysql_credentials():
    user = config["mysql"]["user"]
    password = config["mysql"]["pass"]
    port = config["mysql"]["port"]
    
    return {"user": user, "password": password, "port": port}
