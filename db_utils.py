import yaml
from sqlalchemy import create_engine
import pandas as pd

def read_credentials(credential_file_path):
    with open(credential_file_path) as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)
    return cfg



class RDSDatabaseConnector:
    def __init__(self,credential_file_path):
        self.credentials = read_credentials(credential_file_path)

    def get_connection(self):
        hostname = self.credentials .get("hostname", "")
        database = self.credentials .get("database", "")
        port = self.credentials.get("port", "")
        username = self.credentials .get("username", "")
        password = self.credentials .get("password", "")
        

        connection_string = f"postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{database}"
        return create_engine(
                connection_string
            )
    
    def extract_data(self):
        schema_name = self.credentials.get("schema_name", "")
        table_name = self.credentials.get("table_name", "")
        
        engine = self.get_connection()
        query = f'SELECT * FROM {schema_name}.{table_name}' 
        df = pd.read_sql(query, con=engine) 
        return df  


