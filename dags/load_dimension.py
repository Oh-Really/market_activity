from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from datetime import date
import pandas as pd
import requests

class DataFetchOperator(BaseOperator):
    url = 'https://analytics.trayport.com/api/trades/activity'
    headers = {
    'x-api-key': Variable.get('api_key'),
    'accept': 'application/json'}

    paramaters = {'from': date.today().strftime('%Y-%m-%dT%H:%M:%S')}

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DataFetchOperator, self).__init__(*args, **kwargs)


    def get(self, url, params, headers):
        response = requests.get(url, params, headers=headers)
        if response.status_code != 200:
            raise Exception(f'Status: {response.status_code}. {response.content.decode()}')
        return response.content.decode("utf-8")
    
    
    def get_df(self, url, params={}, headers=headers):
        content = self.get(url, params, headers)
        #TODO replace with StringIO
        df = pd.read_json(content)
        print(df)
        return df
        

    def execute(self, context):
        self.log.info("Fetching data from API endpoint")
        return self.get_df(DataFetchOperator.url, 
                    params=DataFetchOperator.paramaters, 
                    headers=DataFetchOperator.headers)