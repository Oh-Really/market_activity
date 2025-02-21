from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from datetime import date
from io import StringIO  
import pandas as pd
import requests

class DataFetchOperator(BaseOperator):
    trades_url = 'https://analytics.trayport.com/api/trades/activity'
    insts_url = 'https://referencedata.trayport.com/instruments'
    # seqs_url = 'https://referencedata.trayport.com/instruments/{instrumentId}/sequences'
    # seq_items_url = 'https://referencedata.trayport.com/sequences/{sequenceId}/sequenceItems'

    headers = {
    'x-api-key': Variable.get('api_key'),
    'accept': 'application/json'}

    paramaters = {'from': date.today().strftime('%Y-%m-%dT%H:%M:%S')}

    @apply_defaults
    def __init__(self, endpoint: str, *args, **kwargs):
        super(DataFetchOperator, self).__init__(*args, **kwargs)
        self.endpoint = endpoint


    def get(self, url, params, headers):
        response = requests.get(url, params, headers=headers)
        self.log.info(response.request.url)
        if response.status_code != 200:
            raise Exception(f'Status: {response.status_code}. {response.content.decode()}')
        return response.content.decode("utf-8")
    
    
    def get_df(self, url, params={}, headers=headers):
        content = self.get(url, params, headers)
        df = pd.read_json(StringIO(content))
        print(df)
        return df
        

    def execute(self, context):
        self.log.info("Fetching data from API endpoint")
        if self.endpoint == 'trades':
            url = DataFetchOperator.trades_url
        elif self.endpoint == 'instruments':
            url = DataFetchOperator.insts_url
        return self.get_df(url, 
                    params=DataFetchOperator.paramaters, 
                    headers=DataFetchOperator.headers)