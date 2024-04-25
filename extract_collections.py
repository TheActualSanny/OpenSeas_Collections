'''Extracts The Collections from openseas.io . The returned data will be used in other modules to parse and clean'''

import requests
import json
from configparser import ConfigParser
from main_logger import logger

reader = ConfigParser() # I will change this into os.environ() later
reader.read('config.ini')

class Extract_Collections:

    def __init__(self):
        '''Sets the basic attributes (User-Agent and the URL itself)'''
        self._url = 'https://api.opensea.io/api/v2/collections?chain=ethereum'
        self._headers = { # By using environ, I will add the user agent as a parameter
            'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 OPR/107.0.0.0',
            'x-api-key' : reader['USER_DETAILS']['API_KEY']
        }

        self._params = {
            'limit' : int(reader['PROGRAM_CONFIG']['LIMIT'])
        }
    def write_to_lake(self, data):
        '''A simple function which writes the extracted (raw) data to to a JSON file which acts as a Data Lake.'''

        with open('data_lake/collections.json', 'r') as file:
            file_data = json.load(file)
            existing_data = file_data['collections']
            existing_data.append(data)
        
        with open('data_lake/collections.json', 'w') as file:
            json.dump(file_data, file, indent = 6)

    def extract_json(self):
        '''Extracts the JSON data.'''
        res = requests.get(self._url, headers = self._headers)
        return res.content
    
    def get_collection_data(self):
        '''Gets all of the important information from the extracted data (Collection name, contracts, usernames, etc...). The data will be represented
        as a dictionary and will be used in other modules' methods to finalize it.'''
        logger.debug('Extracting collections...')
        data = json.loads(self.extract_json())
        main_data = []
        for collection in data['collections']:
            self.write_to_lake(collection)
            if collection['twitter_username']:
                twittername = collection['twitter_username']
            else:
                twittername = None

            if collection['contracts']:
                dict_data = {'collection_name' : collection['name'], 'collection_id' : collection['collection'], 'owner_id' : collection['owner'],
                         'twitter_username' : twittername, 'contract_address' : collection['contracts'][0]['address'], 
                         'contract_chain' : collection['contracts'][0]['chain']}
            else:
                dict_data = {'collection_name' : collection['name'], 'collection_id' : collection['collection'], 'owner_id' : collection['owner'],
                         'twitter_username' : twittername, 'contracts' : collection['contracts']}
                         
            main_data.append(dict_data)
        logger.debug('Collections extracted!')
        return main_data
    
