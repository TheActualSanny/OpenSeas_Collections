'''Contains the class which is responsible for managing the owner name extractions.'''
import requests
import json
import threading
from configparser import ConfigParser
from main_logger import logger

lock = threading.Lock()

reader = ConfigParser()
reader.read('config.ini')

class Extract_Owners:

    def __init__(self):
        '''Sets the basic attributes'''
        self._headers = {
            'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 OPR/107.0.0.0',
            'x-api-key' : reader['USER_DETAILS']['API_KEY']
        }
        self.finalized = []

    def send_request(self, owner_id):
        '''Sends a request to the API and retrieves the Owner's name.'''
        url = 'https://api.opensea.io/api/v2/accounts/{}'.format(owner_id)
        res = requests.get(url, headers = self._headers)
        return res.content
    
    def modify_data(self, collection):
        '''This modifies the passed on values and appends the finalized collection data to the self.finalized attribute. 
        In the get_owners() method, this will be used paired with threading to speed up the performance. '''
        
        res = self.send_request(collection['owner_id'])
        data = json.loads(res)
        del collection['owner_id']
        if data['username']:
            collection['owner_name'] = data['username']
        else:
            collection['owner_name'] = None
        lock.acquire()
        self.finalized.append(collection)
        lock.release()
    
    def get_owners(self, collection_data):
        
        '''Adds the owner name to every single collection. 
        (This could be done after seperating the collections into NFTs, but all of the NFTs must be modified afterwards, so it will be slow)'''
        logger.debug('Extracting owners and adding them to the collections...')

        threads = [threading.Thread(target = self.modify_data, args = (i, )) for i in collection_data]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        logger.debug('Extractions finished! Sleeping for a minute for the API...')
        return self.finalized
    
