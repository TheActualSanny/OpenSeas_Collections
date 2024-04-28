'''Contains the class which is responsible for managing the owner name extractions.'''
import requests
import json
import time
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
        self._finalized = []
        self.api_counter = 0


    def send_request(self, owner_id):
        '''Sends a request to the API and retrieves the Owner's name.'''
        url = 'https://api.opensea.io/api/v2/accounts/{}'.format(owner_id)
        res = requests.get(url, headers = self._headers)
        return res.content
    
    def modify_data(self, collection):
        '''This modifies the passed on values and appends the finalized collection data to the self.finalized attribute. 
        In the get_owners() method, this will be used paired with threading to speed up the performance. '''
        try:
            res = self.send_request(collection['owner_id'])
        except KeyError:
            print(collection)
        data = json.loads(res)
        del collection['owner_id']
        if data['username']:
            collection['owner_name'] = data['username']
        else:
            collection['owner_name'] = None
        lock.acquire()
        self._finalized.append(collection)
        self.api_counter += 1
        lock.release()


    def get_owners(self, collection_data):
        
        '''Adds the owner name to every single collection. 
        (This could be done after seperating the collections into NFTs, but all of the NFTs must be modified afterwards, so it will be slow)
        This is done with multithreading. I've also added pagination to it, but for it to work, the program must sleep after every
        100 requests to the API. The same rule will be applied to extracting the NFTs'''

        logger.debug('Extracting owners and adding them to the collections...')
        length = len(collection_data)
        last_ind = 0
        for i in range(99, length, 100):
            seperate_data = collection_data[last_ind : i + 1]
            threads = [threading.Thread(target = self.modify_data, args = (b, )) for b in seperate_data]
            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()

            if length != 100 and i != length - 1:
                logger.debug('Sleeping for a minute for the API...')
                time.sleep(60)
                last_ind = i + 1
                logger.debug('Continued the extraction of the owners...')

        logger.debug('Extractions finished! Sleeping for a minute for the API...')
        return self._finalized
        

