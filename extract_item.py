'''The class which manages extraction the singular items is written here'''

import threading
import requests
import json 
import time
from configparser import ConfigParser
from main_logger import logger

lock = threading.RLock()

reader = ConfigParser()
reader.read('config.ini')

class Extract_ColItems():
    def __init__(self):
        '''Basic attributes are initialized here.'''
        self._headers = {
            'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 OPR/107.0.0.0',
            'x-api-key' : reader['USER_DETAILS']['API_KEY']
        }
        self.finalized_nfts = []

    def extract_json(self, collection):
        '''Gets the JSON data about a collection. As the the collection is a path parameter, this sets a new URL for every item. Will
        probably change in the future as this is not optimized.'''

        self._url = 'https://api.opensea.io/api/v2/collection/{}/nfts'.format(collection)
        res = requests.get(self._url, headers = self._headers)
        return res.content
    
    def get_item_data(self, collection):
        '''Gets the necessarry item data about an item(singular collection), finalizes it and appends it to the self.finalized_nfts
        attribute. While fetching the nfts of the collections, 
        the collection_id key in the dictionaries will also be deleted. This is seperated from the get_nfts() in order to add multithreading'''

        raw_data = self.extract_json(collection['collection_id'])
        data = json.loads(raw_data)
        if data:
            lock.acquire()
            self.write_to_lake(data['nfts'])
            lock.release()
        del collection['collection_id']
        
        if not data['nfts']:
            instance = {'name' : None, 'description' : None, 'image_url' : None, **collection}
            self.finalized_nfts.append(instance)

        lock.acquire()
        for nft in data['nfts']:
            nft_name = nft['name']
            nft_description = nft['description']
            image = nft['image_url']
            instance = {'name' : nft_name, 'description' : nft_description, 'image_url' : image, **collection}
            self.finalized_nfts.append(instance)
        lock.release()
    
    def write_to_lake(self, data):
        '''Just like the collections, this writes NFTs in a data lake.'''
        try:
            with open('data_lake/collection_items.json', 'r') as file:
                file_data = json.load(file)
                existing_data = file_data['nfts']
                existing_data.append(data)

            with open('data_lake/collection_items.json', 'w') as file:
                json.dump(file_data, file, indent = 6)
        except ValueError:
            return
        
    def get_nfts(self, collection_data):
        '''The Main loop, which gets all of the NFTs via Multithreading'''
        time.sleep(60)
        logger.debug('Extracting NFTs...')
        length = len(collection_data)
        last_ind = 0
        for col in range(99, length, 100):
            current_data = collection_data[last_ind : col + 1]
            threads = [threading.Thread(target = self.get_item_data, args = (i, )) for i in current_data]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            if length != 100 and col != length - 1:
                last_ind = col + 1
                logger.debug('Sleeping for a minute for the API...')
                time.sleep(60)
                logger.debug('Continued the extraction...')

        logger.debug('Successfully extracted the NFTs!')
        return self.finalized_nfts

