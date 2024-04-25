'''The class which manages EVERYTHING connected to the database is contained in this module'''
import psycopg2
from configparser import ConfigParser
from main_logger import logger

reader = ConfigParser()
reader.read('config.ini')

class Database_Manager:
    def connector(self):
        '''Method which connects to the database and sets the conn and cur attributes. If it is called for the first time,
        it creates the table.'''

        self._conn = psycopg2.connect(
            host = reader['DATABASE_DETAILS']['HOST'],
            user = reader['DATABASE_DETAILS']['USER'],
            password = reader['DATABASE_DETAILS']['PASS'],
            database = reader['DATABASE_DETAILS']['DB']
        )

        self._cur = self._conn.cursor()

        self._cur.execute('''CREATE TABLE IF NOT EXISTS collection_items(
                          id SERIAL PRIMARY KEY,
                          collection text,
                          name text,
                          description text,
                          image_url text,
                          owner text,
                          twitter_username text,
                          contract_address text,
                          contract_chain text
        )''')

    def disconnect(self):
        self._cur.close()
        self._conn.close() 

    def insert_data(self, finalized_data):
        '''Inserts all of the items into the table. '''

        logger.debug('Inserting the data into the database...')
        self.connector()
        iter_counter = 0
        for item in finalized_data:
            try:
                contract_address = item['contract_address']
                contract_chain = item['contract_chain']
            except:
                contract_address = None
                contract_chain = None

            item_values = (item['collection_name'], item['name'], item['description'], item['image_url'], item['owner_name'],  item['twitter_username'],
                           contract_address, contract_chain)
            
            self._cur.execute('SELECT * FROM collection_items WHERE collection = %s', (item['collection_name'],))

            if not self._cur.fetchall() or iter_counter == 0:
                with self._conn:
                    self._cur.execute('''INSERT INTO collection_items(collection, name, description, image_url, owner, twitter_username, contract_address, contract_chain)
                                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)''', item_values)
                iter_counter += 1
            
        logger.debug('Data successfully inserted!')
                

