'''Connects all of the components. '''
from extract_collections import Extract_Collections
from extract_item import Extract_ColItems
from extract_owners import Extract_Owners
from database_manager import Database_Manager

def main():
    '''The main function. The whole functionality is connected here'''
    cols = Extract_Collections()
    items = Extract_ColItems()
    owners = Extract_Owners()
    manager = Database_Manager()
    collections = cols.get_collection_data()
    print(len(collections))
    owner_data = owners.get_owners(collections)
    print(owner_data)
    nft_data = items.get_nfts(owner_data)
    manager.insert_data(nft_data)

if __name__ == '__main__':
    main()