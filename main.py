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
    collections = cols.get_all_pages()
    owner_data = owners.get_owners(collections)
    nft_data = items.get_nfts(owner_data)
    manager.insert_data(nft_data)

# if __name__ == '__main__':
#     main() 

test = [1, 2, 3, 4]
for i, b in enumerate(test):
    print(b)