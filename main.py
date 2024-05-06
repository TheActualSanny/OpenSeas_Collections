'''Connects all of the components. '''
from extract_collections import Extract_Collections
from extract_item import Extract_ColItems
from extract_owners import Extract_Owners
from custom_orm import Cust_ORM

def main():
    '''The main function. The whole functionality is connected here'''
    cols = Extract_Collections()
    items = Extract_ColItems()
    owners = Extract_Owners()
    orm = Cust_ORM()
    orm.create_table(table_name = 'collection_items',  id = 'SERIAL PRIMARY KEY', collection_name = 'TEXT',  name = 'TEXT', description = 'TEXT', image_url = 'TEXT',
                    owner_name = 'TEXT', twitter_username = 'TEXT', contract_address = 'TEXT', contract_chain = 'TEXT') 
                    
    orm.select_table('collection_items')
    collections = cols.get_all_pages()
    owner_data = owners.get_owners(collections)
    nft_data = items.get_nfts(owner_data)
    orm.insert_rows(nft_data, has_id = True)

if __name__ == '__main__':
    main() 