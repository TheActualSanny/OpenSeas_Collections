import psycopg2
from configparser import ConfigParser

import psycopg2.sql
from main_logger import logger

reader = ConfigParser()
reader.read('config.ini')

class Cust_ORM:
    ''' The custom ORM class. It will have all of the necessary CRUD methods '''

    def __init__(self):
        self._conn = psycopg2.connect(
            host = reader['DATABASE_DETAILS']['HOST'],
            user = reader['DATABASE_DETAILS']['USER'],
            password = reader['DATABASE_DETAILS']['PASS'],
            database = reader['DATABASE_DETAILS']['DB']
        )

        self._cur = self._conn.cursor()

        # All of the created tables will be stored here. 
        self.initialized_tables = {}

        # This will be used to check on which table we are sending the queries.
        self.selected_table = None

        self.insertion_id = 1

    def column_checker(self, columnname):
        '''This is used in all of the methods which deal with columns in order to catch exceptions'''
        try:
            self._cur.execute('SELECT {}  FROM {}'.format(columnname, self.selected_table))
        except psycopg2.errors.UndefinedColumn:
            raise ValueError('Please make sure to input a valid column name which is in the table!')


    def type_validator(self, column_name, given_type):
        '''Also used for catching exceptions'''
        try:
            self._cur.execute('ALTER TABLE {} ALTER COLUMN {} TYPE {};'.format(self.selected_table, column_name, given_type))
        except psycopg2.errors.UndefinedObject:
            raise ValueError('Make sure to pass in correct data types!')
        

    def finalized_query(self, *conditions):
        '''This is used in the SELECT methods. Basically, it checks which params the user passed to the select method (ILIKE, LIKE or IN)'''
        initial_query = ['WHERE ']
        first_checker = True
        for ind in range(len(conditions)):
            if conditions[ind] and first_checker:
                initial_query.append(conditions[ind])
                first_checker = False
            elif conditions[ind]:
                initial_query.append(' AND {}'.format(conditions[ind]))

        return ''.join(initial_query)


    def table_checker(self, tablename):
        '''Checks whether a table is initialized or not. This is used in other methods'''

        if tablename not in self.initialized_tables:
            raise 'A table with the given name is not initialized. Please try again!'

    
    def select_table(self, tablename):
        '''Selects a new table in which we want to insert the values.'''

        self.table_checker(tablename)
        self.selected_table = tablename

    
    def create_table(self, **columns):
        '''Creates a new table in the database. The user passes the columns (and the value types) as key-value pairs.'''
        if 'table_name' not in columns:
            raise ValueError('Make sure to pass the table_name as an argument!')
        
        name = columns['table_name']
        del columns['table_name']
        length = len(columns)
        #The insertion query stores the query which we need to insert rows in the table.

        insertion_query = ''.join(['%({})s, '.format(i) if b < length - 1 else ' %({})s'.format(i) for b, i in enumerate(columns)])
        self.initialized_tables[name] = insertion_query

        base = '{} {},'
        query = []
        for b, i in enumerate(columns):
            if b < length - 1:
                query.append('{} {}, '.format(i, columns[i]))
            else:
                query.append('{} {}'.format(i, columns[i]))

        with self._conn:
            self._cur.execute('CREATE TABLE IF NOT EXISTS {}({})'.format(name, ''.join(query)))
       

    def delete_table(self, tablename):
        '''Firstly it calls the table_checker method to check if the table is registered or not. If there was no exception and a 
        table with the given name was registered, it deletes it.'''
        self.table_checker(tablename)

        with self._conn:
            self._cur.execute('DROP TABLE {}'.format(tablename))
        del self.initialized_tables[tablename]
        if self.selected_table == tablename:
            self.selected_table = None

    
    def remove_column(self, columnname):
        ''' The column with the given name is dropped. For not, this doesn't have a checker, but I will add it later '''

        self.column_checker(columnname)
        with self._conn:
            self._cur.execute('ALTER TABLE {} DROP COLUMN {}'.format(self.selected_table, columnname))

    
    def change_column(self, columnname, datatype):
        ''' The datatype of the column with the given name is changed. This also doesn't have a checker currently'''
        self.column_checker(columnname)
        self.type_validator(columnname, datatype)
        with self._conn:
            self._cur.execute('ALTER TABLE {} ALTER COLUMN {} TYPE {};'.format(self.selected_table, columnname, datatype))

    
    def select_column(self, columnname):
        '''Retrieves data from the given column '''

        self.column_checker(columnname)        
        return self._cur.fetchall()
    

    def insert_rows(self, given_data, **params):
        '''Inserts the given row/s in the table. The data is passed as a list of tuples (NFTs in our case)'''
  
        if not params:
            raise ValueError('Please make sure to pass in a list/tuple of dictionaries as an argument.')
        logger.debug('Inserting the rows...')
        query = '''INSERT INTO {} VALUES({})'''.format(self.selected_table, self.initialized_tables[self.selected_table])
        if 'has_id' in params:
            for instance in given_data:
                with self._conn:
                    try:
                        self._cur.execute(query, {'id' : self.insertion_id, **instance})
                    except:
                        self._cur.execute(query, {**instance})
                self.insertion_id += 1
        else:
            for instance in given_data:
                with self._conn:
                    self._cur.execute(query, {**instance})
        logger.debug('Inserted successfully!')

    def select_rows(self, **conditions):
        ''' Method used for selecting the rows from a table. This will include Limiting the returned rows, Ordering them and filtering the data
            If you want to use these features, pass the following key-value pairs:
            Note: FOR NOW THIS ONLY WORKS ON SINGLE A SINGLE COLUMN. I WILL EXTEND THIS LATER

            limit = x (integer)
            order_column =  (columnname): It will order that column
            order_type = (ASC or DESC)
            in_val = (value1, value2 . . . ) : Pass the argument as a tuple.
            like_val = "...": Query in LIKE's syntax
            ilike_val = "..."
            

        '''
        
        limit = False
        order_by = None
        ilike = None
        like = None
        in_op = None

        if 'limit' in conditions:
            limit = 'LIMIT {}'.format(conditions['limit'])
        if 'order_type' in conditions:
            order_by = 'ORDER BY {} {}'.format(conditions['column'], conditions['order_type'])
        if 'ilike_val' in conditions:
            ilike = '{} ILIKE {}'.format(conditions['column'], repr(conditions['ilike_val']))
        if 'like_val' in conditions:
            like = '{} LIKE {}'.format(conditions['column'], repr(conditions['like_val']))
        if 'in_val' in conditions:
            in_op = '{} IN {}'.format(conditions['column'], repr(conditions['in_val']))    
    
        self.finalized_query(ilike, like, in_op)
        self._cur.execute('''SELECT * FROM {} {} {} {}'''.format(self.selected_table, self.finalized_query(ilike, like, in_op) ,'' if not order_by else order_by,  '' if not limit else limit))
        return self._cur.fetchall()


