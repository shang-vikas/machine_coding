import os,time
from typing import List, OrderedDict, Union
from threading import Lock

class Row:
    def __init__(self,values,created_at) -> None:
        self.values = values
        self.created_at = created_at
        self.updated_at = created_at
        self.lock = Lock()
    
    def update(self,newvalues) -> int:
        with self.lock:
            self.values = newvalues
            self.updated_at = time.time()
    
    def getvalues(self):
        return self.values

class Table:
    def __init__(self,columnnames_types:List[str,Union[str,int]],tablename) -> None:
        self.columnnames_types = columnnames_types
        self.tablename = tablename
        self.rows = []
        self.index = {}
        self.lock = Lock()
    
    def insert_row(self,values):
        with self.lock:
            created_at = time.time()
            newrow = Row(values,created_at)
            self.rows.append(newrow)
            # update index also
            self.index[newrow.values[0]] = len(self.rows) - 1

    def update_row(self,unique_id,newdata):
        row_index = self.index[unique_id]
        oldrow = self.rows[row_index]
        oldrow.update(newdata) 

    def get_row(self,unique_id):
        if unique_id not in self.index:
            print(f'no row present with unique_id - {unique_id}')
            return 0
        return self.rows[self.index[unique_id]]

    def delete_row(self,rowindex) -> int:
        pass

    def get_all_rows(self):
        return [row.get_values() for row in self.rows]

class Database:
    def __init__(self,database_name) -> None:
        self.databasename = database_name
        self.tables = {}
    
    def create_table(self,tablename,columnname_types) -> int:
        """supports passing columns names with types only in list format.

        Args:
            tablename (str): name of table
            columnname_types (List[str,Union[str,int]]): types can be either string or int.
        """
        is_feasible = True
        for name,datatype in columnname_types:
            if datatype not in [str,int]:
                is_feasible = False
                break
        if is_feasible == False:
            print(f'Error: Passed datatypes can be only str or int.You passed {datatype} for {name}.')
            return 0
        self.tables[tablename] = Table(columnname_types,tablename)
        return 1

    def delete_table(self,tablename):
        if tablename not in self.tables:
            print(f'Table {tablename} doesn"t exist.')
            return 0
        del self.tables[tablename]
        return 1

    def get_table(self,name) -> None:
        if name not in self.tables:
            print(f'Error: Table doesn"t exist.')
            return 0
        return self.tables[name]

    def update_table(self):
        pass # update table datatypes


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in Singleton._instances:
            Singleton._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return Singleton._instances[cls]


class DBManager(metaclass=Singleton): # this should be a singleton class.
    _instance = None
    
    def __init__(self) -> None:
        if cls._instance is not None:
            return cls._instance
        else:
            _instance = DBManager()
        return _instance
    
    def create_db(self,name) -> int:
        if name in self.databases:
            print(f'Error: Database with name {name} already exists')
            return 0
        self.databases[name] = Database(name)
        return 1
    
    def delete_db(self,name) -> int:
        if name not in self.databases:
            print(f'Error: Database doesn"t exist.')
            return 0
        try:
            del self.databases[name]
            return 1
        except Exception as e:
            print(f'Error occured while deleting database - {name}')
            return 0

    def get_db(self,name) -> Union[Database,int]:
        if name not in self.databases:
            print(f'Error: Database with name {name} doesn"t exist.')
            return 0
        return self.databases[name]