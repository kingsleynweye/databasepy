from math import isnan
from sqlite3 import connect as sqlite3_connect, Connection, register_adapter as sqlite3_register_adapter
from numpy import bool_, datetime64, datetime_as_string, float32, float64, int32, int64
from pandas import DataFrame, read_csv, read_parquet, read_pickle, read_sql
from psycopg2._psycopg import connection as psycopg2_connection
from psycopg2 import connect as psycopg2_connect
from psycopg2.extensions import AsIs, register_adapter as psycopg2_register_adapter
from psycopg2.extras import execute_batch

class __Database:
    def __init__(self):
        self.register_adapters()

    def get_table(self,table_name:str) -> str:
        query = f"""SELECT * FROM {table_name}"""
        df = self.select_query(query)
        return df

    def select_query(self,query:str) -> DataFrame:
        try:
            connection = self.get_connection()
            query = self.validate_query(query)
            df = read_sql(query,connection)
        finally:
            connection.close()

        return df

    def query_from_file(self,filepath:str) -> list:
        with open(filepath,'r') as f:
            query = f.read()
        
        responses = self.query(query)
        return responses

    def query(self,query:str,**kwargs) -> list:
        query = self.validate_query(query)
        responses = []
        connection = self.get_connection()
        
        try:
            for q in query.split(';'):
                cursor = connection.execute(q,**kwargs)
                response = cursor.fetchall()
                responses.append(response)
                connection.commit()
        finally:
            connection.close()

        return responses

    def insert_file(self,filepath:str,table_name:str,**kwargs):
        df = self.read_table(filepath)
        kwargs['values'] = df.to_records(index=False)
        kwargs['fields'] = kwargs.get('fields',list(df.columns))
        kwargs['table_name'] = table_name
        kwargs['on_conflict_fields'] = kwargs.get('on_conflict_fields',None)
        kwargs['ignore_on_conflict'] = kwargs.get('ignore_on_conflict',False)
        self.insert(**kwargs)

    def insert(self):
        raise NotImplementedError

    def get_connection(self):
        raise NotImplementedError

    def register_adapters(self):
        pass

    @classmethod
    def get_insert_query(cls,table_name:str,fields:list,language='sqlite',on_conflict_fields:list=None,ignore_on_conflict:bool=False) -> str:
        language = language.lower()
        languages = {'sqlite':'?','postgresql':'%s'}
        assert language in languages.keys(), f'Valid languages are {languages}'
        fields_placeholder = ', '.join([f'\"{field}\"' for field in fields])
        values_placeholder = ', '.join([languages[language] for _ in fields])
        query = f"INSERT INTO {table_name} ({fields_placeholder}) VALUES ({values_placeholder})"

        if on_conflict_fields:
            on_conflict_update_fields = [f'\"{field}\"' for field in fields if field not in on_conflict_fields]
            on_conflict_fields_placeholder = ', '.join([f'\"{field}\"' for field in on_conflict_fields])
            on_conflict_placeholder = f'({", ".join(on_conflict_update_fields)}) = '\
                f'({", ".join(["EXCLUDED." + field for field in on_conflict_update_fields])})'

            if language == 'sqlite':
                if ignore_on_conflict or len(set(fields+on_conflict_fields)) == len(on_conflict_fields):
                    query = query.replace('INSERT','INSERT OR IGNORE')
                else:
                    query += f"ON CONFLICT ({on_conflict_fields_placeholder}) DO UPDATE SET {on_conflict_placeholder}"

            elif language == 'postgresql':
                query += f"ON CONFLICT ({on_conflict_fields_placeholder}) DO"

                if ignore_on_conflict or len(set(fields+on_conflict_fields)) == len(on_conflict_fields):
                    query += ' NOTHING'
                else:
                    query += f" UPDATE SET {on_conflict_placeholder}"

            else:
                pass
        
        else:
            pass

        query = cls.validate_query(query)
        return query

    @classmethod
    def validate_query(cls,query:str) -> str:
        query = query.replace(',)',')')
        return query

    @classmethod
    def validate_insert_values(cls,values:list) -> list:
        values = [
            [
                None if isinstance(values[i][j],(int,float)) and isnan(values[i][j])\
                    else values[i][j] for j in range(len(values[i]))
            ] for i in range(len(values))
        ]
        return values

    @classmethod
    def read_table(self,filepath:str) -> DataFrame:
        reader = {
            'csv':read_csv,
            'pkl':read_pickle,
            'parquet':read_parquet,
        }
        extension = filepath.split('.')[-1]
        method = reader.get(extension,None)

        if method is not None:
            df = method(filepath)
        else:
            raise TypeError(f'Unsupported file extension: .{extension}. Supported file extensions are {list(reader.values())}')
        
        return df

class SQLiteDatabase(__Database):
    def __init__(self,filepath:str,**kwargs):
        super().__init__()
        self.filepath = filepath
        self.kwargs = kwargs
        self.__language = 'sqlite'
    
    @property
    def filepath(self) -> str:
        return self.__filepath

    @property
    def kwargs(self) -> dict:
        return self.__kwargs
    
    @filepath.setter
    def filepath(self,filepath:str):
        self.__filepath = filepath

    @kwargs.setter
    def kwargs(self,kwargs):
        self.__kwargs = kwargs

    def get_schema(self) -> str:
        try:
            connection = self.get_connection()
            query = "SELECT * FROM sqlite_master WHERE type IN ('table', 'view')"
            schema = read_sql(self.validate_query(query),connection)['sql'].tolist()
        finally:
            connection.close()
        
        schema = '\n\n'.join(schema)
        return schema
    
    def insert(self,values:list,query:str=None,**kwargs):
        values = self.validate_insert_values(values)
        kwargs['language'] = self.__language
        query = query if query is not None else self.get_insert_query(**kwargs)
        
        try:
            connection = self.get_connection()
            connection.executemany(query,values)
            connection.commit()
        finally:
            connection.close()

    def get_connection(self) -> Connection:
        return sqlite3_connect(self.filepath,**self.__kwargs)

    def register_adapters(self):
        sqlite3_register_adapter(int64,lambda x: int(x))
        sqlite3_register_adapter(int32,lambda x: int(x))
        sqlite3_register_adapter(float64,lambda x: float(x))
        sqlite3_register_adapter(float32,lambda x: float(x))
        sqlite3_register_adapter(bool_,lambda x: bool(x))
        sqlite3_register_adapter(datetime64,lambda x: datetime_as_string(x,unit='s').replace('T',' '))

class PostgreSQLDatabase(__Database):
    def __init__(self,dbname:str,user:str,password:str,**kwargs):
        super().__init__()
        self.dbname = dbname
        self.user = user
        self.set_password(password)
        self.kwargs = kwargs
        self.__language = 'postgresql'

    @property
    def dbname(self) -> str:
        return self.__dbname

    @property
    def user(self) -> str:
        return self.__user

    @property
    def kwargs(self) -> dict:
        return self.__kwargs

    @dbname.setter
    def dbname(self,dbname:str):
        self.__dbname = dbname

    @user.setter
    def user(self,user:str):
        self.__user = user

    def set_password(self,password:str):
        self.__password = password

    @kwargs.setter
    def kwargs(self,kwargs:dict):
        self.__kwargs = kwargs

    def insert(self,values:list,query:str=None,**kwargs):
        values = self.validate_insert_values(values)
        kwargs['language'] = self.__language
        query = query if query is not None else self.get_insert_query(**kwargs)
        
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            execute_batch(cursor,query,values)
            connection.commit()
            
        finally:
            cursor.close()
            connection.close()

    def get_connection(self) -> psycopg2_connection:
        return psycopg2_connect(dbname=self.dbname,user=self.user,password=self.__password,**self.kwargs)

    def register_adapters(self):
        psycopg2_register_adapter(float64,AsIs)
        psycopg2_register_adapter(int64,AsIs)
        psycopg2_register_adapter(float32,AsIs)
        psycopg2_register_adapter(int32,AsIs)
        psycopg2_register_adapter(bool_,AsIs)