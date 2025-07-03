import polars as pl
import json
import mysql.connector


class DBConnector:
    def __init__(self, db_config_path: str):
        with open(db_config_path, 'r') as f:
            database_creds = json.load(f)
        
        self.mydb = mysql.connector.connect(
            host=database_creds["host"],
            user=database_creds["user"],
            password=database_creds["password"],
            allow_local_infile=True
        )
    
    def execute_sql_file(self, sql_file_path: str):
        mycursor = self.mydb.cursor()
        
        # Execute the SQL file to set up the database and loading tables
        with open(sql_file_path, 'r') as sql_file:
            sql_script = sql_file.read()
        
        for statement in sql_script.split(';'):
            if statement.strip():
                mycursor.execute(statement)
        
        self.mydb.commit()
        mycursor.close()
    
    def get_query_result(self, query: str) -> pl.DataFrame:
        db_cursor = self.mydb.cursor()
        db_cursor.execute(query)
        
        rows = db_cursor.fetchall()
        if db_cursor.description is not None:
            columns = [desc[0] for desc in db_cursor.description]
            df = pl.DataFrame(rows, schema=columns)
        else:
            df = pl.DataFrame()
        db_cursor.close()
        return df

        