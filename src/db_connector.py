import polars as pl
import json
import mysql.connector


class DBConnector:
    def __init__(self, db_config_path: str):
        with open(db_config_path, 'r') as f:
            database_creds = json.load(f)

        # try:
        # self.mydb = mysql.connector.connect(
        #     host="test-mysql-db",
        #     user="root",
        #     password="password",
        #     allow_local_infile=True
        # )
        # print("Connection successful.")
        # except mysql.connector.Error as err:
        #     print(f"Error: {err}")

        try:
            self.mydb = mysql.connector.connect(
                host=database_creds["host"],
                user=database_creds["user"],
                password=database_creds["password"],
                port=database_creds["port"],
                allow_local_infile=True
            )
        except mysql.connector.Error as err:
            print("connection error")
            raise err

    def execute_sql_file(self, sql_file_path: str):
        mycursor = self.mydb.cursor()

        # Execute the SQL file to set up the database and loading tables
        with open(sql_file_path, 'r') as sql_file:
            sql_script = sql_file.read()

        for statement in sql_script.split(';'):
            if statement.strip():
                try:
                    mycursor.execute(statement)
                except mysql.connector.Error as err:
                    print(f"Error executing SQL statement: {statement.strip()[:100]}...")
                    print(f"MySQL Error: {err}")

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


    def load_parquet_to_mysql(self, parquet_path: str, table_name: str):
        # Read Parquet file
        df = pl.read_parquet(parquet_path)

        if df.is_empty():
            print(f"No data found in {parquet_path}")
            return

        self.push_dataframe_to_db(df, table_name)


    def push_dataframe_to_db(self, df: pl.DataFrame, table_name: str):
        if df.is_empty():
            print("DataFrame is empty. Nothing to insert.")
            return

        cursor = self.mydb.cursor()
        columns = df.columns
        placeholders = ','.join(['%s'] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"

        for row in df.iter_rows():
            cursor.execute(insert_sql, row)
        self.mydb.commit()
        cursor.close()
