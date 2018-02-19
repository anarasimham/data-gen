import pyhs2
import mysql.connector
import sys

class SQLDataInserter:
    conn = None
    table_name = None
    column_order = None

    def __init__(self):
        pass
    def insert_rows(self, rows_json):
        insert_stmt = 'insert into '+self.table_name+' values ('

        for row_json in rows_json:
            ordered_column_data = []
            for c in self.column_order:
                val = "'"+str(row_json[c])+"'"
                ordered_column_data.append(val)
            insert_stmt_loop = insert_stmt + ",".join(ordered_column_data)+")"
            print(insert_stmt_loop)

            cur = self.conn.cursor()
            cur.execute(insert_stmt_loop)
        self.conn.commit()
        cur.close()

class HiveInserter(SQLDataInserter):

    def __init__(self, host, port, username, password, db, table_name, column_order):
        self.conn = pyhs2.connect(host=host, port=port,
                authMechanism="PLAIN", user=username, password=password, database=db)
        self.table_name = table_name
        self.column_order = column_order

    def insert_rows(self, rows_json):
        SQLDataInserter.insert_rows(self, rows_json)

class MySQLInserter(SQLDataInserter):
    def __init__(self, host, port, username, password, db, table_name, column_order):
        self.conn = mysql.connector.connect(user=username, password=password, database=db, host=host)
        self.table_name = table_name
        self.column_order = column_order

    def insert_rows(self, rows_json):
        SQLDataInserter.insert_rows(self, rows_json)
