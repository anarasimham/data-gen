import pyhs2
import mysql.connector
import sys

sys.path.append('../datagen')
from datagen import ManufacturingDataGenerator

class SQLDataInserter:
    conn = None
    table_name = None
    column_order = None
    last_rec_id = 0

    def __init__(self):
        pass
    def insert_rows(self, rows_json):
        insert_stmt = 'insert into '+self.table_name+' values ('

        for row_json in rows_json:
            self.last_rec_id += 1
            row_json['id'] = self.last_rec_id

            ordered_column_data = []
            for c in self.column_order:
                val = "'"+str(row_json[c])+"'"
                ordered_column_data.append(val)
            insert_stmt_loop = insert_stmt + ",".join(ordered_column_data)+")"

            cur = self.conn.cursor()
            cur.execute(insert_stmt_loop)
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

        check_stmt = 'select max(id) from '+self.table_name
        cur = self.conn.cursor()
        cur.execute(check_stmt)
        try:
            self.last_rec_id = int(cur.fetchone()[0])
        except TypeError:
            self.last_rec_id = 0

    def insert_rows(self, rows_json):
        SQLDataInserter.insert_rows(self, rows_json)

if __name__ == '__main__':
    ins = HiveInserter('<HOSTNAME>', 10500, 'admin', 'admin', 'default', 'partsdata',
            ['time','id','shortname','notes','part_loc','vibr_tolr_pct','vibr_tolr_thrs','heat_tolr_pct','heat_tolr_thrs','qty'])
    gen = ManufacturingDataGenerator()
    rows = []
    for i in range(10):
        rows.append(gen.gen_row())
    ins.insert_rows(rows)
