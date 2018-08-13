import pyhs2
import mysql.connector
import sys
import csv

sys.path.append('../datagen')
from datagen import ManufacturingDataGenerator

class DataInserter(object):
    last_rec_id = 0
    def __init__(self):
        pass
    def insert_rows(self, rows_json):
        pass
    def insert_rows_helper(self, row_json):
        self.last_rec_id += 1
        row_json['id'] = self.last_rec_id
        return row_json

class SQLDataInserter(DataInserter):
    conn = None
    table_name = None
    column_order = None
    last_rec_id = 0

    def __init__(self):
        pass
    def insert_rows(self, rows_json, do_commit):
        insert_stmt = 'insert into '+self.table_name+' values ('

        for row_json in rows_json:
            row_json = super(SQLDataInserter, self).insert_rows_helper(row_json)

            ordered_column_data = []
            for c in self.column_order:
                val = "'"+str(row_json[c])+"'"
                ordered_column_data.append(val)
            insert_stmt_loop = insert_stmt + ",".join(ordered_column_data)+")"

            cur = self.conn.cursor()
            cur.execute(insert_stmt_loop)
        if do_commit:
            self.conn.commit()
        cur.close()

class HiveInserter(SQLDataInserter):

    def __init__(self, host, port, username, password, db, table_name, column_order):
        self.conn = pyhs2.connect(host=host, port=port,
                authMechanism="PLAIN", user=username, password=password, database=db)
        self.table_name = table_name
        self.column_order = column_order

    def insert_rows(self, rows_json):
        SQLDataInserter.insert_rows(self, rows_json, False)

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
        SQLDataInserter.insert_rows(self, rows_json, True)

class CSVInserter(DataInserter):
    def __init__(self, separator, filename, column_order, do_overwrite=None):
        file_options = 'w' if do_overwrite == True or do_overwrite == None else 'a'
        self.csvfile = open(filename, file_options)
        self.writer = csv.writer(self.csvfile, delimiter=separator, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        self.column_order = column_order

    def insert_rows(self, rows_json):
        ins_arr = []
        for row_json in rows_json:
            row_json = super(CSVInserter, self).insert_rows_helper(row_json)
            for c in self.column_order:
                ins_arr.append(row_json[c])
            self.writer.writerow(ins_arr)
    def close(self):
        self.csvfile.close()

if __name__ == '__main__':
    ins = HiveInserter('<HOSTNAME>', 10500, 'admin', 'admin', 'default', 'partsdata',
            ['time','id','shortname','notes','part_loc','vibr_tolr_pct','vibr_tolr_thrs','heat_tolr_pct','heat_tolr_thrs','qty'])
    gen = ManufacturingDataGenerator()
    rows = []
    for i in range(10):
        rows.append(gen.gen_row())
    ins.insert_rows(rows)
