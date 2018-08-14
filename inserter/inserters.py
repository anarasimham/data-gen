import pyhs2
import mysql.connector
import sys
import csv
import os

sys.path.append('../datagen')
from datagen import ManufacturingDataGenerator

class DataInserter(object):
    def __init__(self, start_id):
        self.last_rec_id = start_id
    def insert_rows(self, rows_json):
        pass
    def insert_rows_helper(self, row_json):
        row_json['id'] = self.last_rec_id
        self.last_rec_id += 1
        return row_json

class SQLDataInserter(DataInserter):
    conn = None
    table_name = None
    column_order = None

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
            self.last_rec_id = 1

    def insert_rows(self, rows_json):
        SQLDataInserter.insert_rows(self, rows_json, True)

class CSVInserter(DataInserter):
    def __init__(self, filename, separator, column_order, start_id=None):
        if start_id == None:
            start_id = 1
        super(CSVInserter, self).__init__(start_id)
        self.file_count = 0
        self.filename_base = filename
        self.separator = separator
        self.init_file()
        self.column_order = column_order

    def init_file(self):
        filename = self.filename_base[0:self.filename_base.index('.')]+'_'+str(self.last_rec_id)+self.filename_base[self.filename_base.index('.'):len(self.filename_base)]
        self.csvfile = open(filename, 'w')
        self.writer = csv.writer(self.csvfile, delimiter=self.separator, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        self.file_count += 1

    def insert_rows(self, rows_json):
        rows_to_write = []
        for row_json in rows_json:
            ins_arr = []
            row_json = super(CSVInserter, self).insert_rows_helper(row_json)
            for c in self.column_order:
                ins_arr.append(row_json[c])
            rows_to_write.append(ins_arr)

        self.writer.writerows(rows_to_write)
        if os.path.getsize(self.csvfile.name)/1000/1000/1000 >= 1:
            self.csvfile.close()
            self.init_file()
            print('File count is: '+str(self.file_count))

if __name__ == '__main__':
    ins = HiveInserter('<HOSTNAME>', 10500, 'admin', 'admin', 'default', 'partsdata',
            ['time','id','shortname','notes','part_loc','vibr_tolr_pct','vibr_tolr_thrs','heat_tolr_pct','heat_tolr_thrs','qty'])
    gen = ManufacturingDataGenerator()
    rows = []
    for i in range(10):
        rows.append(gen.gen_row())
    ins.insert_rows(rows)
