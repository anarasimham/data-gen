from insert_data import MySQLInserter
import sys
import time
import random

sys.path.append('../datagen')
from datagen import ManufacturingDataGenerator

num_rows = int(sys.argv[1])

f = open('mysql.passwd')

host,port,username,password,db,table = f.read().splitlines()

ins = MySQLInserter(host, port, username, password, db, table , 
        ['time','id','shortname','notes','part_loc','vibr_tolr_pct','vibr_tolr_thrs','heat_tolr_pct','heat_tolr_thrs','qty'])
gen = ManufacturingDataGenerator()

rows = []
while num_rows > 0:
    rows.append(gen.gen_row())
    num_rows -= 1
    time.sleep(random.random()/10)
    print(num_rows)

    if num_rows % 100 == 0:
        ins.insert_rows(rows)
        rows = []
