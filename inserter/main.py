from insert_data import MySQLInserter
import sys
import time
import random

sys.path.append('../datagen')
from datagen import POSDataGenerator

num_rows = int(sys.argv[1])

f = open('mysql.passwd')

host,port,username,password,db,table = f.read().splitlines()

ins = MySQLInserter(host, port, username, password, db, table , 
        ['id','trxn_time','cust_id','trxn_amt','discount_amt','store_id','rep_id','part_sku','qty'])
gen = POSDataGenerator()

rows = []
while num_rows > 0:
    rows.append(gen.gen_row())
    num_rows -= 1
    time.sleep(random.random()/10)
    print(num_rows)

    if num_rows % 100 == 0:
        ins.insert_rows(rows)
        rows = []
