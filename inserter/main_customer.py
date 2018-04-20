from insert_data import MySQLInserter
from insert_data import HiveInserter
import sys
import time
import random

sys.path.append('../datagen')
from datagen import CustomerDataGenerator

if len(sys.argv) < 3:
    print('Please provide 2 arguments, first is number of records, second is hive or mysql for db to insert into')
    sys.exit(1)

num_rows = int(sys.argv[1])
hive_or_mysql = sys.argv[2]

if hive_or_mysql == 'hive':
    filename = 'hive.passwd'
else:
    filename = 'mysql.passwd'

f = open(filename)
host,port,username,password,db,table = f.read().splitlines()

if hive_or_mysql == 'hive':
    ins = HiveInserter(host, port, username, password, db, table ,
            ['id','contact_name','ssn','date_reg','is_active','address','company_name'])
else:
    ins = MySQLInserter(host, port, username, password, db, table ,
            ['id','contact_name','ssn','date_reg','is_active','address','company_name'])

gen = CustomerDataGenerator()

rows = []
while num_rows != 0:
    rows.append(gen.gen_row())
    num_rows -= 1
    time.sleep(random.random())
    print(num_rows)

    if num_rows % 10 == 0:
        ins.insert_rows(rows)
        rows = []
