from insert_data import MySQLInserter
from insert_data import HiveInserter
from insert_data import CSVInserter
import sys
import time
import random
from multiprocessing import Process

sys.path.append('../datagen')
from datagen import POSDataGenerator

start = time.time()

if len(sys.argv) < 3:
    print('Please provide 3 arguments, first is number of records, second is hive or mysql or csv for db to insert into, third is number of parallel splits')
    sys.exit(1)

num_rows = int(sys.argv[1])
data_dest = sys.argv[2]

if data_dest == 'hive':
    filename = 'hive.passwd'
elif data_dest == 'mysql':
    filename = 'mysql.passwd'

if data_dest == 'hive' or data_dest == 'mysql':
    f = open(filename)
    host,port,username,password,db,table = f.read().splitlines()

column_order = ['id','cust_contact_name', 'cust_ssn', 'cust_date_reg', 'cust_is_active', 'cust_address', 'cust_company_name','trxn_time','trxn_amt','discount_amt','store_id','rep_id','part_sku','qty']
if data_dest == 'hive':
    ins = HiveInserter(host, port, username, password, db, table , column_order)
elif data_dest == 'mysql':
    ins = MySQLInserter(host, port, username, password, db, table , column_order)
elif data_dest == 'csv':
    ins = CSVInserter('pos_cust_data.csv', ',', column_order)

gen = POSDataGenerator()

rows = []
while num_rows > 0:
    rows.append(gen.gen_row())
    num_rows -= 1
    #time.sleep(random.random()/10)

    if num_rows % 100 == 0:
        ins.insert_rows(rows)
        rows = []

end = time.time()
print('Total watch time - '+str(end-start)+' seconds')
