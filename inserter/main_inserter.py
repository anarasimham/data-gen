from inserters import *
import sys
import time
import random
from multiprocessing import Process
from multiprocessing import Queue
from optparse import OptionParser
import os

from threading import Timer

sys.path.append('../datagen')
from datagen import DataGeneratorFactory
from datagen import rep_count

start = time.time()
checkpoints = [5,10,20,60,600,3600]

csv_out_dir = 'data_out/'

table_to_columns = {
    'customer':
        ['id','cust_contact_name','cust_ssn','cust_date_reg','cust_is_active','cust_address','cust_company_name'],
    'part_dashboard':
        ['time','id','shortname','notes','part_loc','vibr_tolr_pct','vibr_tolr_thrs','heat_tolr_pct','heat_tolr_thrs','qty'],
    'transactions':
        ['id','trxn_time','cust_id','trxn_amt','discount_amt','store_id','rep_id','part_sku','qty'],
    'transactions_customer':
        ['id','cust_contact_name', 'cust_ssn', 'cust_date_reg', 'cust_is_active', 'cust_address', 'cust_company_name','trxn_time','trxn_amt','discount_amt','store_id','rep_id','part_sku','qty'],
    'rep':
        ['id','name','address','salary','quota','start_date']
}

parser = OptionParser(usage='Usage: %prog [options] TABLE_NAME')
parser.add_option("-n", "--num-records", type="int", dest="num_records", help="write NUM_RECORDS to files", default=100)
parser.add_option("-d", "--data-destination", dest="data_destination", help="where to write the data to, options are: 'hive', 'mysql', 'csv'", default="csv")
parser.add_option("-j", "--jobs", type="int", dest="num_jobs", help="how many parallel processes to start (only applicable for CSV)", default="1")


(options, args) = parser.parse_args()

if len(args) != 1 :
    print("Incorrect number of arguments provided - please provide the table name in addition to any flags")
    sys.exit(1)
if (    (options.num_jobs > 1 and options.data_destination != 'csv') or
        (options.num_jobs > 1 and options.data_destination == 'csv' and args[0] == 'rep') ):
    options.num_jobs = 1

table_name = args[0]

if options.data_destination in ('hive','mysql'):
    filename = options.data_destination+'.passwd'
    f = open(filename)
    host,port,username,password,db = f.read().splitlines()

if table_name in table_to_columns:
    column_order = table_to_columns[table_name]
else:
    print("Invalid table name provided. Choose from one of: ["+', '.join(table_to_columns.keys())+']')
    sys.exit(1)

def parallelize(start_id, num_records, q):
    if options.data_destination == 'hive':
        ins = HiveInserter(host, port, username, password, db, table , column_order)
    elif options.data_destination == 'mysql':
        ins = MySQLInserter(host, port, username, password, db, table , column_order)
    elif options.data_destination == 'csv':
        if not os.path.exists(csv_out_dir):
            os.makedirs(csv_out_dir)
        ins = CSVInserter(csv_out_dir+table_name+'.csv', ',', column_order, start_id)

    start = time.time()
    start_rec_count = num_records
    def record_count():
        q.put(start_rec_count-num_records)
    for num in checkpoints:
        t = Timer(num, record_count)
        t.start()

    gen = DataGeneratorFactory.factory(table_name)

    rows = []
    while num_records > 0:
        rows.append(gen.gen_row())
        num_records -= 1

        if num_records % 100 == 0:
            ins.insert_rows(rows)
            rows = []

procs = []

per_process_num = options.num_records/options.num_jobs
if args[0] == 'rep':
    per_process_num = rep_count
records_per_proc = [per_process_num] * options.num_jobs
remainder = options.num_records%options.num_jobs

for i in range(len(records_per_proc)):
    if i < remainder:
        records_per_proc[i] += 1

curr_id = 1
q = Queue()
for i in range(options.num_jobs):
    num_records = records_per_proc.pop()
    print(curr_id)
    p = Process(target=parallelize, args=(curr_id, num_records, q))
    p.start()
    procs.append(p)
    curr_id += num_records

def printer(elapsed):
    sum_counts = 0
    try:
        while not q.empty():
            sum_counts = sum_counts + q.get()
    except Queue.Empty:
        pass
    print(str(sum_counts/elapsed) + " per second after " + str(elapsed) + " seconds")

for num in checkpoints:
    t = Timer(num+3, printer, [num])
    t.start()

for p in procs:
    p.join()

end = time.time()
print('Total watch time - '+str(end-start)+' seconds')
