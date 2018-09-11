# Data Generator

This can be used to generate two different types of data
- Manufacturing data
- POS transaction data

To get started:
- Configure a MySQL database and allow remote connections
- Create the desired type of table using one of the scripts in the `mysql/` folder using the user and database you plan on connecting to
- Copy the `inserter/mysql.passwd.template` into a `inserter/mysql.passwd` file and edit to provide the correct information
- Test it out by executing `python main_manf.py 10 mysql` from the `inserter` folder, where 10 is the desired number of records to be inserted into the MySQL table and 'mysql' is one of 'mysql' or 'hive' for which datasource to insert into
- `main_pos.py` will work as well, generating different data

You can speed up/down the data generation by changing the `time.sleep` value in the `main_*.py` files

Leave this running by executing:
```
nohup python main_manf.py -1 mysql &
```

This will generate data at a steady rate until killed.

Notes:
- On the machine where you install the data-gen repository and want to insert data to MySQL/Hive, you'll need to install the respective Pip packages
  - Install Pip first
  - Pyhs2 - must run sudo yum install gcc-c++ python-devel.x86_64 cyrus-sasl-devel.x86_64 cyrus-sasl-gssapi cyrus-sasl-md5 cyrus-sasl-plain as a dependency, then you can run pip install pyhs2
  - MySQL - must run pip install mysql-connector==2.1.6
