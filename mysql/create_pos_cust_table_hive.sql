create table transactions (
    id int,
    `cust_contact_name` varchar(50), 
    `cust_ssn` varchar(9), 
    `cust_date_reg` date, 
    `cust_is_active` int,
    cust_address varchar(100),
    cust_company_name varchar(100),
    trxn_time timestamp,
    trxn_amt double,
    discount_amt double,
    store_id int,
    rep_id int,
    part_sku varchar(30),
    qty int
);
