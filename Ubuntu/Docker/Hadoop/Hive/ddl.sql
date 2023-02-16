create database sales_products_raizen;

create external table sales_products_raizen.tbl_derivative
(year_month date,
uf string,
product string,
unit string,
volume double,
created_at timestamp)
stored as parquet
location "/user/cloudera/data/derivative";

create external table sales_products_raizen.tbl_diesel
(year_month date,
uf string,
product string,
unit string,
volume double,
created_at timestamp)
stored as parquet
location "/user/cloudera/data/diesel";


