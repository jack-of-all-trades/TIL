#!/bin/bash
# MH
# Beeline/hive example
# modify before run
#    hive2 server, port, database...
#    hadoop path.
banner()
{
echo "#########################"
echo "#   $1 ..."
echo "#########################"
}
create_table()
{
banner " create table"
perform drop_table.sql
perform create_table.sql
}
insert_table()
{
banner " insert table"
perform insert_table.sql
}
query_table()
{
banner " query table"
perform query_table.sql
}

update_table()
{
banner " update table"
perform update_table.sql
}

create_index()
{
banner " create index"
perform cr_index.sql
}
drop_table()
{
banner " drop table "
perform drop_table.sql
}

perform()
{
beeline -u "jdbc:hive2://${HIVE2SERVER}:${HIVE2PORT}/${DATABASE};principal=hive/${HIVE2SERVER}@${REALM}" -f $1
}

load_partition()
{
gen_sample
perform  load_partition.sql
}
gen_sample()
{
cat >  employee_2015_1.txt << EOF
1      	'michael'      	10     	'xyz'          	2015
2      	'michael'      	20     	'xyz'          	2015
3      	'michael'      	30     	'xyz'          	2015
4      	'michael'      	40     	'xyz'          	2015
EOF
ls employee_2015_1.txt
hadoop fs -put employee_2015_1.txt employee_2015.txt
hadoop fs -ls employee_2015.txt
hadoop fs -ls /user/mhuang/employee_2015.txt
}

gen_sql()
{
cat >  lock_table.sql << EOF
set hive.txn.manager;
set hive.compactor.initiator.on;
set hive.compactor.worker.threads;
set hive.support.concurrency;
show locks  employee;
lock table employee exclusive;
show locks employee;
lock table employee shared;
show locks employee;
EOF


cat > query_table.sql << EOF
select * from employee;
EOF
cat  > drop_table.sql << EOF
drop table employee;
EOF

cat >  cr_index.sql << EOF
set hive.execution.engine=mr;
CREATE INDEX employee_idx
ON TABLE employee (eid)
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
WITH DEFERRED REBUILD;

EOF


cat  > lock_table.sql << EOF
set hive.txn.manager;
set hive.compactor.initiator.on;
set hive.compactor.worker.threads;
set hive.support.concurrency;
show locks  employee;
lock table employee exclusive;
show locks employee;
lock table employee shared;
show locks employee;
EOF

cat  > insert_table.sql << EOF
set hive.execution.engine=mr;
insert into employee values (1, 'michael', 10, 'xyz',2016);
insert into employee values (2, 'michael', 20, 'xyz',2015);
insert into employee values (3, 'michael', 30, 'xyz',2016);
EOF

cat  > load_partition.sql << EOF
drop table employee;
CREATE TABLE IF NOT EXISTS employee ( eid int, name String, salary String, destination String)
PARTITIONED BY (year int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;



LOAD DATA  INPATH '/user/mhuang/employee_2015.txt'
      INTO TABLE employee
      PARTITION (year = 2015);


#ALTER TABLE employee ADD IF NOT EXISTS
#PARTITION (year = 2015) LOCATION '/apps/hive/warehouse/qatest.db/employee/2015/part2015'
#PARTITION (year = 2016) LOCATION '/apps/hive/warehouse/qatest.db/employee/2016/part2016'
#;
EOF


cat > create_table.sql << EOF
CREATE TABLE IF NOT EXISTS employee ( eid int, name String, salary String, destination String, year int)
STORED AS TEXTFILE;
EOF


cat > hadoop.env << EOF
HIVE2SERVER=
HIVE2PORT=
DATABASE=
REALM=
EOF

}
e2e()
{
gen_sql
create_table
query_table
insert_table
query_table
#update_table
create_index
drop_table
load_partition
query_table

}

source hadoop.env

$1
