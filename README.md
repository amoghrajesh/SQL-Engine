<h1>An SQL engine on top of Hadoop</h1>

<h2>There are 3 files</h2>

1. selectreducer.py : this does the reducing job.
2. selectmapper.py : this does the mappping job.
3. implementation.py : this file contains the implemenations of SQL queries and loading function which calls the mapper and reducer.

<h2>The SQL Queries Currently Supported :</h2>

1. select
2. project
3. load
4. min
5. max
6. sum
7. where 

<h2>What are requirements ?</h2>

hadoop
python3

<h2>How to run the interpreter ?</h2>

`python3 implementation.py`


<h2>Syntax </h2>

1. LOAD

load bigdata/csv_file_name.csv AS (column_name1:data_type,column_name2:data_type,...)

2. SELECT

select column_name1 from table_name where condition


**Note : data_type can be int,float,str** 


<h2>What's is done ?</h2>

1. Implement select and project
2. Implement aggregate functions MAX, COUNT, SUM
3. Loading of the csv file onto the hadoop.


