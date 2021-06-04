# Map-Reduce

The goal of this project was to compare the performance of traditional SQL vs MapReduce. 

For running Hadoop MapReduce, I installed a Cloudera VM in the Oracle Virtual Box virtualization software. This virtual machine is recommended to have at least 10gb of RAM assigned to it and 4 CPU cores. The benefit of installing this virtual machine is that it provides the HDFS instances as well as all the services in the Hadoop framework, which can be installed separately. The components include the HDFS file system, Hive, Impala, MapReduce, PigLatin. 

The city database was the input data which has columns like ID, Name, CountryCode, District, Population. This database consists of 4000 records and is stored as a CSV input in the HDFS folder. The program takes SQL queries as input and generates output using MapReduce Framework that is implemented in Java. The following type of queries were tested:

1. Select * from table
2. Select column_names from table
3. Select column_names from a table where \[conditions]
4. Select column_names from a table where \[conditions] group by field
5. Select count(\*) from table where \[conditions] 
6. Select count(\*) from a table where \[conditions] group by field
7. Select field, MAX(column_name) from table where \[conditions] group by field
8. Select field, MIN(column_name) from table where \[conditions] group by field
9. Select field, SUM(column_name) from table where \[conditions] group by field

The output of these queries is generated in the text file on HDFS. The motive behind this project was to compare the performance of Relational Database Management System Hadoop and MapReduce for SQL queries as MapReduce works on a principle of distributed systems. We implemented SQL queries on both relational databases as well as Hadoop Map Reduce.

# Conclusion

Even though the Hadoop MapReduce works on distributed systems principle, when we tested the SQL Queries by running it on both RDBMS and MapReduce, we found out that the performance of the MapReduce is prolonged compared to the relational databases. Also, the indexing in RDBMS improves the performance of query processing. On further research, we found out that the performance of MapReduce is better when the volume of data is large i.e., in PetaBytes. Future Work can be done by running a project on high configuration servers and large datasets. More types of queries like sub-queries, joins, unions, and so on can be implemented using Hadoop MapReduce. 
