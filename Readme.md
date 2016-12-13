# Data Ingest
The skills to transfer data between external systems and your cluster. This includes the following:
Basic useful feature list:
## Import data from a MySQL database into HDFS using Sqoop
### sqoop-import
#### Import table product from mysql clouderacert database
```sh
sqoop-import --connect jdbc:mysql://localhost:3306/clouderacert --username root --password cloudera -m 2 --table product
```
#### Import all tables from mysql clouderacert database
```sh
sqoop-import-all-tables --connect jdbc:mysql://localhost:3306/clouderacert --username root --password cloudera -m 2  
```
#### Import subset tables from mysql clouderacert database
```sh
sqoop-import-all-tables --connect jdbc:mysql://localhost:3306/clouderacert --username root --password cloudera -m 2 --exclude-tables product_trans
```
#### Import subset of table content from mysql clouderacert database
```sh
sqoop-import --connect jdbc:mysql://localhost:3306/clouderacert --username root --password cloudera --table product --where "trype='hospital'"
```
#### Import subset of table content from mysql clouderacert database to a directory
```sh 
sqoop-import --connect jdbc:mysql://localhost:3306/clouderacert --username root --password cloudera --query 'select * from product_trans where product_id=1 AND $CONDITIONS' --target-dir hdfs://quickstart.cloudera:8020/user/cloudera/product_trans -m 2 --split-by product_trans.trans_id
```
#### Incremental import
 * By column value
  ```sh
sqoop-import --connect jdbc:mysql://localhost:3306/clouderacert --username root --password cloudera --incremental append --table product_trans --check-column trans_id --last-value 4
```
 * By last modified date
 ```sh
sqoop-import --connect jdbc:mysql://localhost:3306/clouderacert --username root --password cloudera --incremental lastmodified --table product_trans --check-column <date_column> --last-value <timestamp>
Import job
```
## sqoop-export
### Export data to a MySQL product table in clouderacert database from HDFS using Sqoop
```sh
sqoop-export --connect jdbc:mysql://localhost/clouderacert --username root -P --table productc --export-dir ./product -m 10
```
## Change the delimiter and file format of data during import using Sqoop
```sh
sqoop-import --connect jdbc:mysql://localhost/clouderacert --username root -P --table product --fields-terminated-by '|' --lines-terminated-by '\t' --as-textfile -m 1
```
## Ingest real-time and near-real time (NRT) streaming data into HDFS using Flume
### Flume Components
Source -> Channel->Sink
### To fetch data from Sequence generator using a sequence generator source, a memory channel, and an HDFS sink.
* Configuration  in /usr/lib/flume-ng/conf/seq_gen.conf  
\# Naming the components on the current agent 
SeqGenAgent.sources = SeqSource   
SeqGenAgent.channels = MemChannel 
SeqGenAgent.sinks = HDFS  
\# Describing/Configuring the source 
SeqGenAgent.sources.SeqSource.type = syslogtcp
SeqGenAgent.sources.SeqSource.port = 44444  
  
\# Describing/Configuring the sink
SeqGenAgent.sinks.HDFS.type = hdfs 
SeqGenAgent.sinks.HDFS.hdfs.path = hdfs://localhost:8020/user/training/seq_data
SeqGenAgent.sinks.HDFS.hdfs.filePrefix = syslog 
SeqGenAgent.sinks.HDFS.hdfs.rollInterval = 0
SeqGenAgent.sinks.HDFS.hdfs.rollCount = 10000
SeqGenAgent.sinks.HDFS.hdfs.fileType = DataStream   
\# Describing/Configuring the channel 
SeqGenAgent.channels.MemChannel.type = memory 
SeqGenAgent.channels.MemChannel.capacity = 1000 
SeqGenAgent.channels.MemChannel.transactionCapacity = 100   
\# Binding the source and sink to the channel 
SeqGenAgent.sources.SeqSource.channels = MemChannel
SeqGenAgent.sinks.HDFS.channel = MemChannel 
* Script
```sh
sudo flume-ng agent --conf /usr/lib/flume-ng/conf/ -f /usr/lib/flume-ng/conf/seq_gen.conf  -n SeqGenAgent
```
# Load data into and out of HDFS using the Hadoop File System (FS) commands
### Transfer the file mark.csv in current directory to HDFS directory /user/training
```sh
hdfs dfs -put ./mark.csv /user/training
```
# Transform, Stage, Store
Convert a set of data values in a given format stored in HDFS into new data values and/or a new data format and write them into HDFS. This includes writing Spark applications in both Scala and Python (see note above on exam question format for more information on using either Scala or Python):
# Data Analysis
Use Data Definition Language (DDL) to create tables in the Hive metastore for use by Hive and Impala.