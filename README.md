YCSB-mysql-cluster-binding
====================

MySQL database interface for YCSB. Developed for benchmarking a MySQL cluster running in master-slave replication. Read operations are directed to the slave nodes while write operations are executed on the master node. 

Installation guide
==================

* Download the YCSB project as follows: git clone https://github.com/brianfrankcooper/YCSB.git
* Include the MySQL cluster DB binding within the YCSB directory: git clone https://github.com/arnaudsjs/YCSB-mysql-cluster-binding.git mysql
* Add <module>mysql</module> to the list of modules in YCSB/pom.xml
* Add the following lines to the DATABASE section in YCSB/bin/ycsb: "mysql" : "jdbcBinding.JdbcDBClient"
* Compile everything by executing the following command within the YCSB directory: mvn clean package
