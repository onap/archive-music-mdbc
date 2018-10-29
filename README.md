METRIC
====

To enable edge computing in its full capacity, a crucial requirement is to manage the state of edge applications, preferably in database that provides the full features of SQL including joins and transactions. The key challenge here is to provide a replicated database for the edge that can scale to thousands of geo-distributed nodes. Existing solutions either provide semantics that are too weak (PostgreSQL replicates asynchronously) or too strong and hence expensive to realize in a geo-distributed network with its WAN latencies and complex failure modes (MariaDb, Spanner, provide full transactionality). Inspired by entry consistency in shared memory systems, wherein only the lock holder for an object obtains sequential consistency for the object, we define the notion of an entry transactional database, which is a novel partitioned database in which only the “owner” of a partition obtains full ACID transactionality. In this work, we define the semantics of an entry transactional database, describe the hard challenges faced in building it and present a novel middleware called mdbc that combines existing SQL databases with an underlying  geo-distributed entry consistent store to provide entry transactional semantics. Further, we present crucial use cases such as a federated regional controller for the network control plane and a state management service for edge mobility enabled by entry transactionality. 

## Dependencies

### Music installation
Given that METRIC is still a project in development, we are using newer versions (aka not release) of MUSIC, such that once 
it reaches production it matches the latest MUSIC version. So for now we cannot use maven repositories to pull MUSIC. Follow 
the next steps to compile and add the right version of music locally:
```bash
cd /tmp/
git clone https://gerrit.onap.org/r/music
cd music
git checkout  dev-cassandra-only
mvn install -Dfile=target/MUSIC.jar -DpomFile=./pom.xml -DskipTests
```
### Music properties
We need to create a property for the music system, as follows
```bash
# Create directory where the configuration file needs to be
sudo mkdir -p /opt/app/music/etc/ 
# Modify the configuration file located in src/main/resources/music.properties
# Add the local dev cassandra cluster and zookeeper information to this file
vim src/main/resources/music.properties
# copy the configuration file to the new location
sudo cp src/main/resources/music.properties /opt/app/music/etc/ 
```

## Running METRIC

run cassandra (tested with v3.11)
run mysql/mariadb (5.7+, 10.2.3+)

Download and install MUSIC (branch dev-cassandra-only). Install the jar into your local maven repository (from MUSIC home run)
mvn install:install-file -Dfile=target/MUSIC.jar -DpomFile=./pom.xml


1) Create a configuration file using as a template:
src/main/java/org/onap/music/mdbc/configurations/tableConfiguration.json

The meaning of the fields is as follows: 

•	partitions: is an array of each partition in the system. There is a partition for each ETDB (EDM). Each partition is composed of the following fields: 
o	tables: all the tables that are going to be within this table. Should at least have one element
o	owner: is the url of the ETDB (EDM) node. It can be an empty string
o	titTableName: it is the name of the transaction information table that the owner of this partition is going to be using
o	rrtTableName: it is the name of the redo records table that the owner of this partition is going to be using 
o	partitionId: if this partition was previously createad, this is the uuid associated, if new just leave it empty
o	replicationFactor: indicates the needs of replication for this partition (the max of all the tables involved). Note: this is not used yet in the code.
•	musicNamespace: is the music (cassandra) namespace that is going to be used by all the tables
•	tableToPartitionName:  it is the name of the table to partition table that the all nodes in the system are going to use
•	partitionInformationTableName: it is the name of the partition information table that the all nodes in the system are going to use
•	redoHistoryTableName:  it is the name of the redo history able that the all nodes in the system are going to use
•	sqlDatabaseName: is the name of the local SQL database that is going to be used on each ETDB node.

2) Create the configuration for each node using the command line program in the following location:

src/main/java/org/onap/music/mdbc/tools/CreateNodeConfiguration.java

To run it, use the following parameters:

-t ../ETDB/src/main/java/org/onap/music/mdbc/configurations/tableConfiguration.json -b base -o /Users/quique/Desktop/

This program is going to generate all the required configuration json for each ETDB node in the system and additionally initialize all the corresponding rows and tables for the system to correctly work. The meaning of the parameters is:
•	-t: the tableConfiguration.json explained in the step 1
•	-b: is the basename that is going to prepend to all the output files
•	-d: output directory where all the configuration files are going to be saved (It has to exist already)

Some notes about the limitations of this command line program:
•	It cannot handle multiple nodes handling the same lock. For example when creating a new row (or modifying one) in the table to partition table, the program is just going to crash
•	The creation of tables doesn’t include replication yet.
•	It doesn’t create the directory for the output configurations.

3) Run each of the server in its corresponding node: The ETDB server can be found in the file:
 
src/main/java/org/onap/music/mdbc/MdbcServer.java
 
It requires three parameters:

 -c ../ETDB/src/main/java/org/onap/music/mdbc/configurations/config-0.json -u jdbc:mysql://localhost -p 30000

 -c is a json with the configuration created in step 2. 
•	-u is where the local mysql database is located (without the database name, just the url, see example)
•	-p is the port that server is going to be used

4) Run the clients. A client example can be found in this folder:
 
src/main/java/org/onap/music/mdbc/examples

## Building METRIC

METRIC is built with Maven.  This directory contains two pom.xml files.
The first (*pom.xml*) will build a jar file to be used by applications wishing to use the
MDBC JDBC driver.
The second (*pom-h2server.xml*) is used to built the special code that needs to be loaded
into an H2 server, when running METRIC against a copy of H2 running as a server.

### Building the JBoss METRIC Module

There is a shell script (located in `src/main/shell/mk_jboss_module`) which, when run in
the mdbc source directory, will create a tar file `target/mdbc-jboss-module.tar` which can
be used as a JBoss module.  This tar file should be installed by un-taring it in the
$JBOSS_DIR/modules directory on the JBoss server.

## Using METRIC

This package provides a JDBC driver that can be used to mirror the contents of a database
to and from Cassandra. The mirroring occurs as a side effect of execute() statements against
a JDBC connection, and triggers placed in the database to catch database modifications.
The initial implementation is written to support H2, MySQL, and MariaDB databases.

This JDBC driver will intercept all table creations, SELECTs, INSERTs, DELETEs, and UPDATEs
made to the underlying database, and make sure they are copied to Cassandra.
In addition, for every table XX that is created, another table DIRTY\_XX will be created to
communicate the existence of dirty rows to other Cassandra replicas (with the Cassandra2
Mixin, the table is called DIRTY\_\_\_\_ and there is only one table).
Dirty rows will be copied, as needed back into the database from Cassandra before any SELECT.



1. Add this jar, and all dependent jars to your CLASSPATH.
2. Rewrite your JDBC URLs from jdbc:_yourdb_:... to jdbc:mdbc:....
3. If you supply properties to the DriverManager.getConnection(String, Properties) call,
 use the properties defined below to control behavior of the proxy.
4. Load the driver using the following call:
        Class.forName("org.onap.music.mdbc.ProxyDriver");

The following properties can be passed to the JDBC DriverManager.getConnection(String, Properties)
call to influence how METRIC works.

| Property Name	     | Property Value	                                                              | Default Value |
|--------------------|--------------------------------------------------------------------------------|---------------|
| MDBC\_DB\_MIXIN	 | The mixin name to use to select the database mixin to use for this connection. | mysql         |
| MDBC\_MUSIC\_MIXIN | The mixin name to use to select the MUSIC mixin to use for this connection.    | cassandra2    |
| myid	             | The ID of this replica in the collection of replicas sharing the same tables.  | 0             |
| replicas           | A comma-separated list of replica names for the collection of replicas sharing the same tables. | the value of myid |
| music\_keyspace    | The keyspace name to use in Cassandra for all tables created by this instance of METRIC. | mdbc  |
| music\_address     | The IP address to use to connect to Cassandra.	                              | localhost     |
| music\_rfactor     | The replication factor to use for the new keyspace that is created.	          | 2            	 |
| disabled	         | If set to true the mirroring is completely disabled; this is the equivalent of using the database driver directly. | false |

The values of the mixin properties may be:

| Property Name	     | Property Value | Purpose |
|--------------------|----------------|---------------|
| MDBC\_DB\_MIXIN	 | h2             | This mixin provides access to either an in-memory, or a local (file-based) version of the H2 database. |
| MDBC\_DB\_MIXIN	 | h2server       | This mixin provides access to a copy of the H2 database running as a server. Because the server needs special Java classes in order to handle certain TRIGGER actions, the server must be et up in a special way (see below). |
| MDBC\_DB\_MIXIN	 | mysql          | This mixin provides access to MySQL or MariaDB (10.2+) running on a remote server. |
| MDBC\_MUSIC\_MIXIN | cassandra      | A Cassandra based persistence layer (without any of the table locking that MUSIC normally provides). |
| MDBC\_MUSIC\_MIXIN | cassandra2     | Similar to the _cassandra_ mixin, but stores all dirty row information in one table, rather than one table per real table. |

### To Define a JBoss DataSource

The following code snippet can be used as a guide when setting up a JBoss DataSource.
This snippet goes in the JBoss *service.xml* file. The connection-property tags may
need to be added/modified for your purposes.  See the table above for names and values for
these tags.

```
<datasources>
  <datasource jta="true" jndi-name="java:jboss/datasources/ProcessEngine" pool-name="ProcessEngine" enabled="true" use-java-context="true" use-ccm="true">
    <connection-url>jdbc:mdbc:/opt/jboss-eap-6.2.4/standalone/camunda-h2-dbs/process-engine;DB_CLOSE_DELAY=-1;MVCC=TRUE;DB_CLOSE_ON_EXIT=FALSE</connection-url>
    <connection-property name="music_keyspace">
      camunda
    </connection-property>
    <driver>mdbc</driver>
    <security>
      <user-name>sa</user-name>
      <password>sa</password>
    </security>
  </datasource>
  <drivers>
    <driver name="mdbc" module="org.onap.music.mdbc">
      <driver-class>org.onap.music.mdbc.ProxyDriver</driver-class>
    </driver>
  </drivers>
</datasources>
```

Note: This assumes that you have built and installed the org.onap.music.mdbc module within JBoss.

### To Define a Tomcat DataSource Resource

The following code snippet can be used as a guide when setting up a Tomcat DataSource resource.
This snippet goes in the Tomcat *server.xml* file.  As with the JBoss DataSource, you will
probably need to make changes to the _connectionProperties_ attribute.

```
<Resource name="jdbc/ProcessEngine"
    auth="Container"
    type="javax.sql.DataSource"
    factory="org.apache.tomcat.jdbc.pool.DataSourceFactory"
    uniqueResourceName="process-engine"
    driverClassName="org.onap.music.mdbc.ProxyDriver"
    url="jdbc:mdbc:./camunda-h2-dbs/process-engine;MVCC=TRUE;TRACE_LEVEL_FILE=0;DB_CLOSE_ON_EXIT=FALSE"
    connectionProperties="myid=0;replicas=0,1,2;music_keyspace=camunda;music_address=localhost"
    username="sa"
    password="sa"
    maxActive="20"
    minIdle="5" />
```

## Databases Supported

Currently, the following databases are supported with METRIC:

* H2: The `H2Mixin` mixin is used when H2 is used with an in-memory (`jdbc:h2:mem:...`)
or local file based (`jdbc:h2:path_to_file`) database.

* H2 (Server): The `H2ServerMixin` mixin is used when H2 is used with an H2 server (`jdbc:h2:tcp:...`).

* MySQL: The `MySQLMixin` mixin is used for MySQL.

* MariaDB: The `MySQLMixin` mixin is used for MariaDB, which is functionally identical to MySQL.

## Testing Mixin Combinations

The files under `src/main/java/org/onap/music/mdbc/tests` can be used to test various METRIC
operations with various combinations of Mixins.  The tests are controlled via the file
`src/main/resources/tests.json`.  More details are available in the javadoc for this package.

## Limitations of METRIC

* The `java.sql.Statement.executeBatch()` method is not supported by METRIC.
It is not prohibited either; if you use this, your results will be unpredictable (and probably wrong).

* When used with a DB server, there is some delay as dirty row information is copied
from a table in the database, to the dirty table in Cassandra.  This opens a window
during which all sorts of mischief may occur.

* METRIC *only* copies the results of SELECTs, INSERTs, DELETEs, and UPDATEs.  Other database
operations must be performed outside of the purview of METRIC.  In particular, CREATE-ing or
DROP-ing tables or databases must be done individually on each database instance.

* Some of the table definitions may need adjusting depending upon the variables of your use
of METRIC.  For example, the MySQL mixin assumes (in its definition of the METRIC_TRANSLOG table)
that all table names will be no more than 255 bytes, and that tables rows (expressed in JSON)
will be no longer than 512 bytes. If this is not true, you should adjust, edit, and recompile.

* METRIC is limited to only data types that can be easily translated to a Cassandra equivalent;
e.g. BIGINT, BOOLEAN, BLOB, DOUBLE, INT, TIMESTAMP, VARCHAR

* To find the data types that your database is currently using run the following command:
SELECT DISTINCT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='<your db name here>';
