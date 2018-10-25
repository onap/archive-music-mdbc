package com.att.research.mdbc;

import com.att.research.exceptions.MDBCServiceException;
import com.att.research.logging.EELFLoggerDelegate;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;

import java.util.*;

public class DatabaseOperations {
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(DatabaseOperations.class);
    /**
     * This functions is used to generate cassandra uuid
     * @return a random UUID that can be used for fields of type uuid
     */
    public static String generateUniqueKey() {
		return UUID.randomUUID().toString();
	}

    /**
     * This functions returns the primary key used to managed a specific row in the TableToPartition tables in Music
     * @param namespace namespace where the TableToPartition resides
     * @param tableToPartitionTableName name of the tableToPartition table
     * @param tableName name of the application table that is being added to the system
     * @return primary key to be used with MUSIC
     */
    public static String getTableToPartitionPrimaryKey(String namespace, String tableToPartitionTableName, String tableName){
        return namespace+"."+tableToPartitionTableName+"."+tableName;
    }

    /**
     * Create a new row for a table, with not assigned partition
     * @param namespace namespace where the TableToPartition resides
     * @param tableToPartitionTableName name of the tableToPartition table
     * @param tableName name of the application table that is being added to the system
     * @param lockId if the lock for this key is already hold, this is the id of that lock.
     *         May be <code>null</code> if lock is not hold for the corresponding key
     */
    public static void createNewTableToPartitionRow(String namespace, String tableToPartitionTableName,
    		String tableName,String lockId) throws MDBCServiceException {
        final String primaryKey = getTableToPartitionPrimaryKey(namespace,tableToPartitionTableName,tableName);
        StringBuilder insert = new StringBuilder("INSERT INTO ")
                .append(namespace)
                .append('.')
                .append(tableToPartitionTableName)
                .append(" (tablename) VALUES ")
                .append("('")
                .append(tableName)
                .append("');");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(insert.toString());
        try {
            executedLockedPut(namespace,tableToPartitionTableName,tableName,query,lockId,null);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to create new row table to partition table ");
            throw new MDBCServiceException("Initialization error: Failure to create new row table to partition table");
        }
    }

    /**
     * Update the partition to which a table belongs
     * @param namespace namespace where the TableToPartition resides
     * @param tableToPartitionTableName name of the tableToPartition table
     * @param table name of the application table that is being added to the system
     * @param newPartition partition to which the application table is assigned
     * @param lockId if the lock for this key is already hold, this is the id of that lock.
     *                May be <code>null</code> if lock is not hold for the corresponding key
     */
    public static void updateTableToPartition(String namespace, String tableToPartitionTableName,
    		String table, String newPartition, String lockId) throws MDBCServiceException {
        final String primaryKey = getTableToPartitionPrimaryKey(namespace,tableToPartitionTableName,table);
        PreparedQueryObject query = new PreparedQueryObject();
        StringBuilder update = new StringBuilder("UPDATE ")
                .append(namespace)
                .append('.')
                .append(tableToPartitionTableName)
                .append(" SET previouspartitions = previouspartitions + {")
                .append(newPartition)
                .append("}, partition = " )
                .append(newPartition)
                .append(" WHERE tablename = '")
                .append(table)
                .append("';");
        query.appendQueryString(update.toString());
        try {
            executedLockedPut(namespace,tableToPartitionTableName,table,query,lockId,null);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to update a row in table to partition table ");
            throw new MDBCServiceException("Initialization error: Failure to update a row in table to partition table");
        }
    }


    public static String getPartitionInformationPrimaryKey(String namespace, String partitionInformationTable, String partition){
        return namespace+"."+partitionInformationTable+"."+partition;
    }

    /**
     * Create a new row, when a new partition is initialized
     * @param namespace namespace to which the partition info table resides in Cassandra
     * @param partitionInfoTableName name  of the partition information table
     * @param replicationFactor associated replicated factor for the partition (max of all the tables)
     * @param tables list of tables that are within this partitoin
     * @param lockId if the lock for this key is already hold, this is the id of that lock.
     *            May be <code>null</code> if lock is not hold for the corresponding key
     * @return the partition uuid associated to the new row
     */
    public static String createPartitionInfoRow(String namespace, String partitionInfoTableName,
    		int replicationFactor, List<String> tables, String lockId) throws MDBCServiceException {
        String id = generateUniqueKey();
        final String primaryKey = getPartitionInformationPrimaryKey(namespace,partitionInfoTableName,id);
        StringBuilder insert = new StringBuilder("INSERT INTO ")
                .append(namespace)
                .append('.')
                .append(partitionInfoTableName)
                .append(" (partition,replicationfactor,tables) VALUES ")
                .append("(")
                .append(id)
                .append(",")
                .append(replicationFactor)
                .append(",{");
        boolean first = true;
        for(String table: tables){
            if(!first){
                insert.append(",");
            }
            first = false;
            insert.append("'")
                    .append(table)
                    .append("'");
        }
        insert.append("});");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(insert.toString());
        try {
            executedLockedPut(namespace,partitionInfoTableName,id,query,lockId,null);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to create new row in partition information table ");
            throw new MDBCServiceException("Initialization error: Failure to create new row in partition information table");
        }
        return id;
    }

    /**
     * Update the TIT row and table that currently handles the partition
     * @param namespace namespace to which the partition info table resides in Cassandra
     * @param partitionInfoTableName name  of the partition information table
     * @param partitionId row identifier for the partition being modiefd
     * @param newTitRow new TIT row and table that are handling this partition
     * @param owner owner that is handling the new tit row (url to the corresponding etdb nodej
     * @param lockId if the lock for this key is already hold, this is the id of that lock.
     *           May be <code>null</code> if lock is not hold for the corresponding key
     */
    public static void updateRedoRow(String namespace, String partitionInfoTableName,
    		String partitionId, RedoRow newTitRow, String owner, String lockId) throws MDBCServiceException {
        final String primaryKey = getTableToPartitionPrimaryKey(namespace,partitionInfoTableName,partitionId);
        PreparedQueryObject query = new PreparedQueryObject();
        String newOwner = (owner==null)?"":owner;
        StringBuilder update = new StringBuilder("UPDATE ")
                .append(namespace)
                .append('.')
                .append(partitionInfoTableName)
                .append(" SET currentowner='")
                .append(newOwner)
                .append("', latesttitindex=")
                .append(newTitRow.getRedoRowIndex())
                .append(", latesttittable='")
                .append(newTitRow.getRedoTableName())
                .append("' WHERE partition = ")
                .append(partitionId)
                .append(";");
        query.appendQueryString(update.toString());
        try {
            executedLockedPut(namespace,partitionInfoTableName,partitionId,query,lockId,null);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to add new owner to partition in music table ");
            throw new MDBCServiceException("Initialization error:Failure to add new owner to partition in music table  ");
        }
    }

    /**
     * Create the first row in the history of the redo history table for a given partition
     * @param namespace namespace to which the redo history table resides in Cassandra
     * @param redoHistoryTableName name of the table where the row is being created
     * @param firstTitRow first tit  associated to the partition
     * @param partitionId partition for which a history is created
     */
	public static void createRedoHistoryBeginRow(String namespace, String redoHistoryTableName,
			RedoRow firstTitRow, String partitionId, String lockId) throws MDBCServiceException {
	    createRedoHistoryRow(namespace,redoHistoryTableName,firstTitRow,partitionId, new ArrayList<>(),lockId);
    }

    /**
     * Create a new row on the history for a given partition
     * @param namespace namespace to which the redo history table resides in Cassandra
     * @param redoHistoryTableName name of the table where the row is being created
     * @param currentRow new tit row associated to the partition
     * @param partitionId partition for which a history is created
     * @param parentsRows parent tit rows associated to this partition
     */
	public static void createRedoHistoryRow(String namespace, String redoHistoryTableName, RedoRow currentRow,
			String partitionId, List<RedoRow> parentsRows, String lockId) throws MDBCServiceException {
	    final String primaryKey = partitionId+"-"+currentRow.getRedoTableName()+"-"+currentRow.getRedoRowIndex();
        StringBuilder insert = new StringBuilder("INSERT INTO ")
                .append(namespace)
                .append('.')
                .append(redoHistoryTableName)
                .append(" (partition,redotable,redoindex,previousredo) VALUES ")
                .append("(")
                .append(partitionId)
                .append(",'")
                .append(currentRow.getRedoTableName())
                .append("',")
                .append(currentRow.getRedoRowIndex())
                .append(",{");
        boolean first = true;
        for(RedoRow parent: parentsRows){
            if(!first){
                insert.append(",");
            }
            else{
                first = false;
            }
            insert.append("('")
                    .append(parent.getRedoTableName())
                    .append("',")
                    .append(parent.getRedoRowIndex())
                    .append("),");
        }
        insert.append("});");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(insert.toString());
        try {
            executedLockedPut(namespace,redoHistoryTableName,primaryKey,query,lockId,null);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to add new row to redo history");
            throw new MDBCServiceException("Initialization error:Failure to add new row to redo history");
        }
    }

    /**
     * Creates a new empty tit row
     * @param namespace namespace where the tit table is located
     * @param titTableName name of the corresponding tit table where the new row is added
     * @param partitionId partition to which the redo log is hold
     * @return uuid associated to the new row
     */
    public static String CreateEmptyTitRow(String namespace, String titTableName, String partitionId, String lockId)
    		throws MDBCServiceException {
        String id = generateUniqueKey();
        StringBuilder insert = new StringBuilder("INSERT INTO ")
                .append(namespace)
                .append('.')
                .append(titTableName)
                .append(" (id,applied,latestapplied,partition,redo) VALUES ")
                .append("(")
                .append(id)
                .append(",false,-1,")
                .append(partitionId)
                .append(",[]);");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(insert.toString());
        try {
            executedLockedPut(namespace,titTableName,id,query,lockId,null);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to add new row to transaction information");
            throw new MDBCServiceException("Initialization error:Failure to add new row to transaction information");
        }
        return id;
    }





	/**
	 * This function creates the Table To Partition table. It contain information related to
	 */
	public static void CreateTableToPartitionTable(String musicNamespace, String tableToPartitionTableName)
			throws MDBCServiceException {
		String tableName = tableToPartitionTableName;
		String priKey = "tablename";
		StringBuilder fields = new StringBuilder();
		fields.append("tablename text, ");
		fields.append("partition uuid, ");
		fields.append("previouspartitions set<uuid> ");
		String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s));",
										musicNamespace, tableName, fields, priKey);
        try {
            executeMusicWriteQuery(musicNamespace,tableName,cql);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to create table to partition table");
            throw(e);
        }
    }

	public static void CreatePartitionInfoTable(String musicNamespace, String partitionInformationTableName)
			throws MDBCServiceException {
		String tableName = partitionInformationTableName;
		String priKey = "partition";
		StringBuilder fields = new StringBuilder();
		fields.append("partition uuid, ");
		fields.append("latesttittable text, ");
		fields.append("latesttitindex uuid, ");
		fields.append("tables set<text>, ");
		fields.append("replicationfactor int, ");
		fields.append("currentowner text");
		String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s));",
										musicNamespace, tableName, fields, priKey);
        try {
            executeMusicWriteQuery(musicNamespace,tableName,cql);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to create partition information table");
            throw(e);
        }
    }

	public static void CreateRedoHistoryTable(String musicNamespace, String redoHistoryTableName) throws MDBCServiceException {
		String tableName = redoHistoryTableName;
		String priKey = "partition,redotable,redoindex";
		StringBuilder fields = new StringBuilder();
		fields.append("partition uuid, ");
		fields.append("redotable text, ");
		fields.append("redoindex uuid, ");
        //TODO: Frozen is only needed for old versions of cassandra, please update correspondingly
		fields.append("previousredo set<frozen<tuple<text,uuid>>>");
		String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s));",
										musicNamespace, tableName, fields, priKey);
        try {
            executeMusicWriteQuery(musicNamespace,tableName,cql);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to create redo history table");
            throw(e);
        }
    }

    /**
     * This method executes a write query in Music
     * @param cql the CQL to be sent to Cassandra
     */
    protected static void executeMusicWriteQuery(String keyspace, String table, String cql) throws MDBCServiceException {
        PreparedQueryObject pQueryObject = new PreparedQueryObject();
        pQueryObject.appendQueryString(cql);
        ResultType rt = null;
        try {
            rt = MusicCore.createTable(keyspace,table,pQueryObject,"critical");
        } catch (MusicServiceException e) {
            e.printStackTrace();
        }
        if (rt.getResult().toLowerCase().equals("failure")) {
            throw new MDBCServiceException("Music eventual put failed");
        }
    }

    protected static void executedLockedPut(String namespace, String tableName, String primaryKeyWithoutDomain,
    		PreparedQueryObject queryObject, String lockId, MusicCore.Condition conditionInfo)
    				throws MDBCServiceException {
        ReturnType rt ;
        if(lockId==null) {
            try {
                rt = MusicCore.atomicPut(namespace, tableName, primaryKeyWithoutDomain, queryObject, conditionInfo);
            } catch (MusicLockingException e) {
                logger.error("Music locked put failed");
                throw new MDBCServiceException("Music locked put failed");
            } catch (MusicServiceException e) {
                logger.error("Music service fail: Music locked put failed");
                throw new MDBCServiceException("Music service fail: Music locked put failed");
            } catch (MusicQueryException e) {
                logger.error("Music query fail: locked put failed");
                throw new MDBCServiceException("Music query fail: Music locked put failed");
            }
        }
        else {
            rt = MusicCore.criticalPut(namespace, tableName, primaryKeyWithoutDomain, queryObject, lockId, conditionInfo);
        }
        if (rt.getResult().getResult().toLowerCase().equals("failure")) {
            throw new MDBCServiceException("Music locked put failed");
        }
    }

    public static void createNamespace(String namespace, int replicationFactor) throws MDBCServiceException {
        Map<String,Object> replicationInfo = new HashMap<String, Object>();
        replicationInfo.put("'class'", "'SimpleStrategy'");
        replicationInfo.put("'replication_factor'", replicationFactor);

        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(
                "CREATE KEYSPACE " + namespace + " WITH REPLICATION = " + replicationInfo.toString().replaceAll("=", ":"));

        try {
            MusicCore.nonKeyRelatedPut(queryObject, "critical");
        } catch (MusicServiceException e) {
            if (e.getMessage().equals("Keyspace "+namespace+" already exists")) {
                // ignore
            } else {
                logger.error("Error creating namespace: "+namespace);
                throw new MDBCServiceException("Error creating namespace: "+namespace+". Internal error:"+e.getErrorMessage());
            }
        }
    }

    /**
     * This function creates the MusicTxDigest table. It contain information related to each transaction committed
     * 	* LeaseId: id associated with the lease, text
     * 	* LeaseCounter: transaction number under this lease, bigint \TODO this may need to be a varint later
     *  * TransactionDigest: text that contains all the changes in the transaction
     */
    public static void CreateMusicTxDigest(int musicTxDigestTableNumber, String musicNamespace,
    		String musicTxDigestTableName) throws MDBCServiceException {
        String tableName = musicTxDigestTableName;
        if(musicTxDigestTableNumber >= 0) {
            StringBuilder table = new StringBuilder();
            table.append(tableName);
            table.append("-");
            table.append(Integer.toString(musicTxDigestTableNumber));
            tableName=table.toString();
        }
        String priKey = "leaseid,leasecounter";
        StringBuilder fields = new StringBuilder();
        fields.append("leaseid text, ");
        fields.append("leasecounter varint, ");
        fields.append("transactiondigest text ");//notice lack of ','
        String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s));",
        								musicNamespace, tableName, fields, priKey);
        try {
            executeMusicWriteQuery(musicNamespace,tableName,cql);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to create redo records table");
            throw(e);
        }
    }

    /**
     * This function creates the TransactionInformation table. It contain information related
     * to the transactions happening in a given partition.
     * 	 * The schema of the table is
     * 		* Id, uiid.
     * 		* Partition, uuid id of the partition
     * 		* LatestApplied, int indicates which values from the redologtable wast the last to be applied to the data tables
     *		* Applied: boolean, indicates if all the values in this redo log table where already applied to data tables
     *		* Redo: list of uiids associated to the Redo Records Table
     *
     */
    public static void CreateMusicRangeInformationTable(String musicNamespace, String musicRangeInformationTableName)
    		throws MDBCServiceException {
        String tableName = musicRangeInformationTableName;
        String priKey = "id";
        StringBuilder fields = new StringBuilder();
        fields.append("id uuid, ");
        fields.append("partition uuid, ");
        fields.append("latestapplied int, ");
        fields.append("applied boolean, ");
        //TODO: Frozen is only needed for old versions of cassandra, please update correspondingly
        fields.append("redo list<frozen<tuple<text,tuple<text,varint>>>> ");
        String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s));",
        							musicNamespace, tableName, fields, priKey);
        try {
            executeMusicWriteQuery(musicNamespace,tableName,cql);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to create transaction information table");
            throw(e);
        }
    }


}
