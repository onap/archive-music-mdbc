package org.onap.music.mdbc;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TupleValue;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.MusicTxDigestId;
import org.onap.music.mdbc.tables.PartitionInformation;
import org.onap.music.mdbc.tables.StagingTable;

import java.io.IOException;
import java.util.*;

import static com.datastax.driver.core.utils.UUIDs.random;

public class DatabaseOperations {
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(DatabaseOperations.class);
    /**
     * This functions is used to generate cassandra uuid
     * @return a random UUID that can be used for fields of type uuid
     */
    public static UUID generateUniqueKey() {
		return random();
	}

    /**
     * This function creates the MusicTxDigest table. It contain information related to each transaction committed
     * 	* LeaseId: id associated with the lease, text
     * 	* LeaseCounter: transaction number under this lease, bigint \TODO this may need to be a varint later
     *  * TransactionDigest: text that contains all the changes in the transaction
     */
    public static void CreateMusicTxDigest(int musicTxDigestTableNumber, String musicNamespace, String musicTxDigestTableName) throws MDBCServiceException {
        String tableName = musicTxDigestTableName;
        if(musicTxDigestTableNumber >= 0) {
            StringBuilder table = new StringBuilder();
            table.append(tableName);
            table.append("-");
            table.append(Integer.toString(musicTxDigestTableNumber));
            tableName=table.toString();
        }
        String priKey = "txid";
        StringBuilder fields = new StringBuilder();
        fields.append("txid uuid, ");
        fields.append("transactiondigest text ");//notice lack of ','
        String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s));", musicNamespace, tableName, fields, priKey);
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
    public static void CreateMusicRangeInformationTable(String musicNamespace, String musicRangeInformationTableName) throws MDBCServiceException {
        String tableName = musicRangeInformationTableName;
        String priKey = "rangeid";
        StringBuilder fields = new StringBuilder();
        fields.append("rangeid uuid, ");
        fields.append("keys set<text>, ");
        fields.append("ownerid text, ");
        fields.append("metricprocessid text, ");
        //TODO: Frozen is only needed for old versions of cassandra, please update correspondingly
        fields.append("txredolog list<frozen<tuple<text,uuid>>> ");
        String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s));", musicNamespace, tableName, fields, priKey);
        try {
            executeMusicWriteQuery(musicNamespace,tableName,cql);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to create transaction information table");
            throw(e);
        }
    }

    /**
     * Creates a new empty tit row
     * @param namespace namespace where the tit table is located
     * @param mriTableName name of the corresponding mri table where the new row is added
     * @param processId id of the process that is going to own initially this.
     * @return uuid associated to the new row
     */
    public static UUID CreateEmptyMriRow(String namespace, String mriTableName,
                                           String processId, String lockId, List<Range> ranges) throws MDBCServiceException {
        UUID id = generateUniqueKey();
        StringBuilder insert = new StringBuilder("INSERT INTO ")
                .append(namespace)
                .append('.')
                .append(mriTableName)
                .append(" (rangeid,keys,ownerid,metricprocessid,txredolog) VALUES ")
                .append("(")
                .append(id)
                .append(",{");
        for(Range r: ranges){
            insert.append(r.toString()).append(",");
        }
        insert.append("},")
                .append(lockId)
                .append(",")
                .append(processId)
                .append(",[]);");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(insert.toString());
        try {
            executeLockedPut(namespace,mriTableName,id.toString(),query,lockId,null);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to add new row to transaction information");
            throw new MDBCServiceException("Initialization error:Failure to add new row to transaction information");
        }
        return id;
    }

    public static MusicRangeInformationRow GetMriRow(String namespace, String mriTableName, UUID id, String lockId)
        throws MDBCServiceException{
		String cql = String.format("SELECT * FROM %s.%s WHERE rangeid = ?;", namespace, mriTableName);
		PreparedQueryObject pQueryObject = new PreparedQueryObject();
		pQueryObject.appendQueryString(cql);
		pQueryObject.addValue(id);
		Row newRow;
        try {
            newRow = executeLockedGet(namespace,mriTableName,pQueryObject,id.toString(),lockId);
        } catch (MDBCServiceException e) {
            logger.error("Get operationt error: Failure to get row from MRI "+mriTableName);
            throw new MDBCServiceException("Initialization error:Failure to add new row to transaction information");
        }
//        	public MusicRangeInformationRow(UUID index, List<MusicTxDigestId> redoLog, PartitionInformation partition,
 //                                   String ownerId, String metricProcessId) {
        List<TupleValue> log = newRow.getList("txredolog",TupleValue.class);
        List<MusicTxDigestId> digestIds = new ArrayList<>();
        for(TupleValue t: log){
           //final String tableName = t.getString(0);
           final UUID index = t.getUUID(1);
           digestIds.add(new MusicTxDigestId(index));
        }
        List<Range> partitions = new ArrayList<>();
        List<String> tables = newRow.getList("keys",String.class);
        for (String table:tables){
            partitions.add(new Range(table));
        }
        return new MusicRangeInformationRow(id,digestIds,new PartitionInformation(partitions),newRow.getString("ownerid"),newRow.getString("metricprocessid"));

    }

    public static HashMap<Range,StagingTable> getTransactionDigest(String namespace, String musicTxDigestTable, MusicTxDigestId id)
            throws MDBCServiceException{
		String cql = String.format("SELECT * FROM %s.%s WHERE rangeid = ?;", namespace, musicTxDigestTable);
		PreparedQueryObject pQueryObject = new PreparedQueryObject();
		pQueryObject.appendQueryString(cql);
		pQueryObject.addValue(id.tablePrimaryKey);
		Row newRow;
        try {
            newRow = executeUnlockedQuorumGet(pQueryObject);
        } catch (MDBCServiceException e) {
            logger.error("Get operation error: Failure to get row from txdigesttable with id:"+id.tablePrimaryKey);
            throw new MDBCServiceException("Initialization error:Failure to add new row to transaction information");
        }
        String digest = newRow.getString("transactiondigest");
        HashMap<Range,StagingTable> changes;
        try {
            changes = (HashMap<Range, StagingTable>) MDBCUtils.fromString(digest);
        } catch (IOException e) {
            logger.error("IOException when deserializing digest failed with an invalid class for id:"+id.tablePrimaryKey);
            throw new MDBCServiceException("Deserializng digest failed with ioexception");
        } catch (ClassNotFoundException e) {
            logger.error("Deserializng digest failed with an invalid class for id:"+id.tablePrimaryKey);
            throw new MDBCServiceException("Deserializng digest failed with an invalid class");
        }
        return changes;
    }

    /**
     * This method executes a write query in Music
     * @param cql the CQL to be sent to Cassandra
     */
    protected static void executeMusicWriteQuery(String keyspace, String table, String cql)
    		throws MDBCServiceException {
        PreparedQueryObject pQueryObject = new PreparedQueryObject();
        pQueryObject.appendQueryString(cql);
        ResultType rt = null;
        try {
            rt = MusicCore.createTable(keyspace,table,pQueryObject,"critical");
        } catch (MusicServiceException e) {
            //\TODO: handle better, at least transform into an MDBCServiceException
            e.printStackTrace();
        }
        if (rt.getResult().toLowerCase().equals("failure")) {
            throw new MDBCServiceException("Music eventual put failed");
        }
    }

    protected static Row executeLockedGet(String keyspace, String table, PreparedQueryObject cqlObject, String primaryKey,
                                           String lock)
        throws MDBCServiceException{
        ResultSet result;
        try {
            result = MusicCore.criticalGet(keyspace,table,primaryKey,cqlObject,lock);
        } catch(MusicServiceException e){
            //\TODO: handle better, at least transform into an MDBCServiceException
            e.printStackTrace();
            throw new MDBCServiceException("Error executing critical get");
        }
        if(result.isExhausted()){
            throw new MDBCServiceException("There is not a row that matches the id "+primaryKey);
        }
        return result.one();
    }

    protected static Row executeUnlockedQuorumGet(PreparedQueryObject cqlObject)
        throws MDBCServiceException{
        ResultSet result = MusicCore.quorumGet(cqlObject);
            //\TODO: handle better, at least transform into an MDBCServiceException
        if(result.isExhausted()){
            throw new MDBCServiceException("There is not a row that matches the query: ["+cqlObject.getQuery()+"]");
        }
        return result.one();
    }

    protected static void executeLockedPut(String namespace, String tableName,
                                           String primaryKeyWithoutDomain, PreparedQueryObject queryObject, String lockId,
                                           MusicCore.Condition conditionInfo) throws MDBCServiceException {
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




}
