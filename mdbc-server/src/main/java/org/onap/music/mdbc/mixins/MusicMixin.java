/*
 * ============LICENSE_START====================================================
 * org.onap.music.mdbc
 * =============================================================================
 * Copyright (C) 2018 AT&T Intellectual Property. All rights reserved.
 * =============================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ============LICENSE_END======================================================
 */
package org.onap.music.mdbc.mixins;

import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.lang.NotImplementedException;
import org.onap.music.mdbc.*;
import org.onap.music.mdbc.tables.MusicTxDigestId;
import org.onap.music.mdbc.tables.StagingTable;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.TxCommitProgress;
import org.onap.music.service.impl.MusicCassaCore;
import org.json.JSONObject;
import org.onap.music.lockingservice.cassandra.CassaLockStore;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.datastore.Condition;
import org.onap.music.main.MusicCore;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;

import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TupleValue;

/**
 * This class provides the methods that MDBC needs to access Cassandra directly in order to provide persistence
 * to calls to the user's DB.  It does not do any table or row locking.
 *
 * <p>This code only supports the following limited list of H2 and Cassandra data types:</p>
 * <table summary="">
 * <tr><th>H2 Data Type</th><th>Mapped to Cassandra Data Type</th></tr>
 * <tr><td>BIGINT</td><td>BIGINT</td></tr>
 * <tr><td>BOOLEAN</td><td>BOOLEAN</td></tr>
 * <tr><td>CLOB</td><td>BLOB</td></tr>
 * <tr><td>DOUBLE</td><td>DOUBLE</td></tr>
 * <tr><td>INTEGER</td><td>INT</td></tr>
 *  <tr><td>TIMESTAMP</td><td>TIMESTAMP</td></tr>
 * <tr><td>VARBINARY</td><td>BLOB</td></tr>
 * <tr><td>VARCHAR</td><td>VARCHAR</td></tr>
 * </table>
 *
 * @author Robert P. Eby
 */
public class MusicMixin implements MusicInterface {
    /** The property name to use to identify this replica to MusicSqlManager */
    public static final String KEY_MY_ID              = "myid";
    /** The property name to use for the comma-separated list of replica IDs. */
    public static final String KEY_REPLICAS           = "replica_ids";
    /** The property name to use to identify the IP address for Cassandra. */
    public static final String KEY_MUSIC_ADDRESS      = "cassandra.host";
    /** The property name to use to provide the replication factor for Cassandra. */
    public static final String KEY_MUSIC_RFACTOR      = "music_rfactor";
    /** The property name to use to provide the replication factor for Cassandra. */
    public static final String KEY_MUSIC_NAMESPACE = "music_namespace";
    /** Namespace for the tables in MUSIC (Cassandra) */
    public static final String DEFAULT_MUSIC_NAMESPACE = "namespace";
    /** The default property value to use for the Cassandra IP address. */
    public static final String DEFAULT_MUSIC_ADDRESS  = "localhost";
    /** The default property value to use for the Cassandra replication factor. */
    public static final int    DEFAULT_MUSIC_RFACTOR  = 1;
    /** The default primary string column, if none is provided. */
    public static final String MDBC_PRIMARYKEY_NAME = "mdbc_cuid";
    /** Type of the primary key, if none is defined by the user */
    public static final String MDBC_PRIMARYKEY_TYPE = "uuid";


    //\TODO Add logic to change the names when required and create the tables when necessary
    private String musicTxDigestTableName = "musictxdigest";
    private String musicRangeInformationTableName = "musicrangeinformation";

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicMixin.class);

    private class LockResult{
        private final UUID musicRangeInformationIndex;
        private final String ownerId;
        private final List<Range> ranges;
        private final boolean newLock;
        public LockResult(UUID rowId, String ownerId, boolean newLock, List<Range> ranges){
            this.musicRangeInformationIndex = rowId;
            this.ownerId=ownerId;
            this.newLock=newLock;
            this.ranges=ranges;
        }
        public String getOwnerId(){
            return ownerId;
        }
        public boolean getNewLock(){
            return newLock;
        }
        public UUID getIndex() {return musicRangeInformationIndex;}
        public List<Range> getRanges() {return ranges;}
    }

    private class RangeMriRow{
        private MusicRangeInformationRow currentRow;
        private List<MusicRangeInformationRow> oldRows;
        private final Range range;
        public RangeMriRow(Range range) {
            this.range = range;
            oldRows = new ArrayList<>();
        }
        Range getRange(){
            return range;
        }
        public MusicRangeInformationRow getCurrentRow(){
            return currentRow;
        }
        public void setCurrentRow(MusicRangeInformationRow row){
            currentRow=row;
        }
        public void addOldRow(MusicRangeInformationRow row){
            oldRows.add(row);
        }
        public List<MusicRangeInformationRow> getOldRows(){
            return oldRows;
        }
    }

    private class LockRequest{
        private final String table;
        private final UUID id;
        private final List<Range> toLockRanges;
        public LockRequest(String table, UUID id, List<Range> toLockRanges){
            this.table=table;
            this.id=id;
            this.toLockRanges=toLockRanges;
        }
        public UUID getId() {
            return id;
        }
        public List<Range> getToLockRanges() {
            return toLockRanges;
        }

        public String getTable() {
            return table;
        }
    }


    private static final Map<Integer, String> typemap         = new HashMap<>();
    static {
        // We only support the following type mappings currently (from DB -> Cassandra).
        // Anything else will likely cause a NullPointerException
        typemap.put(Types.BIGINT,    "BIGINT");	// aka. IDENTITY
        typemap.put(Types.BLOB,      "VARCHAR");
        typemap.put(Types.BOOLEAN,   "BOOLEAN");
        typemap.put(Types.CLOB,      "BLOB");
        typemap.put(Types.DATE,      "VARCHAR");
        typemap.put(Types.DOUBLE,    "DOUBLE");
        typemap.put(Types.DECIMAL,   "DECIMAL");
        typemap.put(Types.INTEGER,   "INT");
        //typemap.put(Types.TIMESTAMP, "TIMESTAMP");
        typemap.put(Types.SMALLINT, "SMALLINT");
        typemap.put(Types.TIMESTAMP, "VARCHAR");
        typemap.put(Types.VARBINARY, "BLOB");
        typemap.put(Types.VARCHAR,   "VARCHAR");
        typemap.put(Types.CHAR,   	 "VARCHAR");
        //The "Hacks", these don't have a direct mapping
        //typemap.put(Types.DATE,   	 "VARCHAR");
        //typemap.put(Types.DATE,   	 "TIMESTAMP");
    }

    protected final String music_ns;
    protected final String myId;
    protected final String[] allReplicaIds;
    private final String musicAddress;
    private final int    music_rfactor;
    private MusicConnector mCon        = null;
    private Session musicSession       = null;
    private boolean keyspace_created   = false;
    private Map<String, PreparedStatement> ps_cache = new HashMap<>();
    private Set<String> in_progress    = Collections.synchronizedSet(new HashSet<String>());

    public MusicMixin() {
        //this.logger         = null;
        this.musicAddress   = null;
        this.music_ns       = null;
        this.music_rfactor  = 0;
        this.myId           = null;
        this.allReplicaIds  = null;
    }

    public MusicMixin(String mdbcServerName, Properties info) throws MDBCServiceException {
        // Default values -- should be overridden in the Properties
        // Default to using the host_ids of the various peers as the replica IDs (this is probably preferred)
        this.musicAddress   = info.getProperty(KEY_MUSIC_ADDRESS, DEFAULT_MUSIC_ADDRESS);
        logger.info(EELFLoggerDelegate.applicationLogger,"MusicSqlManager: musicAddress="+musicAddress);

        String s            = info.getProperty(KEY_MUSIC_RFACTOR);
        this.music_rfactor  = (s == null) ? DEFAULT_MUSIC_RFACTOR : Integer.parseInt(s);

        this.myId           = info.getProperty(KEY_MY_ID,    getMyHostId());
        logger.info(EELFLoggerDelegate.applicationLogger,"MusicSqlManager: myId="+myId);


        this.allReplicaIds  = info.getProperty(KEY_REPLICAS, getAllHostIds()).split(",");
        logger.info(EELFLoggerDelegate.applicationLogger,"MusicSqlManager: allReplicaIds="+info.getProperty(KEY_REPLICAS, this.myId));

        this.music_ns       = info.getProperty(KEY_MUSIC_NAMESPACE,DEFAULT_MUSIC_NAMESPACE);
        logger.info(EELFLoggerDelegate.applicationLogger,"MusicSqlManager: music_ns="+music_ns);

        initializeMetricTables();
    }

    public String getMusicTxDigestTableName(){
        return musicTxDigestTableName;
    }

    public String getMusicRangeInformationTableName(){
        return musicRangeInformationTableName;
    }

    /**
     * This method creates a keyspace in Music/Cassandra to store the data corresponding to the SQL tables.
     * The keyspace name comes from the initialization properties passed to the JDBC driver.
     */
    @Override
    public void createKeyspace() throws MDBCServiceException {

        Map<String,Object> replicationInfo = new HashMap<>();
        replicationInfo.put("'class'", "'SimpleStrategy'");
        replicationInfo.put("'replication_factor'", music_rfactor);

        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(
            "CREATE KEYSPACE IF NOT EXISTS " + this.music_ns +
                " WITH REPLICATION = " + replicationInfo.toString().replaceAll("=", ":"));

        try {
            MusicCore.nonKeyRelatedPut(queryObject, "eventual");
        } catch (MusicServiceException e) {
            if (!e.getMessage().equals("Keyspace "+music_ns+" already exists")) {
                throw new MDBCServiceException("Error creating namespace: "+music_ns+". Internal error:"+e.getErrorMessage(), e);
            }
        }
    }

    private String getMyHostId() {
        ResultSet rs = null;
        try {
            rs = executeMusicRead("SELECT HOST_ID FROM SYSTEM.LOCAL");
        } catch (MDBCServiceException e) {
            return "UNKNOWN";
        }
        Row row = rs.one();
        return (row == null) ? "UNKNOWN" : row.getUUID("HOST_ID").toString();
    }
    private String getAllHostIds() {
        ResultSet results = null;
        try {
            results = executeMusicRead("SELECT HOST_ID FROM SYSTEM.PEERS");
        } catch (MDBCServiceException e) {
        }
        StringBuilder sb = new StringBuilder(myId);
        if(results!=null) {
            for (Row row : results) {
                sb.append(",");
                sb.append(row.getUUID("HOST_ID").toString());
            }
        }
        return sb.toString();
    }

    /**
     * Get the name of this MusicInterface mixin object.
     * @return the name
     */
    @Override
    public String getMixinName() {
        return "cassandra";
    }
    /**
     * Do what is needed to close down the MUSIC connection.
     */
    @Override
    public void close() {
        if (musicSession != null) {
            musicSession.close();
            musicSession = null;
        }
    }

    /**
     * This function is used to created all the required data structures, both local
     */
    private void initializeMetricTables() throws MDBCServiceException {
        createKeyspace();
        try {
            createMusicTxDigest();//\TODO If we start partitioning the data base, we would need to use the redotable number
            createMusicRangeInformationTable();
        }
        catch(MDBCServiceException e){
            logger.error(EELFLoggerDelegate.errorLogger,"Error creating tables in MUSIC");
        }
    }

    /**
     * This method performs all necessary initialization in Music/Cassandra to store the table <i>tableName</i>.
     * @param tableName the table to initialize MUSIC for
     */
    @Override
    public void initializeMusicForTable(TableInfo ti, String tableName) {
        /**
         * This code creates two tables for every table in SQL:
         * (i) a table with the exact same name as the SQL table storing the SQL data.
         * (ii) a "dirty bits" table that stores the keys in the Cassandra table that are yet to be
         * updated in the SQL table (they were written by some other node).
         */
        StringBuilder fields = new StringBuilder();
        StringBuilder prikey = new StringBuilder();
        String pfx = "", pfx2 = "";
        for (int i = 0; i < ti.columns.size(); i++) {
            fields.append(pfx)
                .append(ti.columns.get(i))
                .append(" ")
                .append(typemap.get(ti.coltype.get(i)));
            if (ti.iskey.get(i)) {
                // Primary key column
                prikey.append(pfx2).append(ti.columns.get(i));
                pfx2 = ", ";
            }
            pfx = ", ";
        }
        if (prikey.length()==0) {
            fields.append(pfx).append(MDBC_PRIMARYKEY_NAME)
                .append(" ")
                .append(MDBC_PRIMARYKEY_TYPE);
            prikey.append(MDBC_PRIMARYKEY_NAME);
        }
        String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s));", music_ns, tableName, fields.toString(), prikey.toString());
        executeMusicWriteQuery(cql);
    }

    // **************************************************
    // Dirty Tables (in MUSIC) methods
    // **************************************************

    /**
     * Create a <i>dirty row</i> table for the real table <i>tableName</i>.  The primary keys columns from the real table are recreated in
     * the dirty table, along with a "REPLICA__" column that names the replica that should update it's internal state from MUSIC.
     * @param tableName the table to create a "dirty" table for
     */
    @Override
    public void createDirtyRowTable(TableInfo ti, String tableName) {
        // create dirtybitsTable at all replicas
//		for (String repl : allReplicaIds) {
////			String dirtyRowsTableName = "dirty_"+tableName+"_"+allReplicaIds[i];
////			String dirtyTableQuery = "CREATE TABLE IF NOT EXISTS "+music_ns+"."+ dirtyRowsTableName+" (dirtyRowKeys text PRIMARY KEY);";
//			cql = String.format("CREATE TABLE IF NOT EXISTS %s.DIRTY_%s_%s (dirtyRowKeys TEXT PRIMARY KEY);", music_ns, tableName, repl);
//			executeMusicWriteQuery(cql);
//		}
        StringBuilder ddl = new StringBuilder("REPLICA__ TEXT");
        StringBuilder cols = new StringBuilder("REPLICA__");
        for (int i = 0; i < ti.columns.size(); i++) {
            if (ti.iskey.get(i)) {
                // Only use the primary keys columns in the "Dirty" table
                ddl.append(", ")
                    .append(ti.columns.get(i))
                    .append(" ")
                    .append(typemap.get(ti.coltype.get(i)));
                cols.append(", ").append(ti.columns.get(i));
            }
        }
        if(cols.length()==0) {
            //fixme
            System.err.println("Create dirty row table found no primary key");
        }
        ddl.append(", PRIMARY KEY(").append(cols).append(")");
        String cql = String.format("CREATE TABLE IF NOT EXISTS %s.DIRTY_%s (%s);", music_ns, tableName, ddl.toString());
        executeMusicWriteQuery(cql);
    }
    /**
     * Drop the dirty row table for <i>tableName</i> from MUSIC.
     * @param tableName the table being dropped
     */
    @Override
    public void dropDirtyRowTable(String tableName) {
        String cql = String.format("DROP TABLE %s.DIRTY_%s;", music_ns, tableName);
        executeMusicWriteQuery(cql);
    }
    /**
     * Mark rows as "dirty" in the dirty rows table for <i>tableName</i>.  Rows are marked for all replicas but
     * this one (this replica already has the up to date data).
     * @param tableName the table we are marking dirty
     * @param keys an ordered list of the values being put into the table.  The values that correspond to the tables'
     * primary key are copied into the dirty row table.
     */
    @Override
    public void markDirtyRow(TableInfo ti, String tableName, JSONObject keys) {
        Object[] keyObj = getObjects(ti,tableName, keys);
        StringBuilder cols = new StringBuilder("REPLICA__");
        PreparedQueryObject pQueryObject = null;
        StringBuilder vals = new StringBuilder("?");
        List<Object> vallist = new ArrayList<Object>();
        vallist.add(""); // placeholder for replica
        for (int i = 0; i < ti.columns.size(); i++) {
            if (ti.iskey.get(i)) {
                cols.append(", ").append(ti.columns.get(i));
                vals.append(", ").append("?");
                vallist.add(keyObj[i]);
            }
        }
        if(cols.length()==0) {
            //FIXME
            System.err.println("markDIrtyRow need to fix primary key");
        }
        String cql = String.format("INSERT INTO %s.DIRTY_%s (%s) VALUES (%s);", music_ns, tableName, cols.toString(), vals.toString());
		/*Session sess = getMusicSession();
		PreparedStatement ps = getPreparedStatementFromCache(cql);*/
        String primaryKey;
        if(ti.hasKey()) {
            primaryKey = getMusicKeyFromRow(ti,tableName, keys);
        }
        else {
            primaryKey = getMusicKeyFromRowWithoutPrimaryIndexes(ti,tableName, keys);
        }
        System.out.println("markDirtyRow: PK value: "+primaryKey);

        Object pkObj = null;
        for (int i = 0; i < ti.columns.size(); i++) {
            if (ti.iskey.get(i)) {
                pkObj = keyObj[i];
            }
        }
        for (String repl : allReplicaIds) {
            pQueryObject = new PreparedQueryObject();
            pQueryObject.appendQueryString(cql);
            pQueryObject.addValue(tableName);
            pQueryObject.addValue(repl);
            pQueryObject.addValue(pkObj);
            updateMusicDB(tableName, primaryKey, pQueryObject);
            //if (!repl.equals(myId)) {
				/*logger.info(EELFLoggerDelegate.applicationLogger,"Executing MUSIC write:"+ cql);
				vallist.set(0, repl);
				BoundStatement bound = ps.bind(vallist.toArray());
				bound.setReadTimeoutMillis(60000);
				synchronized (sess) {
					sess.execute(bound);
				}*/
            //}

        }
    }
    /**
     * Remove the entries from the dirty row (for this replica) that correspond to a set of primary keys
     * @param tableName the table we are removing dirty entries from
     * @param keys the primary key values to use in the DELETE.  Note: this is *only* the primary keys, not a full table row.
     */
    @Override
    public void cleanDirtyRow(TableInfo ti, String tableName, JSONObject keys) {
        Object[] keysObjects = getObjects(ti,tableName,keys);
        PreparedQueryObject pQueryObject = new PreparedQueryObject();
        StringBuilder cols = new StringBuilder("REPLICA__=?");
        List<Object> vallist = new ArrayList<Object>();
        vallist.add(myId);
        int n = 0;
        for (int i = 0; i < ti.columns.size(); i++) {
            if (ti.iskey.get(i)) {
                cols.append(" AND ").append(ti.columns.get(i)).append("=?");
                vallist.add(keysObjects[n++]);
                pQueryObject.addValue(keysObjects[n++]);
            }
        }
        String cql = String.format("DELETE FROM %s.DIRTY_%s WHERE %s;", music_ns, tableName, cols.toString());
        logger.debug(EELFLoggerDelegate.applicationLogger,"Executing MUSIC write:"+ cql);
        pQueryObject.appendQueryString(cql);
        ReturnType rt = MusicCore.eventualPut(pQueryObject);
        if(rt.getResult().getResult().toLowerCase().equals("failure")) {
            System.out.println("Failure while cleanDirtyRow..."+rt.getMessage());
        }
		/*Session sess = getMusicSession();
		PreparedStatement ps = getPreparedStatementFromCache(cql);
		BoundStatement bound = ps.bind(vallist.toArray());
		bound.setReadTimeoutMillis(60000);
		synchronized (sess) {
			sess.execute(bound);
		}*/
    }
    /**
     * Get a list of "dirty rows" for a table.  The dirty rows returned apply only to this replica,
     * and consist of a Map of primary key column names and values.
     * @param tableName the table we are querying for
     * @return a list of maps; each list item is a map of the primary key names and values for that "dirty row".
     */
    @Override
    public List<Map<String,Object>> getDirtyRows(TableInfo ti, String tableName) {
        String cql = String.format("SELECT * FROM %s.DIRTY_%s WHERE REPLICA__=?;", music_ns, tableName);
        ResultSet results = null;
        logger.debug(EELFLoggerDelegate.applicationLogger,"Executing MUSIC write:"+ cql);
		
		/*Session sess = getMusicSession();
		PreparedStatement ps = getPreparedStatementFromCache(cql);
		BoundStatement bound = ps.bind(new Object[] { myId });
		bound.setReadTimeoutMillis(60000);
		synchronized (sess) {
			results = sess.execute(bound);
		}*/
        PreparedQueryObject pQueryObject = new PreparedQueryObject();
        pQueryObject.appendQueryString(cql);
        try {
            results = MusicCore.get(pQueryObject);
        } catch (MusicServiceException e) {

            e.printStackTrace();
        }

        ColumnDefinitions cdef = results.getColumnDefinitions();
        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        for (Row row : results) {
            Map<String,Object> objs = new HashMap<String,Object>();
            for (int i = 0; i < cdef.size(); i++) {
                String colname = cdef.getName(i).toUpperCase();
                String coltype = cdef.getType(i).getName().toString().toUpperCase();
                if (!colname.equals("REPLICA__")) {
                    switch (coltype) {
                        case "BIGINT":
                            objs.put(colname, row.getLong(colname));
                            break;
                        case "BOOLEAN":
                            objs.put(colname, row.getBool(colname));
                            break;
                        case "BLOB":
                            objs.put(colname, row.getString(colname));
                            break;
                        case "DATE":
                            objs.put(colname, row.getString(colname));
                            break;
                        case "DOUBLE":
                            objs.put(colname, row.getDouble(colname));
                            break;
                        case "DECIMAL":
                            objs.put(colname, row.getDecimal(colname));
                            break;
                        case "INT":
                            objs.put(colname, row.getInt(colname));
                            break;
                        case "TIMESTAMP":
                            objs.put(colname, row.getTimestamp(colname));
                            break;
                        case "VARCHAR":
                        default:
                            objs.put(colname, row.getString(colname));
                            break;
                    }
                }
            }
            list.add(objs);
        }
        return list;
    }

    /**
     * Drops the named table and its dirty row table (for all replicas) from MUSIC.  The dirty row table is dropped first.
     * @param tableName This is the table that has been dropped
     */
    @Override
    public void clearMusicForTable(String tableName) {
        dropDirtyRowTable(tableName);
        String cql = String.format("DROP TABLE %s.%s;", music_ns, tableName);
        executeMusicWriteQuery(cql);
    }
    /**
     * This function is called whenever there is a DELETE to a row on a local SQL table, wherein it updates the
     * MUSIC/Cassandra tables (both dirty bits and actual data) corresponding to the SQL write. MUSIC propagates
     * it to the other replicas.
     *
     * @param tableName This is the table that has changed.
     * @param oldRow This is a copy of the old row being deleted
     */
    @Override
    public void deleteFromEntityTableInMusic(TableInfo ti, String tableName, JSONObject oldRow) {
        Object[] objects = getObjects(ti,tableName,oldRow);
        PreparedQueryObject pQueryObject = new PreparedQueryObject();
        if (ti.hasKey()) {
            assert(ti.columns.size() == objects.length);
        } else {
            assert(ti.columns.size()+1 == objects.length);
        }

        StringBuilder where = new StringBuilder();
        List<Object> vallist = new ArrayList<Object>();
        String pfx = "";
        for (int i = 0; i < ti.columns.size(); i++) {
            if (ti.iskey.get(i)) {
                where.append(pfx)
                    .append(ti.columns.get(i))
                    .append("=?");
                vallist.add(objects[i]);
                pQueryObject.addValue(objects[i]);
                pfx = " AND ";
            }
        }
        if (!ti.hasKey()) {
            where.append(MDBC_PRIMARYKEY_NAME + "=?");
            //\FIXME this is wrong, old row is not going to contain the UUID, this needs to be fixed
            vallist.add(UUID.fromString((String) objects[0]));
            pQueryObject.addValue(UUID.fromString((String) objects[0]));
        }

        String cql = String.format("DELETE FROM %s.%s WHERE %s;", music_ns, tableName, where.toString());
        logger.error(EELFLoggerDelegate.errorLogger,"Executing MUSIC write:"+ cql);
        pQueryObject.appendQueryString(cql);
		
		/*PreparedStatement ps = getPreparedStatementFromCache(cql);
		BoundStatement bound = ps.bind(vallist.toArray());
		bound.setReadTimeoutMillis(60000);
		Session sess = getMusicSession();
		synchronized (sess) {
			sess.execute(bound);
		}*/
        String primaryKey = getMusicKeyFromRow(ti,tableName, oldRow);

        updateMusicDB(tableName, primaryKey, pQueryObject);

        // Mark the dirty rows in music for all the replicas but us
        markDirtyRow(ti,tableName, oldRow);
    }

    public Set<String> getMusicTableSet(String ns) {
        Set<String> set = new TreeSet<String>();
        String cql = String.format("SELECT TABLE_NAME FROM SYSTEM_SCHEMA.TABLES WHERE KEYSPACE_NAME = '%s'", ns);
        ResultSet rs = null;
        try {
            rs = executeMusicRead(cql);
        } catch (MDBCServiceException e) {
            e.printStackTrace();
        }
        if(rs!=null) {
            for (Row row : rs) {
                set.add(row.getString("TABLE_NAME").toUpperCase());
            }
        }
        return set;
    }
    /**
     * This method is called whenever there is a SELECT on a local SQL table, wherein it first checks the local
     * dirty bits table to see if there are any keys in Cassandra whose value has not yet been sent to SQL
     * @param tableName This is the table on which the select is being performed
     */
    @Override
    public void readDirtyRowsAndUpdateDb(DBInterface dbi, String tableName) {
        // Read dirty rows of this table from Music
        TableInfo ti = dbi.getTableInfo(tableName);
        List<Map<String,Object>> objlist = getDirtyRows(ti,tableName);
        PreparedQueryObject pQueryObject = null;
        String pre_cql = String.format("SELECT * FROM %s.%s WHERE ", music_ns, tableName);
        List<Object> vallist = new ArrayList<Object>();
        StringBuilder sb = new StringBuilder();
        //\TODO Perform a batch operation instead of each row at a time
        for (Map<String,Object> map : objlist) {
            pQueryObject = new PreparedQueryObject();
            sb.setLength(0);
            vallist.clear();
            String pfx = "";
            for (String key : map.keySet()) {
                sb.append(pfx).append(key).append("=?");
                vallist.add(map.get(key));
                pQueryObject.addValue(map.get(key));
                pfx = " AND ";
            }

            String cql = pre_cql + sb.toString();
            System.out.println("readDirtyRowsAndUpdateDb: cql: "+cql);
            pQueryObject.appendQueryString(cql);
            ResultSet dirtyRows = null;
            try {
                //\TODO Why is this an eventual put?, this should be an atomic
                dirtyRows = MusicCore.get(pQueryObject);
            } catch (MusicServiceException e) {

                e.printStackTrace();
            }
			/*
			Session sess = getMusicSession();
			PreparedStatement ps = getPreparedStatementFromCache(cql);
			BoundStatement bound = ps.bind(vallist.toArray());
			bound.setReadTimeoutMillis(60000);
			ResultSet dirtyRows = null;
			synchronized (sess) {
				dirtyRows = sess.execute(bound);
			}*/
            List<Row> rows = dirtyRows.all();
            if (rows.isEmpty()) {
                // No rows, the row must have been deleted
                deleteRowFromSqlDb(dbi,tableName, map);
            } else {
                for (Row row : rows) {
                    writeMusicRowToSQLDb(dbi,tableName, row);
                }
            }
        }
    }

    private void deleteRowFromSqlDb(DBInterface dbi, String tableName, Map<String, Object> map) {
        dbi.deleteRowFromSqlDb(tableName, map);
        TableInfo ti = dbi.getTableInfo(tableName);
        List<Object> vallist = new ArrayList<Object>();
        for (int i = 0; i < ti.columns.size(); i++) {
            if (ti.iskey.get(i)) {
                String col = ti.columns.get(i);
                Object val = map.get(col);
                vallist.add(val);
            }
        }
        cleanDirtyRow(ti, tableName, new JSONObject(vallist));
    }
    /**
     * This functions copies the contents of a row in Music into the corresponding row in the SQL table
     * @param tableName This is the name of the table in both Music and swl
     * @param musicRow This is the row in Music that is being copied into SQL
     */
    private void writeMusicRowToSQLDb(DBInterface dbi, String tableName, Row musicRow) {
        // First construct the map of columns and their values
        TableInfo ti = dbi.getTableInfo(tableName);
        Map<String, Object> map = new HashMap<String, Object>();
        List<Object> vallist = new ArrayList<Object>();
        String rowid = tableName;
        for (String col : ti.columns) {
            Object val = getValue(musicRow, col);
            map.put(col, val);
            if (ti.iskey(col)) {
                vallist.add(val);
                rowid += "_" + val.toString();
            }
        }

        logger.debug("Blocking rowid: "+rowid);
        in_progress.add(rowid);			// Block propagation of the following INSERT/UPDATE

        dbi.insertRowIntoSqlDb(tableName, map);

        logger.debug("Unblocking rowid: "+rowid);
        in_progress.remove(rowid);		// Unblock propagation

//		try {
//			String sql = String.format("INSERT INTO %s (%s) VALUES (%s);", tableName, fields.toString(), values.toString());
//			executeSQLWrite(sql);
//		} catch (SQLException e) {
//			logger.debug("Insert failed because row exists, do an update");
//			// TODO - rewrite this UPDATE command should not update key fields
//			String sql = String.format("UPDATE %s SET (%s) = (%s) WHERE %s", tableName, fields.toString(), values.toString(), where.toString());
//			try {
//				executeSQLWrite(sql);
//			} catch (SQLException e1) {
//				e1.printStackTrace();
//			}
//		}

        ti = dbi.getTableInfo(tableName);
        cleanDirtyRow(ti, tableName, new JSONObject(vallist));

//		String selectQuery = "select "+ primaryKeyName+" FROM "+tableName+" WHERE "+primaryKeyName+"="+primaryKeyValue+";";
//		java.sql.ResultSet rs = executeSQLRead(selectQuery);
//		String dbWriteQuery=null;
//		try {
//			if(rs.next()){//this entry is there, do an update
//				dbWriteQuery = "UPDATE "+tableName+" SET "+columnNameString+" = "+ valueString +"WHERE "+primaryKeyName+"="+primaryKeyValue+";";
//			}else
//				dbWriteQuery = "INSERT INTO "+tableName+" VALUES"+valueString+";";
//			executeSQLWrite(dbWriteQuery);
//		} catch (SQLException e) {
//			// ZZTODO Auto-generated catch block
//			e.printStackTrace();
//		}

        //clean the music dirty bits table
//		String dirtyRowIdsTableName = music_ns+".DIRTY_"+tableName+"_"+myId;
//		String deleteQuery = "DELETE FROM "+dirtyRowIdsTableName+" WHERE dirtyRowKeys=$$"+primaryKeyValue+"$$;";
//		executeMusicWriteQuery(deleteQuery);
    }
    private Object getValue(Row musicRow, String colname) {
        ColumnDefinitions cdef = musicRow.getColumnDefinitions();
        DataType colType;
        try {
            colType= cdef.getType(colname);
        }
        catch(IllegalArgumentException e) {
            logger.warn("Colname is not part of table metadata: "+e);
            throw e;
        }
        String typeStr = colType.getName().toString().toUpperCase();
        switch (typeStr) {
            case "BIGINT":
                return musicRow.getLong(colname);
            case "BOOLEAN":
                return musicRow.getBool(colname);
            case "BLOB":
                return musicRow.getString(colname);
            case "DATE":
                return musicRow.getString(colname);
            case "DECIMAL":
                return musicRow.getDecimal(colname);
            case "DOUBLE":
                return musicRow.getDouble(colname);
            case "SMALLINT":
            case "INT":
                return musicRow.getInt(colname);
            case "TIMESTAMP":
                return musicRow.getTimestamp(colname);
            case "UUID":
                return musicRow.getUUID(colname);
            default:
                logger.error(EELFLoggerDelegate.errorLogger, "UNEXPECTED COLUMN TYPE: columname="+colname+", columntype="+typeStr);
                // fall thru
            case "VARCHAR":
                return musicRow.getString(colname);
        }
    }

    /**
     * This method is called whenever there is an INSERT or UPDATE to a local SQL table, wherein it updates the
     * MUSIC/Cassandra tables (both dirty bits and actual data) corresponding to the SQL write. Music propagates
     * it to the other replicas.
     *
     * @param tableName This is the table that has changed.
     * @param changedRow This is information about the row that has changed
     */
    @Override
    public void updateDirtyRowAndEntityTableInMusic(TableInfo ti, String tableName, JSONObject changedRow) {
        // Build the CQL command
        Object[] objects = getObjects(ti,tableName,changedRow);
        StringBuilder fields = new StringBuilder();
        StringBuilder values = new StringBuilder();
        String rowid = tableName;
        Object[] newrow = new Object[objects.length];
        PreparedQueryObject pQueryObject = new PreparedQueryObject();
        String pfx = "";
        int keyoffset=0;
        for (int i = 0; i < objects.length; i++) {
            if (!ti.hasKey() && i==0) {
                //We need to tack on cassandra's uid in place of a primary key
                fields.append(MDBC_PRIMARYKEY_NAME);
                values.append("?");
                newrow[i] = UUID.fromString((String) objects[i]);
                pQueryObject.addValue(newrow[i]);
                keyoffset=-1;
                pfx = ", ";
                continue;
            }
            fields.append(pfx).append(ti.columns.get(i+keyoffset));
            values.append(pfx).append("?");
            pfx = ", ";
            if (objects[i] instanceof byte[]) {
                // Cassandra doesn't seem to have a Codec to translate a byte[] to a ByteBuffer
                newrow[i] = ByteBuffer.wrap((byte[]) objects[i]);
                pQueryObject.addValue(newrow[i]);
            } else if (objects[i] instanceof Reader) {
                // Cassandra doesn't seem to have a Codec to translate a Reader to a ByteBuffer either...
                newrow[i] = ByteBuffer.wrap(readBytesFromReader((Reader) objects[i]));
                pQueryObject.addValue(newrow[i]);
            } else {
                newrow[i] = objects[i];
                pQueryObject.addValue(newrow[i]);
            }
            if (i+keyoffset>=0 && ti.iskey.get(i+keyoffset)) {
                rowid += "_" + newrow[i].toString();
            }
        }

        if (in_progress.contains(rowid)) {
            // This call to updateDirtyRowAndEntityTableInMusic() was called as a result of a Cassandra -> H2 update; ignore
            logger.debug(EELFLoggerDelegate.applicationLogger, "updateDirtyRowAndEntityTableInMusic: bypassing MUSIC update on "+rowid);

        } else {
            // Update local MUSIC node. Note: in Cassandra you can insert again on an existing key..it becomes an update
            String cql = String.format("INSERT INTO %s.%s (%s) VALUES (%s);", music_ns, tableName, fields.toString(), values.toString());

            pQueryObject.appendQueryString(cql);
            String primaryKey = getMusicKeyFromRow(ti,tableName, changedRow);
            updateMusicDB(tableName, primaryKey, pQueryObject);
			
			/*PreparedStatement ps = getPreparedStatementFromCache(cql);
			BoundStatement bound = ps.bind(newrow);
			bound.setReadTimeoutMillis(60000);
			Session sess = getMusicSession();
			synchronized (sess) {
				sess.execute(bound);
			}*/
            // Mark the dirty rows in music for all the replicas but us
            markDirtyRow(ti,tableName, changedRow);
        }
    }



    private byte[] readBytesFromReader(Reader rdr) {
        StringBuilder sb = new StringBuilder();
        try {
            int ch;
            while ((ch = rdr.read()) >= 0) {
                sb.append((char)ch);
            }
        } catch (IOException e) {
            logger.warn("readBytesFromReader: "+e);
        }
        return sb.toString().getBytes();
    }

    protected PreparedStatement getPreparedStatementFromCache(String cql) {
        // Note: have to hope that the Session never changes!
        if (!ps_cache.containsKey(cql)) {
            Session sess = getMusicSession();
            PreparedStatement ps = sess.prepare(cql);
            ps_cache.put(cql, ps);
        }
        return ps_cache.get(cql);
    }

    /**
     * This method gets a connection to Music
     * @return the Cassandra Session to use
     */
    protected Session getMusicSession() {
        // create cassandra session
        if (musicSession == null) {
            logger.info(EELFLoggerDelegate.applicationLogger, "Creating New Music Session");
            mCon = new MusicConnector(musicAddress);
            musicSession = mCon.getSession();
        }
        return musicSession;
    }

    /**
     * This method executes a write query in Music
     * @param cql the CQL to be sent to Cassandra
     */
    protected void executeMusicWriteQuery(String cql) {
        logger.debug(EELFLoggerDelegate.applicationLogger, "Executing MUSIC write:"+ cql);
        PreparedQueryObject pQueryObject = new PreparedQueryObject();
        pQueryObject.appendQueryString(cql);
        ReturnType rt = MusicCore.eventualPut(pQueryObject);
        if(rt.getResult().getResult().toLowerCase().equals("failure")) {
            logger.error(EELFLoggerDelegate.errorLogger, "Failure while eventualPut...: "+rt.getMessage());
        }
		/*Session sess = getMusicSession();
		SimpleStatement s = new SimpleStatement(cql);
		s.setReadTimeoutMillis(60000);
		synchronized (sess) {
			sess.execute(s);
		}*/
    }

    /**
     * This method executes a read query in Music
     * @param cql the CQL to be sent to Cassandra
     * @return a ResultSet containing the rows returned from the query
     */
    protected ResultSet executeMusicRead(String cql) throws MDBCServiceException {
        logger.debug(EELFLoggerDelegate.applicationLogger, "Executing MUSIC write:"+ cql);
        PreparedQueryObject pQueryObject = new PreparedQueryObject();
        pQueryObject.appendQueryString(cql);
        ResultSet results = null;
        try {
            results = MusicCore.get(pQueryObject);
        } catch (MusicServiceException e) {
            logger.error("Error executing music get operation for query: ["+cql+"]");
            throw new MDBCServiceException("Error executing get: "+e.getMessage(), e);
        }
        return results;
    }

    /**
     * Returns the default primary key name that this mixin uses
     */
    public String getMusicDefaultPrimaryKeyName() {
        return MDBC_PRIMARYKEY_NAME;
    }

    /**
     * Return the function for cassandra's primary key generation
     */
    @Override
    public UUID generateUniqueKey() {
        return MDBCUtils.generateUniqueKey();
    }

    @Override
    public String getMusicKeyFromRowWithoutPrimaryIndexes(TableInfo ti, String table, JSONObject dbRow) {
        //\TODO this operation is super expensive to perform, both latency and BW
        // it is better to add additional where clauses, and have the primary key
        // to be composed of known columns of the table
        // Adding this primary indexes would be an additional burden to the developers, which spanner
        // also does, but otherwise performance is really bad
        // At least it should have a set of columns that are guaranteed to be unique
        StringBuilder cqlOperation = new StringBuilder();
        cqlOperation.append("SELECT * FROM ")
            .append(music_ns)
            .append(".")
            .append(table);
        ResultSet musicResults = null;
        try {
            musicResults = executeMusicRead(cqlOperation.toString());
        } catch (MDBCServiceException e) {
            return null;
        }
        Object[] dbRowObjects = getObjects(ti,table,dbRow);
        while (!musicResults.isExhausted()) {
            Row musicRow = musicResults.one();
            if (rowIs(ti, musicRow, dbRowObjects)) {
                return ((UUID)getValue(musicRow, MDBC_PRIMARYKEY_NAME)).toString();
            }
        }
        //should never reach here
        return null;
    }

    /**
     * Checks to see if this row is in list of database entries
     * @param ti
     * @param musicRow
     * @param dbRow
     * @return
     */
    private boolean rowIs(TableInfo ti, Row musicRow, Object[] dbRow) {
        //System.out.println("Comparing " + musicRow.toString());
        boolean sameRow=true;
        for (int i=0; i<ti.columns.size(); i++) {
            Object val = getValue(musicRow, ti.columns.get(i));
            if (!dbRow[i].equals(val)) {
                sameRow=false;
                break;
            }
        }
        return sameRow;
    }

    @Override
    public String getMusicKeyFromRow(TableInfo ti, String tableName, JSONObject row) {
        List<String> keyCols = ti.getKeyColumns();
        if(keyCols.isEmpty()){
            throw new IllegalArgumentException("Table doesn't have defined primary indexes ");
        }
        StringBuilder key = new StringBuilder();
        String pfx = "";
        for(String keyCol: keyCols) {
            key.append(pfx);
            key.append(row.get(keyCol));
            pfx = ",";
        }
        String keyStr = key.toString();
        return keyStr;
    }

    public void updateMusicDB(String tableName, String primaryKey, PreparedQueryObject pQObject) {
        ReturnType rt = MusicCore.eventualPut(pQObject);
        if(rt.getResult().getResult().toLowerCase().equals("failure")) {
            System.out.println("Failure while critical put..."+rt.getMessage());
        }
    }

    /**
     * Build a preparedQueryObject that appends a transaction to the mriTable
     * @param mriTable
     * @param uuid
     * @param table
     * @param redoUuid
     * @return
     */
    private PreparedQueryObject createAppendMtxdIndexToMriQuery(String mriTable, UUID uuid, String table, UUID redoUuid){
        PreparedQueryObject query = new PreparedQueryObject();
        StringBuilder appendBuilder = new StringBuilder();
        appendBuilder.append("UPDATE ")
            .append(music_ns)
            .append(".")
            .append(mriTable)
            .append(" SET txredolog = txredolog +[('")
            .append(table)
            .append("',")
            .append(redoUuid)
            .append(")] WHERE rangeid = ")
            .append(uuid)
            .append(";");
        query.appendQueryString(appendBuilder.toString());
        return query;
    }

    protected ReturnType acquireLock(String fullyQualifiedKey, String lockId) throws MDBCServiceException{
        ReturnType lockReturn;
        //\TODO Handle better failures to acquire locks
        try {
            lockReturn = MusicCore.acquireLock(fullyQualifiedKey,lockId);
        } catch (MusicLockingException e) {
            logger.error(EELFLoggerDelegate.errorLogger, "Lock was not acquire correctly for key "+fullyQualifiedKey);
            throw new MDBCServiceException("Lock was not acquire correctly for key "+fullyQualifiedKey, e);
        } catch (MusicServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger, "Error in music, when locking key: "+fullyQualifiedKey);
            throw new MDBCServiceException("Error in music, when locking: "+fullyQualifiedKey, e);
        } catch (MusicQueryException e) {
            logger.error(EELFLoggerDelegate.errorLogger, "Error in executing query music, when locking key: "+fullyQualifiedKey);
            throw new MDBCServiceException("Error in executing query music, when locking: "+fullyQualifiedKey, e);
        }
        return lockReturn;
    }

    protected Map<UUID, List<Range>> getPendingRows(Map<Range, RangeMriRow> rangeRows) throws MDBCServiceException{
        throw new NotImplementedException("error");
    }

    protected List<LockResult> waitForLock(LockRequest request, DatabasePartition partition) throws MDBCServiceException {
        if(partition.getMusicRangeInformationIndex()!=request.getId()){
            throw new MDBCServiceException("Invalid argument for wait for lock, range id in request and partition should match");
        }
        List<LockResult> result = new ArrayList<>();
        String fullyQualifiedKey= music_ns+"."+ request.getTable()+"."+request.getId();
        String lockId = MusicCore.createLockReference(fullyQualifiedKey);
        ReturnType lockReturn = acquireLock(fullyQualifiedKey,lockId);
        if(lockReturn.getResult().compareTo(ResultType.SUCCESS) != 0 ) {
            //\TODO Improve the exponential backoff
            List<Range> pendingToLock = request.getToLockRanges();
            Map<UUID, String> currentLockRef = new HashMap<>();
            int n = 1;
            int low = 1;
            int high = 1000;
            Random r = new Random();
            while (!pendingToLock.isEmpty()) {
                final Map<Range, RangeMriRow> rangeRows = findRangeRows(pendingToLock);
                final Map<UUID, List<Range>> rowsToLock = getPendingRows(rangeRows);
                pendingToLock.clear();
                try {
                    Thread.sleep(((int) Math.round(Math.pow(2, n)) * 1000)
                        + (r.nextInt(high - low) + low));
                } catch (InterruptedException e) {
                    continue;
                }
                n++;
                if (n == 20) {
                    throw new MDBCServiceException("Lock was impossible to obtain, waited for 20 exponential backoffs!");
                }
                //\TODO do this in parallel
                //\TODO there is a race condition here, from the time we get the find range rows, to the time we lock the row,
                //\TODO this race condition can only be solved if require to obtain lock to all related rows in MRI
                //\TODO before fully owningn the range
                //\TODO The rows need to be lock in increasing order of timestamp
                //there could be a new row created
                for (Map.Entry<UUID, List<Range>> pending : rowsToLock.entrySet()) {
                    String newFullyQualifiedKey = music_ns + "." + request.getTable() + "." + pending.getKey().toString();
                    String newLockId;
                    boolean success;
                    if (currentLockRef.containsKey(pending.getKey())) {
                        newLockId = currentLockRef.get(pending.getKey());
                        success = (MusicCore.whoseTurnIsIt(newFullyQualifiedKey) == newLockId);
                    } else {
                        newLockId = MusicCore.createLockReference(newFullyQualifiedKey);
                        ReturnType newLockReturn = acquireLock(fullyQualifiedKey, lockId);
                        success = newLockReturn.getResult().compareTo(ResultType.SUCCESS) == 0;
                    }
                    if (!success) {
                        pendingToLock.addAll(pending.getValue());
                        currentLockRef.put(pending.getKey(), newLockId);
                    } else {
                        result.add(new LockResult(pending.getKey(), newLockId, true, pending.getValue()));
                    }
                }
                if (n++ == 20) {
                    throw new MDBCServiceException(
                        "Lock was impossible to obtain, waited for 20 exponential backoffs!");
                }
            }
        }
        else {
            partition.setLockId(lockId);
            result.add(new LockResult(partition.getMusicRangeInformationIndex(), lockId, true, partition.getSnapshot()));
        }
        return result;
    }

    protected String createAndAssignLock(String fullyQualifiedKey, DatabasePartition partition) throws MDBCServiceException {
        UUID mriIndex = partition.getMusicRangeInformationIndex();
        String lockId;
        lockId = MusicCore.createLockReference(fullyQualifiedKey);
        ReturnType lockReturn = acquireLock(fullyQualifiedKey,lockId);
        //\TODO this is wrong, we should have a better way to obtain a lock forcefully, clean the queue and obtain the lock
        if(lockReturn.getResult().compareTo(ResultType.SUCCESS) != 0 ) {
            return null;
        }
        partition.setLockId(lockId);
        return lockId;
    }



    protected void appendIndexToMri(String lockId, UUID commitId, UUID MriIndex) throws MDBCServiceException{
        PreparedQueryObject appendQuery = createAppendMtxdIndexToMriQuery(musicRangeInformationTableName, MriIndex, musicTxDigestTableName, commitId);
        ReturnType returnType = MusicCore.criticalPut(music_ns, musicRangeInformationTableName, MriIndex.toString(), appendQuery, lockId, null);
        if(returnType.getResult().compareTo(ResultType.SUCCESS) != 0 ){
            logger.error(EELFLoggerDelegate.errorLogger, "Error when executing append operation with return type: "+returnType.getMessage());
            throw new MDBCServiceException("Error when executing append operation with return type: "+returnType.getMessage());
        }
    }

    /**
     * Writes the transaction information to metric's txDigest and musicRangeInformation table
     * This officially commits the transaction globally
     */
    @Override
    public void commitLog(DatabasePartition partition, HashMap<Range,StagingTable> transactionDigest, String txId ,TxCommitProgress progressKeeper) throws MDBCServiceException{
        UUID mriIndex = partition.getMusicRangeInformationIndex();
        if(mriIndex==null) {
            own(partition.getSnapshot(),partition);
        }
        String fullyQualifiedMriKey = music_ns+"."+ this.musicRangeInformationTableName+"."+mriIndex.toString();
        //0. See if reference to lock was already created
        String lockId = partition.getLockId();
        if(lockId == null || lockId.isEmpty()) {
            LockRequest newRequest = new LockRequest(this.musicRangeInformationTableName,mriIndex
                ,partition.getSnapshot());
            waitForLock(newRequest,partition);
        }

        UUID commitId;
        //Generate a local commit id
        if(progressKeeper.containsTx(txId)) {
            commitId = progressKeeper.getCommitId(txId);
        }
        else{
            logger.error(EELFLoggerDelegate.errorLogger, "Tx with id "+txId+" was not created in the TxCommitProgress ");
            throw new MDBCServiceException("Tx with id "+txId+" was not created in the TxCommitProgress ");
        }
        //Add creation type of transaction digest

        //1. Push new row to RRT and obtain its index
        String serializedTransactionDigest;
        try {
            serializedTransactionDigest = MDBCUtils.toString(transactionDigest);
        } catch (IOException e) {
            throw new MDBCServiceException("Failed to serialized transaction digest with error "+e.toString(), e);
        }
        MusicTxDigestId digestId = new MusicTxDigestId(commitId);
        addTxDigest(digestId, serializedTransactionDigest);
        //2. Save RRT index to RQ
        if(progressKeeper!= null) {
            progressKeeper.setRecordId(txId,digestId);
        }
        //3. Append RRT index into the corresponding TIT row array
        appendToRedoLog(mriIndex,partition,digestId);
    }

    /**
     * @param tableName
     * @param string
     * @param rowValues
     * @return
     */
    @SuppressWarnings("unused")
    private String getUid(String tableName, String string, Object[] rowValues) {
        //
        // Update local MUSIC node. Note: in Cassandra you can insert again on an existing key..it becomes an update
        String cql = String.format("SELECT * FROM %s.%s;", music_ns, tableName);
        PreparedStatement ps = getPreparedStatementFromCache(cql);
        BoundStatement bound = ps.bind();
        bound.setReadTimeoutMillis(60000);
        Session sess = getMusicSession();
        ResultSet rs;
        synchronized (sess) {
            rs = sess.execute(bound);
        }

        //should never reach here
        logger.error(EELFLoggerDelegate.errorLogger, "Could not find the row in the primary key");
        return null;
    }

    public Object[] getObjects(TableInfo ti, String tableName, JSONObject row) {
        // \FIXME: we may need to add the primary key of the row if it was autogenerated by MUSIC
        List<String> cols = ti.columns;
        int size = cols.size();
        boolean hasDefault = false;
        if(row.has(getMusicDefaultPrimaryKeyName())) {
            size++;
            hasDefault = true;
        }

        Object[] objects = new Object[size];
        int idx = 0;
        if(hasDefault) {
            objects[idx++] = row.getString(getMusicDefaultPrimaryKeyName());
        }
        for(String col : ti.columns) {
            objects[idx]=row.get(col);
        }
        return objects;
    }

    @Override
    public List<UUID> getPartitionIndexes() throws MDBCServiceException {
        ArrayList<UUID> partitions = new ArrayList<UUID>();
        String cql = String.format("SELECT rangeid FROM %s.%s", music_ns, musicRangeInformationTableName);
        ResultSet rs = executeMusicRead(cql);
        for (Row r: rs) {
            partitions.add(r.getUUID("rangeid"));
        }
        return partitions;
    }

    List<Range> getRanges(Row newRow){
        List<Range> partitions = new ArrayList<>();
        Set<String> tables = newRow.getSet("keys",String.class);
        for (String table:tables){
            partitions.add(new Range(table));
        }
        return partitions;
    }

    MusicRangeInformationRow getMRIRowFromCassandraRow(Row newRow){
        UUID partitionIndex = newRow.getUUID("rangeid");
        List<TupleValue> log = newRow.getList("txredolog",TupleValue.class);
        List<MusicTxDigestId> digestIds = new ArrayList<>();
        for(TupleValue t: log){
            //final String tableName = t.getString(0);
            final UUID index = t.getUUID(1);
            digestIds.add(new MusicTxDigestId(index));
        }
        List<Range> partitions = new ArrayList<>();
        Set<String> tables = newRow.getSet("keys",String.class);
        for (String table:tables){
            partitions.add(new Range(table));
        }
        return new MusicRangeInformationRow(partitionIndex, new DatabasePartition(partitions, partitionIndex, ""),
            digestIds, newRow.getString("ownerid"),newRow.getString("metricprocessid"));
    }

    @Override
    public MusicRangeInformationRow getMusicRangeInformation(UUID partitionIndex) throws MDBCServiceException {
        //TODO: verify that lock id is valid before calling the database operations function
        //UUID id = partition.getMusicRangeInformationIndex();

        String cql = String.format("SELECT * FROM %s.%s WHERE rangeid = ?;", music_ns, musicRangeInformationTableName);
        PreparedQueryObject pQueryObject = new PreparedQueryObject();
        pQueryObject.appendQueryString(cql);
        pQueryObject.addValue(partitionIndex);
        Row newRow;
        try {
            newRow = executeMusicUnlockedQuorumGet(pQueryObject);
        } catch (MDBCServiceException e) {
            logger.error("Get operationt error: Failure to get row from MRI "+musicRangeInformationTableName);
            throw new MDBCServiceException("Initialization error:Failure to add new row to transaction information", e);
        }

    	return getMRIRowFromCassandraRow(newRow);
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
    private void createMusicRangeInformationTable() throws MDBCServiceException {
        String tableName = this.musicRangeInformationTableName;
        String priKey = "rangeid";
        StringBuilder fields = new StringBuilder();
        fields.append("rangeid uuid, ");
        fields.append("keys set<text>, ");
        fields.append("ownerid text, ");
        fields.append("metricprocessid text, ");
        //TODO: Frozen is only needed for old versions of cassandra, please update correspondingly
        fields.append("txredolog list<frozen<tuple<text,uuid>>> ");
        String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s));",
            this.music_ns, tableName, fields, priKey);
        try {
            executeMusicWriteQuery(this.music_ns,tableName,cql);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to create transaction information table");
            throw(e);
        }
    }


    @Override
    public DatabasePartition createMusicRangeInformation(MusicRangeInformationRow info) throws MDBCServiceException {
        DatabasePartition newPartition = info.getDBPartition();

        String fullyQualifiedMriKey = music_ns+"."+ musicRangeInformationTableName+"."+newPartition.getMusicRangeInformationIndex().toString();
        String lockId = createAndAssignLock(fullyQualifiedMriKey,newPartition);
        if(lockId == null || lockId.isEmpty()){
            throw new MDBCServiceException("Error initializing music range information, error creating a lock for a new row") ;
        }
        createEmptyMriRow(newPartition.getMusicRangeInformationIndex(),info.getMetricProcessId(),lockId,newPartition.getSnapshot());
        return newPartition;
    }

    /**
     * Creates a new empty MRI row
     * @param processId id of the process that is going to own initially this.
     * @return uuid associated to the new row
     */
    private UUID createEmptyMriRow(String processId, String lockId, List<Range> ranges)
        throws MDBCServiceException {
        UUID id = MDBCUtils.generateTimebasedUniqueKey();
        return createEmptyMriRow(id,processId,lockId,ranges);
    }

    /**
     * Creates a new empty MRI row
     * @param processId id of the process that is going to own initially this.
     * @return uuid associated to the new row
     */
    private UUID createEmptyMriRow(UUID id, String processId, String lockId, List<Range> ranges)
        throws MDBCServiceException{
        StringBuilder insert = new StringBuilder("INSERT INTO ")
            .append(this.music_ns)
            .append('.')
            .append(this.musicRangeInformationTableName)
            .append(" (rangeid,keys,ownerid,metricprocessid,txredolog) VALUES ")
            .append("(")
            .append(id)
            .append(",{");
        boolean first=true;
        for(Range r: ranges){
            if(first){ first=false; }
            else {
                insert.append(',');
            }
            insert.append("'").append(r.toString()).append("'");
        }
        insert.append("},'")
            .append((lockId==null)?"":lockId)
            .append("','")
            .append(processId)
            .append("',[]);");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(insert.toString());
        try {
            executeMusicLockedPut(this.music_ns,this.musicRangeInformationTableName,id.toString(),query,lockId,null);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to add new row to transaction information");
            throw new MDBCServiceException("Initialization error:Failure to add new row to transaction information", e);
        }
        return id;
    }

    @Override
    public void appendToRedoLog(UUID mriRowId, DatabasePartition partition, MusicTxDigestId newRecord) throws MDBCServiceException {
        logger.info("Appending to redo log for partition " + partition.getMusicRangeInformationIndex() + " txId=" + newRecord.txId);
        PreparedQueryObject appendQuery = createAppendMtxdIndexToMriQuery(musicRangeInformationTableName, mriRowId, musicTxDigestTableName, newRecord.txId);
        ReturnType returnType = MusicCore.criticalPut(music_ns, musicRangeInformationTableName, mriRowId.toString(), appendQuery, partition.getLockId(), null);
        if(returnType.getResult().compareTo(ResultType.SUCCESS) != 0 ){
            logger.error(EELFLoggerDelegate.errorLogger, "Error when executing append operation with return type: "+returnType.getMessage());
            throw new MDBCServiceException("Error when executing append operation with return type: "+returnType.getMessage());
        }
    }

    public void createMusicTxDigest() throws MDBCServiceException {
        createMusicTxDigest(-1);
    }


    /**
     * This function creates the MusicTxDigest table. It contain information related to each transaction committed
     * 	* LeaseId: id associated with the lease, text
     * 	* LeaseCounter: transaction number under this lease, bigint \TODO this may need to be a varint later
     *  * TransactionDigest: text that contains all the changes in the transaction
     */
    private void createMusicTxDigest(int musicTxDigestTableNumber) throws MDBCServiceException {
        String tableName = this.musicTxDigestTableName;
        if(musicTxDigestTableNumber >= 0) {
            tableName = tableName +
                "-" +
                Integer.toString(musicTxDigestTableNumber);
        }
        String priKey = "txid";
        StringBuilder fields = new StringBuilder();
        fields.append("txid uuid, ");
        fields.append("transactiondigest text ");//notice lack of ','
        String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s));", this.music_ns, tableName, fields, priKey);
        try {
            executeMusicWriteQuery(this.music_ns,tableName,cql);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to create redo records table");
            throw(e);
        }
    }


    /**
     * Writes the transaction history to the txDigest
     */
    @Override
    public void addTxDigest(MusicTxDigestId newId, String transactionDigest) throws MDBCServiceException {
        //createTxDigestRow(music_ns,musicTxDigestTable,newId,transactionDigest);
        PreparedQueryObject query = new PreparedQueryObject();
        String cqlQuery = "INSERT INTO " +
            this.music_ns +
            '.' +
            this.musicTxDigestTableName +
            " (txid,transactiondigest) " +
            "VALUES (" +
            newId.txId + ",'" +
            transactionDigest +
            "');";
        query.appendQueryString(cqlQuery);
        //\TODO check if I am not shooting on my own foot
        try {
            MusicCore.nonKeyRelatedPut(query,"critical");
        } catch (MusicServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger, "Transaction Digest serialization was invalid for commit "+newId.txId.toString()+ "with error "+e.getErrorMessage());
            throw new MDBCServiceException("Transaction Digest serialization for commit "+newId.txId.toString(), e);
        }
    }

    @Override
    public HashMap<Range,StagingTable> getTxDigest(MusicTxDigestId id) throws MDBCServiceException {
        String cql = String.format("SELECT * FROM %s.%s WHERE txid = ?;", music_ns, musicTxDigestTableName);
        PreparedQueryObject pQueryObject = new PreparedQueryObject();
        pQueryObject.appendQueryString(cql);
        pQueryObject.addValue(id.txId);
        Row newRow;
        try {
            newRow = executeMusicUnlockedQuorumGet(pQueryObject);
        } catch (MDBCServiceException e) {
            logger.error("Get operation error: Failure to get row from txdigesttable with id:"+id.txId);
            throw new MDBCServiceException("Initialization error:Failure to add new row to transaction information", e);
        }
        String digest = newRow.getString("transactiondigest");
        HashMap<Range,StagingTable> changes;
        try {
            changes = (HashMap<Range, StagingTable>) MDBCUtils.fromString(digest);
        } catch (IOException e) {
            logger.error("IOException when deserializing digest failed with an invalid class for id:"+id.txId);
            throw new MDBCServiceException("Deserializng digest failed with ioexception", e);
        } catch (ClassNotFoundException e) {
            logger.error("Deserializng digest failed with an invalid class for id:"+id.txId);
            throw new MDBCServiceException("Deserializng digest failed with an invalid class", e);
        }
        return changes;
    }

    private ResultSet getAllMriRows() throws MDBCServiceException {
        StringBuilder cqlOperation = new StringBuilder();
        cqlOperation.append("SELECT * FROM ")
            .append(music_ns)
            .append(".")
            .append(musicRangeInformationTableName);
        return executeMusicRead(cqlOperation.toString());
    }

    private RangeMriRow findRangeRow(Range range) throws MDBCServiceException {
        RangeMriRow row = null;
        final ResultSet musicResults = getAllMriRows();
        while (!musicResults.isExhausted()) {
            Row musicRow = musicResults.one();
            final MusicRangeInformationRow mriRow = getMRIRowFromCassandraRow(musicRow);
            final List<Range> musicRanges = getRanges(musicRow);
            //\TODO optimize this for loop to avoid redudant access
            for(Range retrievedRange : musicRanges) {
                if (retrievedRange.overlaps(range)) {
                    if(row==null){
                        row = new RangeMriRow(range);
                        row.setCurrentRow(mriRow);
                    }
                    else if(row.getCurrentRow().getTimestamp() < mriRow.getTimestamp()){
                        row.addOldRow(row.getCurrentRow());
                        row.setCurrentRow(mriRow);
                    }
                }
            }
        }
        if(row==null){
             logger.error("Row in MRI doesn't exist for Range "+range.toString());
            throw new MDBCServiceException("Row in MRI doesn't exist for Range "+range.toString());
        }
        return row;
    }

    /**
     * This function is used to find all the related uuids associated with the required ranges
     * @param ranges ranges to be find
     * @return a map that associates each MRI row to the corresponding ranges
     */
    private Map<Range,RangeMriRow> findRangeRows(List<Range> ranges) throws MDBCServiceException {
        /* \TODO this function needs to be improved, by creating an additional index, or at least keeping a local cache
         Additionally, we should at least used pagination and the token function, to avoid retrieving the whole table at
         once, this can become problematic if we have too many connections in the overall METRIC system */
        Map<Range,RangeMriRow> result = new HashMap<>();
        for(Range r:ranges){
            result.put(r,null);
        }
        int counter=0;
        final ResultSet musicResults = getAllMriRows();
        while (!musicResults.isExhausted()) {
            Row musicRow = musicResults.one();
            final MusicRangeInformationRow mriRow = getMRIRowFromCassandraRow(musicRow);
            final List<Range> musicRanges = getRanges(musicRow);
            //\TODO optimize this for loop to avoid redudant access
            for(Range retrievedRange : musicRanges) {
                for(Map.Entry<Range,RangeMriRow> e : result.entrySet()) {
                    Range range = e.getKey();
                    if (retrievedRange.overlaps(range)) {
                        RangeMriRow r = e.getValue();
                        if(r==null){
                            counter++;
                            RangeMriRow newMriRow = new RangeMriRow(range);
                            newMriRow.setCurrentRow(mriRow);
                            result.replace(range,newMriRow);
                        }
                        else if(r.getCurrentRow().getTimestamp() < mriRow.getTimestamp()){
                            r.addOldRow(r.getCurrentRow());
                            r.setCurrentRow(mriRow);
                        }
                    }
                }
            }
        }

        if(ranges.size() != counter){
            logger.error("Row in MRI doesn't exist for "+Integer.toString(counter)+" ranges");
            throw new MDBCServiceException("MRI row doesn't exist for "+Integer.toString(counter)+" ranges");
        }
        return result;
    }

    private List<LockResult> lockRow(LockRequest request, DatabasePartition partition)
        throws MDBCServiceException {
        if(partition.getMusicRangeInformationIndex()==request.id){
            List<LockResult> result = new ArrayList<>();
            result.add(new LockResult(request.id,partition.getLockId(),false,partition.getSnapshot()));
            return result;
        }
        //\TODO: this function needs to be improved, to track possible changes in the owner of a set of ranges
        String fullyQualifiedMriKey = music_ns+"."+ this.musicRangeInformationTableName+"."+request.id.toString();
        //return List<Range> knownRanges, UUID mriIndex, String lockId
        DatabasePartition newPartition = new DatabasePartition(request.toLockRanges,request.id,null);
        return waitForLock(request,newPartition);
    }

    @Override
    public OwnershipReturn own(List<Range> ranges, DatabasePartition partition) throws MDBCServiceException {
        UUID newId = MDBCUtils.generateTimebasedUniqueKey();
        return appendRange(newId.toString(),ranges,partition);
    }

    /**
     * This function is used to check if we need to create a new row in MRI, beacause one of the new ranges is not contained
     * @param ranges ranges that should be contained in the partition
     * @param partition currently own partition
     * @return
     */
    public boolean isAppendRequired(List<Range> ranges, DatabasePartition partition){
        for(Range r: ranges){
            if(!partition.isContained(r)){
                return true;
            }
        }
        return false;
    }

    private Map<UUID,String> mergeMriRows(String newId, Map<UUID,LockResult> lock, DatabasePartition partition)
        throws MDBCServiceException {
        Map<UUID,String> oldIds = new HashMap<>();
        List<Range> newRanges = new ArrayList<>();
        for (Map.Entry<UUID,LockResult> entry : lock.entrySet()) {
            oldIds.put(entry.getKey(),entry.getValue().ownerId);
            //\TODO check if we need to do a locked get? Is that even required?
            final MusicRangeInformationRow mriRow = getMusicRangeInformation(entry.getKey());
            final DatabasePartition dbPartition = mriRow.getDBPartition();
            newRanges.addAll(dbPartition.getSnapshot());
        }
        DatabasePartition newPartition = new DatabasePartition(newRanges,UUID.fromString(newId),null);
        String fullyQualifiedMriKey = music_ns+"."+ this.musicRangeInformationTableName+"."+newId;
        LockRequest newRequest = new LockRequest(musicRangeInformationTableName,UUID.fromString(newId),newRanges);
        final List<LockResult> lockResults = waitForLock(newRequest, newPartition);
        if(lockResults.size()!=1||!lockResults.get(0).newLock){
            logger.error("When merging rows, lock returned an invalid error");
            throw new MDBCServiceException("When merging MRI rows, lock returned an invalid error");
        }
        final LockResult lockResult = lockResults.get(0);
        partition.updateDatabasePartition(newPartition);
        createEmptyMriRow(partition.getMusicRangeInformationIndex(),myId,lockResult.ownerId,partition.getSnapshot());
        return oldIds;
    }



    @Override
    public OwnershipReturn appendRange(String rangeId, List<Range> ranges, DatabasePartition partition)
        throws MDBCServiceException {
        if(!isAppendRequired(ranges,partition)){
            return new OwnershipReturn(partition.getLockId(),UUID.fromString(rangeId),null,null);
        }
        Map<Range, RangeMriRow> rows = findRangeRows(ranges);
        final Map<UUID, List<Range>> rowsToLock = getPendingRows(rows);
        HashMap<UUID, LockResult> rowLock = new HashMap<>();
        List<Range> newRanges = new ArrayList<>();
        boolean newLock = false;
        //\TODO: perform this operations in parallel
        for(Map.Entry<UUID,List<Range>> row : rowsToLock.entrySet()){
            List<LockResult> locks;
            try {
                LockRequest newRequest = new LockRequest(musicRangeInformationTableName,row.getKey(),row.getValue());
                locks = lockRow(newRequest, partition);
            } catch (MDBCServiceException e) {
                //TODO: Make a decision if retry or just fail?
                logger.error("Error locking row");
                throw e;
            }
            for(LockResult l : locks){
                if(l.getNewLock()) {
                    newLock = true;
                    newRanges.addAll(l.getRanges());
                }
                rowLock.put(l.getIndex(),l);
            }
        }
        String lockId;
        Map<UUID,String> oldIds = null;
        if(rowLock.size()!=1){
            oldIds = mergeMriRows(rangeId, rowLock, partition);
            lockId = partition.getLockId();
        }
        else{
            List<LockResult> list = new ArrayList<>(rowLock.values());
            LockResult lockResult = list.get(0);
            lockId = lockResult.getOwnerId();
        }

        return new OwnershipReturn(lockId,UUID.fromString(rangeId),oldIds,newRanges);
    }

    @Override
    public void relinquish(String ownerId, String rangeId) throws MDBCServiceException{
        String fullyQualifiedMriKey = music_ns+"."+ musicRangeInformationTableName+"."+rangeId;
        try {
            MusicCore.voluntaryReleaseLock(fullyQualifiedMriKey,ownerId);
        } catch (MusicLockingException e) {
            throw new MDBCServiceException(e.getMessage(), e);
        }
    }

    /**
     * This function is used to rate the number of times we relinquish at the end of a transaction
     * @return true if we should try to relinquish, else should avoid relinquishing in this iteration
     */
    private boolean canTryRelinquishing(){
        return true;
    }

    @Override
    public void relinquishIfRequired(DatabasePartition partition) throws MDBCServiceException {
        if(!canTryRelinquishing() || !partition.isLocked()){
            return;
        }
        CassaLockStore lsHandle;
        try {
            lsHandle = MusicCassaCore.getLockingServiceHandle();
        } catch (MusicLockingException e) {
            logger.error("Error obtaining the locking service handle when checking if relinquish was required");
            throw new MDBCServiceException("Error obtaining locking service"+e.getMessage(), e);
        }
        long lockQueueSize;
        try {
            lockQueueSize = lsHandle.getLockQueueSize(music_ns, this.musicRangeInformationTableName, partition.getMusicRangeInformationIndex().toString());
        } catch (MusicServiceException|MusicQueryException e) {
            logger.error("Error obtaining the lock queue size");
            throw new MDBCServiceException("Error obtaining lock queue size: "+e.getMessage(), e);
        }
        if(lockQueueSize> 1){
            //If there is any other node waiting, we just relinquish ownership
            try {
                relinquish(partition.getLockId(),partition.getMusicRangeInformationIndex().toString());
            } catch (MDBCServiceException e) {
                logger.error("Error relinquishing lock, will use timeout to solve");
            }
            partition.setLockId("");
        }
    }

    /**
     * This method executes a write query in Music
     * @param cql the CQL to be sent to Cassandra
     */
    private static void executeMusicWriteQuery(String keyspace, String table, String cql)
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
        String result = rt.getResult();
        if (result==null || result.toLowerCase().equals("failure")) {
            throw new MDBCServiceException("Music eventual put failed");
        }
    }

    private static Row executeMusicLockedGet(String keyspace, String table, PreparedQueryObject cqlObject, String primaryKey,
                                             String lock)
        throws MDBCServiceException{
        ResultSet result;
        try {
            result = MusicCore.criticalGet(keyspace,table,primaryKey,cqlObject,lock);
        } catch(MusicServiceException e){
            //\TODO: handle better, at least transform into an MDBCServiceException
            e.printStackTrace();
            throw new MDBCServiceException("Error executing critical get", e);
        }
        if(result.isExhausted()){
            throw new MDBCServiceException("There is not a row that matches the id "+primaryKey);
        }
        return result.one();
    }

    private static Row executeMusicUnlockedQuorumGet(PreparedQueryObject cqlObject)
        throws MDBCServiceException{
        ResultSet result = MusicCore.quorumGet(cqlObject);
        //\TODO: handle better, at least transform into an MDBCServiceException
        if(result.isExhausted()){
            throw new MDBCServiceException("There is not a row that matches the query: ["+cqlObject.getQuery()+"]");
        }
        return result.one();
    }

    private void executeMusicLockedPut(String namespace, String tableName,
                                       String primaryKeyWithoutDomain, PreparedQueryObject queryObject, String lockId,
                                       Condition conditionInfo) throws MDBCServiceException {
        ReturnType rt ;
        if(lockId==null) {
            try {
                rt = MusicCore.atomicPut(namespace, tableName, primaryKeyWithoutDomain, queryObject, conditionInfo);
            } catch (MusicLockingException e) {
                logger.error("Music locked put failed");
                throw new MDBCServiceException("Music locked put failed", e);
            } catch (MusicServiceException e) {
                logger.error("Music service fail: Music locked put failed");
                throw new MDBCServiceException("Music service fail: Music locked put failed", e);
            } catch (MusicQueryException e) {
                logger.error("Music query fail: locked put failed");
                throw new MDBCServiceException("Music query fail: Music locked put failed", e);
            }
        }
        else {
            rt = MusicCore.criticalPut(namespace, tableName, primaryKeyWithoutDomain, queryObject, lockId, conditionInfo);
        }
        if (rt.getResult().getResult().toLowerCase().equals("failure")) {
            throw new MDBCServiceException("Music locked put failed");
        }
    }

    private void executeMusicLockedDelete(String namespace, String tableName, String primaryKeyValue, String lockId
        ) throws MDBCServiceException{
        StringBuilder delete = new StringBuilder("DELETE FROM ")
            .append(namespace)
            .append('.')
            .append(tableName)
            .append(" WHERE rangeid= ")
            .append(primaryKeyValue)
            .append(";");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(delete.toString());
        executeMusicLockedPut(namespace,tableName,primaryKeyValue,query,lockId,null);
    }

    @Override
    public void replayTransaction(HashMap<Range,StagingTable> digest) throws MDBCServiceException{
        throw new NotImplementedException("Error, replay transaction in music mixin needs to be implemented");
    }

    @Override
    public void deleteOldMriRows(Map<UUID, String> oldRowsAndLocks) throws MDBCServiceException {
        //\TODO Do this operations in parallel or combine in only query to cassandra
        for(Map.Entry<UUID,String> rows : oldRowsAndLocks.entrySet()){
            //\TODO handle music delete correctly so we can delete the other rows
            executeMusicLockedDelete(music_ns,musicRangeInformationTableName,rows.getKey().toString(),rows.getValue());
        }

    }
}
