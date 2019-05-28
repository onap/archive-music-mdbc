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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.*;

import com.datastax.driver.core.*;
import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONObject;
import org.onap.music.datastore.Condition;
import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.main.MusicCore;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.MDBCUtils;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.StateManager;
import org.onap.music.mdbc.TableInfo;
import org.onap.music.mdbc.ownership.Dag;
import org.onap.music.mdbc.ownership.DagNode;
import org.onap.music.mdbc.query.SQLOperationType;
import org.onap.music.mdbc.tables.MriReference;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.MusicTxDigestId;
import org.onap.music.mdbc.tables.RangeDependency;
import org.onap.music.mdbc.tables.StagingTable;
import org.onap.music.mdbc.tables.TxCommitProgress;

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
    /**  The property name to use to provide a timeout to mdbc (ownership) */
    public static final String KEY_TIMEOUT = "mdbc_timeout";
    /**  The property name to use to provide a flag indicating if compression is required */
    public static final String KEY_COMPRESSION = "mdbc_compression";
    /** Namespace for the tables in MUSIC (Cassandra) */
    public static final String DEFAULT_MUSIC_NAMESPACE = "namespace";
    /** The default property value to use for the Cassandra IP address. */
    public static final String DEFAULT_MUSIC_ADDRESS  = "localhost";
    /** The default property value to use for the Cassandra replication factor. */
    public static final int    DEFAULT_MUSIC_RFACTOR  = 1;
    /** The default property value to use for the MDBC timeout */
    public static final long DEFAULT_TIMEOUT = 5*60*60*1000;//default of 5 hours
    /** The default primary string column, if none is provided. */
    public static final String MDBC_PRIMARYKEY_NAME = "mdbc_cuid";
    /** Type of the primary key, if none is defined by the user */
    public static final String MDBC_PRIMARYKEY_TYPE = "uuid";
    public static final boolean DEFAULT_COMPRESSION = true;
    //TODO: Control network topology strategy with a configuration file entry
    public static final boolean ENABLE_NETWORK_TOPOLOGY_STRATEGY = false;

    //\TODO Add logic to change the names when required and create the tables when necessary
    private String musicTxDigestTableName = "musictxdigest";
    private String musicEventualTxDigestTableName = "musicevetxdigest";
    public static final String musicRangeInformationTableName = "musicrangeinformation";
    private String musicRangeDependencyTableName = "musicrangedependency";
    private String musicNodeInfoTableName = "nodeinfo";

    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicMixin.class);



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


    private static final Map<Integer, String> typemap         = new HashMap<>();
    static {
        // We only support the following type mappings currently (from DB -> Cassandra).
        // Anything else will likely cause a NullPointerException
        typemap.put(Types.BIGINT,    "BIGINT"); // aka. IDENTITY
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
        typemap.put(Types.CHAR,      "VARCHAR");
        //The "Hacks", these don't have a direct mapping
        //typemap.put(Types.DATE,        "VARCHAR");
        //typemap.put(Types.DATE,        "TIMESTAMP");
    }


    protected final String music_ns;
    protected final String myId;
    protected final String[] allReplicaIds;
    protected ExecutorService commitExecutorThreads;

    private final String musicAddress;
    private final int    music_rfactor;
    private MusicConnector mCon        = null;
    private Session musicSession       = null;
    private boolean keyspace_created   = false;
    private Map<String, PreparedStatement> ps_cache = new HashMap<>();
    private Set<String> in_progress    = Collections.synchronizedSet(new HashSet<String>());
    private StateManager stateManager;
    private boolean useCompression;

    public MusicMixin() {

        //this.logger         = null;
        this.musicAddress   = null;
        this.music_ns       = null;
        this.music_rfactor  = 0;
        this.myId           = null;
        this.allReplicaIds  = null;
    }

    public MusicMixin(StateManager stateManager, String mdbcServerName, Properties info) throws MDBCServiceException {
        // Default values -- should be overridden in the Properties
        // Default to using the host_ids of the various peers as the replica IDs (this is probably preferred)
        this.musicAddress   = info.getProperty(KEY_MUSIC_ADDRESS, DEFAULT_MUSIC_ADDRESS);
        logger.info(EELFLoggerDelegate.applicationLogger,"MusicSqlManager: musicAddress="+musicAddress);
        MusicDataStore dsHandle = null;
        try {
            dsHandle = MusicDataStoreHandle.getDSHandle();
        } catch (MusicServiceException e) {
            e.printStackTrace();
        }

        this.myId           = info.getProperty(KEY_MY_ID,    getMyHostId());
        logger.info(EELFLoggerDelegate.applicationLogger,"MusicSqlManager: myId="+myId);

        this.allReplicaIds  = info.getProperty(KEY_REPLICAS, getAllHostIds()).split(",");
        logger.info(EELFLoggerDelegate.applicationLogger,"MusicSqlManager: allReplicaIds="+info.getProperty(KEY_REPLICAS, this.myId));

        this.music_ns       = info.getProperty(KEY_MUSIC_NAMESPACE,DEFAULT_MUSIC_NAMESPACE);
        logger.info(EELFLoggerDelegate.applicationLogger,"MusicSqlManager: music_ns="+music_ns);

        this.stateManager = stateManager;
        
        String c = info.getProperty(KEY_COMPRESSION);
        this.useCompression = (c == null) ? DEFAULT_COMPRESSION: Boolean.parseBoolean(c);

        String s            = info.getProperty(KEY_MUSIC_RFACTOR);
        this.music_rfactor  = (s == null) ? DEFAULT_MUSIC_RFACTOR : Integer.parseInt(s);

        initializeMetricTables();
        commitExecutorThreads = Executors.newFixedThreadPool(4);
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
        createKeyspace(this.music_ns,this.music_rfactor);
    }

    public static void createKeyspace(String keyspace, int replicationFactor) throws MDBCServiceException {
        Map<String,Object> replicationInfo = new HashMap<>();
        replicationInfo.put("'class'", "'NetworkTopologyStrategy'");

        if (ENABLE_NETWORK_TOPOLOGY_STRATEGY && replicationFactor==3) {
            replicationInfo.put("'dc1'", 1);
            replicationInfo.put("'dc2'", 1);
            replicationInfo.put("'dc3'", 1);
        } else {
            replicationInfo.put("'class'", "'SimpleStrategy'");
            replicationInfo.put("'replication_factor'", replicationFactor);
        }

        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(
            "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                " WITH REPLICATION = " + replicationInfo.toString().replaceAll("=", ":"));

        try {
            MusicCore.nonKeyRelatedPut(queryObject, "eventual");
        } catch (MusicServiceException e) {
            if (!e.getMessage().equals("Keyspace "+keyspace+" already exists")) {
                throw new MDBCServiceException("Error creating namespace: "+keyspace+". Internal error:"+e.getErrorMessage(),
                    e);
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
            createMusicEventualTxDigest();
            createMusicNodeInfoTable();
            createMusicRangeInformationTable(this.music_ns,this.musicRangeInformationTableName);
            createMusicRangeDependencyTable(this.music_ns,this.musicRangeDependencyTableName);
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
//      for (String repl : allReplicaIds) {
////            String dirtyRowsTableName = "dirty_"+tableName+"_"+allReplicaIds[i];
////            String dirtyTableQuery = "CREATE TABLE IF NOT EXISTS "+music_ns+"."+ dirtyRowsTableName+" (dirtyRowKeys text PRIMARY KEY);";
//          cql = String.format("CREATE TABLE IF NOT EXISTS %s.DIRTY_%s_%s (dirtyRowKeys TEXT PRIMARY KEY);", music_ns, tableName, repl);
//          executeMusicWriteQuery(cql);
//      }
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
                set.add(row.getString("TABLE_NAME"));
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
        in_progress.add(rowid);         // Block propagation of the following INSERT/UPDATE

        dbi.insertRowIntoSqlDb(tableName, map);

        logger.debug("Unblocking rowid: "+rowid);
        in_progress.remove(rowid);      // Unblock propagation

//      try {
//          String sql = String.format("INSERT INTO %s (%s) VALUES (%s);", tableName, fields.toString(), values.toString());
//          executeSQLWrite(sql);
//      } catch (SQLException e) {
//          logger.debug("Insert failed because row exists, do an update");
//          // TODO - rewrite this UPDATE command should not update key fields
//          String sql = String.format("UPDATE %s SET (%s) = (%s) WHERE %s", tableName, fields.toString(), values.toString(), where.toString());
//          try {
//              executeSQLWrite(sql);
//          } catch (SQLException e1) {
//              e1.printStackTrace();
//          }
//      }

        ti = dbi.getTableInfo(tableName);
        cleanDirtyRow(ti, tableName, new JSONObject(vallist));

//      String selectQuery = "select "+ primaryKeyName+" FROM "+tableName+" WHERE "+primaryKeyName+"="+primaryKeyValue+";";
//      java.sql.ResultSet rs = executeSQLRead(selectQuery);
//      String dbWriteQuery=null;
//      try {
//          if(rs.next()){//this entry is there, do an update
//              dbWriteQuery = "UPDATE "+tableName+" SET "+columnNameString+" = "+ valueString +"WHERE "+primaryKeyName+"="+primaryKeyValue+";";
//          }else
//              dbWriteQuery = "INSERT INTO "+tableName+" VALUES"+valueString+";";
//          executeSQLWrite(dbWriteQuery);
//      } catch (SQLException e) {
//          // ZZTODO Auto-generated catch block
//          e.printStackTrace();
//      }

        //clean the music dirty bits table
//      String dirtyRowIdsTableName = music_ns+".DIRTY_"+tableName+"_"+myId;
//      String deleteQuery = "DELETE FROM "+dirtyRowIdsTableName+" WHERE dirtyRowKeys=$$"+primaryKeyValue+"$$;";
//      executeMusicWriteQuery(deleteQuery);
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
        
    }

    /**
     * This method executes a read query in Music
     * @param cql the CQL to be sent to Cassandra
     * @return a ResultSet containing the rows returned from the query
     */
    protected ResultSet executeMusicRead(String cql) throws MDBCServiceException {
        logger.debug(EELFLoggerDelegate.applicationLogger, "Executing MUSIC read:"+ cql);
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
     * This method executes a read query in Music
     * @param pQueryObject the PreparedQueryObject to be sent to Cassandra
     * @return a ResultSet containing the rows returned from the query
     */
    protected ResultSet executeMusicRead(PreparedQueryObject pQueryObject) throws MDBCServiceException {
        logger.debug(EELFLoggerDelegate.applicationLogger, "Executing MUSIC read:"+ pQueryObject.getQuery());
        ResultSet results = null;
        try {
            results = MusicCore.get(pQueryObject);
        } catch (MusicServiceException e) {
            logger.error("Error executing music get operation for query: ["+pQueryObject.getQuery()+"]");
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

    private PreparedQueryObject createChangeIsLatestToMriQuery(String mriTable, UUID uuid, String table, boolean isLatest){
         PreparedQueryObject query = new PreparedQueryObject();
        StringBuilder appendBuilder = new StringBuilder();
        appendBuilder.append("UPDATE ")
            .append(music_ns)
            .append(".")
            .append(mriTable)
            .append(" SET islatest =")
            .append(isLatest)
            .append(" WHERE rangeid = ")
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

    private void addRange(Map<UUID,List<Range>> container, UUID index, Range range){
        if(!container.containsKey(index)){
            container.put(index,new ArrayList<Range>());
        }
        container.get(index).add(range);
    }

    private void addRows(Map<UUID,List<Range>> container, RangeMriRow newRow, Range range){
        //First add current row
        MusicRangeInformationRow currentRow = newRow.getCurrentRow();
        addRange(container,currentRow.getPartitionIndex(),range);
        for(MusicRangeInformationRow row : newRow.getOldRows()){
            addRange(container,row.getPartitionIndex(),range);
        }
    }

    private NavigableMap<UUID, List<Range>> getPendingRows(Map<Range, RangeMriRow> rangeRows){
        NavigableMap<UUID,List<Range>> pendingRows = new TreeMap<>();
        rangeRows.forEach((key, value) -> {
            addRows(pendingRows,value,key);
        });
        return pendingRows;
    }

    private List<Range> lockRow(LockRequest request,Map.Entry<UUID, List<Range>> pending,Map<UUID, String> currentLockRef,
                         String fullyQualifiedKey, String lockId, List<Range> pendingToLock,
                         Map<UUID, LockResult> alreadyHeldLocks)
        throws MDBCServiceException{
        List<Range> newRanges = new ArrayList<>();
        String newFullyQualifiedKey = music_ns + "." + musicRangeInformationTableName + "." + pending.getKey().toString();
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
            if(alreadyHeldLocks.containsKey(pending.getKey())){
                throw new MDBCServiceException("Adding key that already exist");
            }
            alreadyHeldLocks.put(pending.getKey(),new LockResult(pending.getKey(), newLockId, true,
                pending.getValue()));
            newRanges.addAll(pending.getValue());
        }
        return newRanges;
    }

    private boolean isDifferent(NavigableMap<UUID, List<Range>> previous, NavigableMap<UUID, List<Range>> current){
        return previous.keySet().equals(current.keySet());
    }

    protected String createAndAssignLock(String fullyQualifiedKey, DatabasePartition partition) throws MDBCServiceException {
        UUID mriIndex = partition.getMRIIndex();
        String lockId;
        lockId = MusicCore.createLockReference(fullyQualifiedKey);
        if(lockId==null) {
           throw new MDBCServiceException("lock reference is null");
        }
        ReturnType lockReturn;
        int counter=0;
        do {
            if(counter > 0){
                //TODO: Improve backoff
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    logger.warn("Error sleeping for acquiring the lock");
                }
                logger.warn("Error acquiring lock id: ["+lockId+"] for key: ["+fullyQualifiedKey+"]");
            }
            lockReturn = acquireLock(fullyQualifiedKey,lockId);
        }while((lockReturn == null||lockReturn.getResult().compareTo(ResultType.SUCCESS) != 0 )&&(counter++<3));

        //\TODO this is wrong, we should have a better way to obtain a lock forcefully, clean the queue and obtain the lock
        if(lockReturn.getResult().compareTo(ResultType.SUCCESS) != 0 ) {
            logger.error("Lock acquire returned invalid error: "+lockReturn.getResult().name());
            return null;
        }
        partition.setLockId(lockId);
        return lockId;
    }

    protected void changeIsLatestToMRI(MusicRangeInformationRow row, boolean isLatest, LockResult lock) throws MDBCServiceException{
       
        if(lock == null)
            return;
        PreparedQueryObject appendQuery = createChangeIsLatestToMriQuery(musicRangeInformationTableName, row.getPartitionIndex(),
            musicTxDigestTableName, isLatest);
        ReturnType returnType = MusicCore.criticalPut(music_ns, musicRangeInformationTableName, row.getPartitionIndex().toString(),
            appendQuery, 
            lock.getLockId()
            , null);
        if(returnType.getResult().compareTo(ResultType.SUCCESS) != 0 ){
            logger.error(EELFLoggerDelegate.errorLogger, "Error when executing change isLatest operation with return type: "+returnType.getMessage());
            throw new MDBCServiceException("Error when executing change isLatest operation with return type: "+returnType.getMessage());
        }
    }

    public void createAndAddTxDigest(final StagingTable transactionDigest, UUID digestId)
        throws MDBCServiceException {
        ByteBuffer serializedTransactionDigest;
        serializedTransactionDigest = transactionDigest.getSerializedStagingAndClean();
        if(useCompression){
            serializedTransactionDigest = StagingTable.Compress(serializedTransactionDigest);
        }
        addTxDigest(digestId, serializedTransactionDigest);
    }

    /**
     * Writes the transaction information to metric's txDigest and musicRangeInformation table
     * This officially commits the transaction globally
     */
    @Override
    public void commitLog(DatabasePartition partition,List<Range> eventualRanges,  StagingTable transactionDigest,
                          String txId ,TxCommitProgress progressKeeper) throws MDBCServiceException {
        
        // first deal with commit for eventually consistent tables
        filterAndAddEventualTxDigest(eventualRanges, transactionDigest, txId, progressKeeper);
        
        if(partition==null){
            logger.warn("Trying tcommit log with null partition");
            return;
        }

        List<Range> snapshot = partition.getSnapshot();
        if(snapshot==null || snapshot.isEmpty()){
            logger.warn("Trying to commit log with empty ranges");
            return;
        }

        UUID mriIndex = partition.getMRIIndex();
        String fullyQualifiedMriKey = music_ns+"."+ this.musicRangeInformationTableName+"."+mriIndex;
        //0. See if reference to lock was already created
        String lockId = partition.getLockId();
        if(mriIndex==null || lockId == null || lockId.isEmpty()) {
            throw new MDBCServiceException("Not able to commit, as you are no longer the lock-holder for this partition");
        }

        //Add creation type of transaction digest
        if(transactionDigest == null || transactionDigest.isEmpty()) {
            return;
        }

        final MusicTxDigestId digestId = new MusicTxDigestId(MDBCUtils.generateUniqueKey(), -1);
        Callable<Boolean> insertDigestCallable =()-> {
            try {
                createAndAddTxDigest(transactionDigest,digestId.transactionId);
                return true;
            } catch (MDBCServiceException e) {
                logger.error(EELFLoggerDelegate.errorLogger, "Error creating and pushing tx digest to music",e);
                return false;
            }
        };
        Callable<Boolean> appendCallable=()-> {
            try {
                appendToRedoLog(music_ns, mriIndex, digestId.transactionId, lockId, musicTxDigestTableName,
                    musicRangeInformationTableName);
                return true;
            } catch (MDBCServiceException e) {
                logger.error(EELFLoggerDelegate.errorLogger, "Error creating and pushing tx digest to music",e);
                return false;
            }
        };

        Future<Boolean> appendResultFuture = commitExecutorThreads.submit(appendCallable);
        Future<Boolean> digestFuture = commitExecutorThreads.submit(insertDigestCallable);
        try {
            //Boolean appendResult = appendResultFuture.get();
            Boolean digestResult = digestFuture.get();
            if(/*!appendResult ||*/ !digestResult){
                logger.error(EELFLoggerDelegate.errorLogger, "Error appending to log or adding tx digest");
                throw new MDBCServiceException("Error appending to log or adding tx digest");
            }
        } catch (InterruptedException|ExecutionException e) {
            logger.error(EELFLoggerDelegate.errorLogger, "Error executing futures for creating and pushing tx " +
                "digest to music",e);
            throw new MDBCServiceException("Failure when retrieving futures for execution of digestion creation and append", e);
        }

        if (progressKeeper != null) {
            progressKeeper.setRecordId(txId, digestId);
        }
        List<Range> ranges = partition.getSnapshot();
        for(Range r : ranges) {
            Map<Range, Pair<MriReference, Integer>> alreadyApplied = stateManager.getOwnAndCheck().getAlreadyApplied();
            if(!alreadyApplied.containsKey(r)){
                throw new MDBCServiceException("already applied data structure was not updated correctly and range "
                    +r+" is not contained");
            }
            Pair<MriReference, Integer> rowAndIndex = alreadyApplied.get(r);
            MriReference key = rowAndIndex.getKey();
            if(!mriIndex.equals(key.index)){
                throw new MDBCServiceException("already applied data structure was not updated correctly and range "+
                    r+" is not pointing to row: "+mriIndex.toString());
            }
            alreadyApplied.put(r, Pair.of(new MriReference(mriIndex), rowAndIndex.getValue()+1));
        }
    }    

    private void filterAndAddEventualTxDigest(List<Range> eventualRanges,
                                              StagingTable transactionDigest, String txId,
                                              TxCommitProgress progressKeeper) throws MDBCServiceException {
        
        if(eventualRanges == null || eventualRanges.isEmpty()) {
            return;
        }

        if(!transactionDigest.areEventualContained(eventualRanges)){
            throw new MDBCServiceException();
        }
        
        if(!transactionDigest.isEventualEmpty()) {
            ByteBuffer serialized = transactionDigest.getSerializedEventuallyStagingAndClean();

            if (serialized!=null && useCompression) {
                serialized = StagingTable.Compress(serialized);
            }

            if (serialized != null) {
                MusicTxDigestId digestId = new MusicTxDigestId(MDBCUtils.generateUniqueKey(), -1);
                addEventualTxDigest(digestId, serialized);
            }
        }
        
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


    public List<Range> getRanges(Row newRow){
        List<Range> partitions = new ArrayList<>();
        Set<String> tables = newRow.getSet("keys",String.class);
        for (String table:tables){
            partitions.add(new Range(table));
        }
        return partitions;
    }

    static public MusicRangeInformationRow getMRIRowFromCassandraRow(Row newRow){
        UUID partitionIndex = newRow.getUUID("rangeid");
        List<TupleValue> log = newRow.getList("txredolog",TupleValue.class);
        List<MusicTxDigestId> digestIds = new ArrayList<>();
        int index=0;
        for(TupleValue t: log){
            //final String tableName = t.getString(0);
            final UUID id = t.getUUID(1);
            digestIds.add(new MusicTxDigestId(partitionIndex,id,index++));
        }
        List<Range> partitions = new ArrayList<>();
        Set<String> tables = newRow.getSet("keys",String.class);
        for (String table:tables){
            partitions.add(new Range(table));
        }
        return new MusicRangeInformationRow(partitionIndex, new DatabasePartition(partitions, partitionIndex, ""),
            digestIds, newRow.getString("ownerid"),newRow.getString("metricprocessid"),
            newRow.getBool("islatest"));
    }

    public RangeDependency getRangeDependenciesFromCassandraRow(Row newRow){
        if(newRow == null) return null;
        String base = newRow.getString("range");
        Range baseRange = new Range(base);
        Set<String> dependencies = newRow.getSet("dependencies", String.class);
        List<Range> rangeDependencies = new ArrayList<>();
        for(String dependency: dependencies){
            rangeDependencies.add(new Range(dependency));
        }
        return new RangeDependency(baseRange,rangeDependencies);
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

    @Override
    public RangeDependency getMusicRangeDependency(Range baseRange) throws MDBCServiceException {
        String cql = String.format("SELECT * FROM %s.%s WHERE range = ?;", music_ns, musicRangeDependencyTableName);
        PreparedQueryObject pQueryObject = new PreparedQueryObject();
        pQueryObject.appendQueryString(cql);
        pQueryObject.addValue(baseRange.getTable());
        Row newRow;
        //TODO Change this when music fix the "." problem in the primary key
        final String table = baseRange.getTable();
        final String tableWithoutDot = table.replaceAll("\\.","");
        try {
            newRow = executeMusicLockedGet(music_ns, musicRangeDependencyTableName,pQueryObject,tableWithoutDot,null);
        } catch (MDBCServiceException e) {
            logger.error("Get operationt error: Failure to get row from MRI "+musicRangeInformationTableName+" trying for table "+tableWithoutDot);
            throw new MDBCServiceException("Initialization error:Failure to add new row to transaction information for table "+tableWithoutDot, e);
        }
        return getRangeDependenciesFromCassandraRow(newRow);
    }

    /**
     * This function creates the TransactionInformation table. It contain information related
     * to the transactions happening in a given partition.
     *   * The schema of the table is
     *      * Id, uiid.
     *      * Partition, uuid id of the partition
     *      * LatestApplied, int indicates which values from the redologtable wast the last to be applied to the data tables
     *      * Applied: boolean, indicates if all the values in this redo log table where already applied to data tables
     *      * Redo: list of uiids associated to the Redo Records Table
     *
     */
    public static void createMusicRangeInformationTable(String namespace, String tableName) throws MDBCServiceException {
        String priKey = "rangeid";
        StringBuilder fields = new StringBuilder();
        fields.append("rangeid uuid, ");
        fields.append("keys set<text>, ");
        fields.append("ownerid text, ");
        fields.append("islatest boolean, ");
        fields.append("metricprocessid text, ");
        //TODO: Frozen is only needed for old versions of cassandra, please update correspondingly
        fields.append("txredolog list<frozen<tuple<text,uuid>>> ");
        String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s));",
           namespace, tableName, fields, priKey);
        try {
            executeMusicWriteQuery(namespace,tableName,cql);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to create transaction information table");
            throw(e);
        }
    }


    @Override
    public DatabasePartition createMusicRangeInformation(MusicRangeInformationRow info) throws MDBCServiceException {
        DatabasePartition newPartition = info.getDBPartition();

        String fullyQualifiedMriKey = music_ns+"."+ musicRangeInformationTableName+"."+newPartition.getMRIIndex().toString();
        String lockId;
        int counter=0;
        do {
            lockId = createAndAssignLock(fullyQualifiedMriKey, newPartition);
            //TODO: fix this retry logic
        } while ((lockId ==null||lockId.isEmpty())&&(counter++<3));
        if (lockId == null || lockId.isEmpty()) {
            throw new MDBCServiceException("Error initializing music range information, error creating a lock for a new row" +
                "for key "+fullyQualifiedMriKey) ;
        }
        logger.info("Creating MRI " + newPartition.getMRIIndex() + " for ranges " + newPartition.getSnapshot());
        createEmptyMriRow(this.music_ns,this.musicRangeInformationTableName,newPartition.getMRIIndex(),info.getMetricProcessId(),
            lockId, newPartition.getSnapshot(),info.getIsLatest());
        info.setOwnerId(lockId);
        return newPartition;
    }

    @Override
    public void createMusicRangeDependency(RangeDependency rangeAndDependencies) throws MDBCServiceException {
        StringBuilder insert = new StringBuilder("INSERT INTO ")
                .append(this.music_ns)
                .append('.')
                .append(this.musicRangeDependencyTableName)
                .append(" (range,dependencies) VALUES ")
                .append("(")
                .append(rangeAndDependencies.getRange().getTable())
                .append(",{");
        boolean first=true;
        for (Range r: rangeAndDependencies.dependentRanges()) {
            if(first){ first=false; }
            else {
                insert.append(',');
            }
            insert.append("'").append(r.toString()).append("'");
        }
        insert.append("};");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(insert.toString());
        MusicCore.eventualPut(query);
    }


    private UUID createEmptyMriRow(List<Range> rangesCopy) {
        //TODO: THis should call one of the other createMRIRows
        UUID id = generateUniqueKey();
        StringBuilder insert = new StringBuilder("INSERT INTO ")
                .append(this.music_ns)
                .append('.')
                .append(this.musicRangeInformationTableName)
                .append(" (rangeid,keys,ownerid,metricprocessid,txredolog) VALUES ")
                .append("(")
                .append(id)
                .append(",{");
        boolean first=true;
        for (Range r: rangesCopy) {
            if(first){ first=false; }
            else {
                insert.append(',');
            }
            insert.append("'").append(r.toString()).append("'");
        }
        insert.append("},'")
        .append("")
        .append("','")
        .append("")
        .append("',[]);");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(insert.toString());
        MusicCore.eventualPut(query);
        return id;
    }
        
    
    /**
     * Creates a new empty MRI row
     * @param processId id of the process that is going to own initially this.
     * @return uuid associated to the new row
     */
    private UUID createEmptyMriRow(String processId, String lockId, List<Range> ranges)
        throws MDBCServiceException {
        UUID id = MDBCUtils.generateTimebasedUniqueKey();
        logger.info("Creating MRI "+ id + " for ranges " + ranges);
        return createEmptyMriRow(this.music_ns,this.musicRangeInformationTableName,id,processId,lockId,ranges,true);
    }

    /**
     * Creates a new empty MRI row
     * @param processId id of the process that is going to own initially this.
     * @return uuid associated to the new row
     */
    public static UUID createEmptyMriRow(String musicNamespace, String mriTableName, UUID id, String processId,
        String lockId, List<Range> ranges, boolean isLatest)
        throws MDBCServiceException{
        StringBuilder insert = new StringBuilder("INSERT INTO ")
            .append(musicNamespace)
            .append('.')
            .append(mriTableName)
            .append(" (rangeid,keys,ownerid,islatest,metricprocessid,txredolog) VALUES ")
            .append("(")
            .append(id)
            .append(",{");
        boolean first=true;
        for (Range r: ranges) {
            if(first){ first=false; }
            else {
                insert.append(',');
            }
            insert.append("'").append(r.toString()).append("'");
        }
        insert.append("},'")
            .append((lockId==null)?"":lockId)
            .append("',")
            .append(isLatest)
            .append(",'")
            .append(processId)
            .append("',[]);");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(insert.toString());
        try {
            executeMusicLockedPut(musicNamespace,mriTableName,id.toString(),query,lockId,null);
        } catch (MDBCServiceException e) {
            throw new MDBCServiceException("Initialization error:Failure to add new row to transaction information", e);
        }
        return id;
    }

    @Override
    public void appendToRedoLog(UUID MRIIndex,  String lockId, MusicTxDigestId newRecord) throws MDBCServiceException {
        logger.debug("Appending to redo log for partition " + MRIIndex + " txId=" + newRecord.transactionId);
        appendToRedoLog(music_ns,MRIIndex,newRecord.transactionId,lockId,musicTxDigestTableName,
            musicRangeInformationTableName);
    }

    public void appendToRedoLog(String musicNamespace, UUID MRIIndex, UUID transactionId, String lockId,
                                        String musicTxDigestTableName, String musicRangeInformationTableName)
        throws MDBCServiceException{
        PreparedQueryObject appendQuery = createAppendMtxdIndexToMriQuery(musicRangeInformationTableName, MRIIndex,
            musicTxDigestTableName, transactionId);
        ReturnType returnType = MusicCore.criticalPut(musicNamespace, musicRangeInformationTableName, MRIIndex.toString(),
            appendQuery, lockId, null);
        //returnType.getExecutionInfo()
        if (returnType.getResult().compareTo(ResultType.SUCCESS) != 0) {
            logger.error(EELFLoggerDelegate.errorLogger, "Error when executing append operation with return type: "+returnType.getMessage());
            throw new MDBCServiceException("Error when executing append operation with return type: "+returnType.getMessage());
        }
    }

    public void createMusicTxDigest() throws MDBCServiceException {
        createMusicTxDigest(this.musicTxDigestTableName,this.music_ns,-1);
    }
    
    public void createMusicEventualTxDigest() throws MDBCServiceException {
        createMusicEventualTxDigest(musicEventualTxDigestTableName,music_ns,-1);
    }


    /**
     * This function creates the MusicEveTxDigest table. It contain information related to each eventual transaction committed
     *  * LeaseId: id associated with the lease, text
     *  * LeaseCounter: transaction number under this lease, bigint \TODO this may need to be a varint later
     *  * TransactionDigest: text that contains all the changes in the transaction
     */
    public static void createMusicEventualTxDigest(String musicEventualTxDigestTableName, String musicNamespace, int musicTxDigestTableNumber) throws MDBCServiceException {
        String tableName = musicEventualTxDigestTableName;
        if (musicTxDigestTableNumber >= 0) {
            tableName = tableName +
                "-" +
                Integer.toString(musicTxDigestTableNumber);
        }
        String priKey = "txTimeId, year";
        StringBuilder fields = new StringBuilder();
        fields.append("txid uuid, ");
        fields.append("transactiondigest blob, ");
        fields.append("compressed boolean, ");
        fields.append("year int, ");
        fields.append("txTimeId TIMEUUID ");//notice lack of ','
        String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s));", musicNamespace, tableName, fields, priKey);
        try {
            executeMusicWriteQuery(musicNamespace,tableName,cql);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to create eventual tx digest table");
            throw(e);
        }
    }
    
    
    /**
     * This function creates the MusicTxDigest table. It contain information related to each transaction committed
     *  * LeaseId: id associated with the lease, text
     *  * LeaseCounter: transaction number under this lease, bigint \TODO this may need to be a varint later
     *  * TransactionDigest: text that contains all the changes in the transaction
     */
    public static void createMusicTxDigest(String musicTxDigestTableName, String musicNamespace, int musicTxDigestTableNumber) throws MDBCServiceException {
        String tableName = musicTxDigestTableName;
        if (musicTxDigestTableNumber >= 0) {
            tableName = tableName +
                "-" +
                Integer.toString(musicTxDigestTableNumber);
        }
        String priKey = "txid";
        StringBuilder fields = new StringBuilder();
        fields.append("txid uuid, ");
        fields.append("compressed boolean, ");
        fields.append("transactiondigest blob ");//notice lack of ','
        String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s));", musicNamespace,
            tableName, fields, priKey);
        try {
            executeMusicWriteQuery(musicNamespace,tableName,cql);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to create redo records table");
            throw(e);
        }
    }

    public static void createMusicRangeDependencyTable(String musicNamespace,String musicRangeDependencyTableName)
        throws MDBCServiceException {
        String tableName = musicRangeDependencyTableName;
        String priKey = "range";
        StringBuilder fields = new StringBuilder();
        fields.append("range text, ");
        fields.append("dependencies set<text> ");//notice lack of ','
        String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s));", musicNamespace, tableName,
            fields, priKey);
        try {
            executeMusicWriteQuery(musicNamespace,tableName,cql);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to create redo records table");
            throw(e);
        }
    }

    /**
     * Writes the transaction history to the txDigest
     */
    @Override
    public void addTxDigest(MusicTxDigestId newId, ByteBuffer transactionDigest) throws MDBCServiceException {
        //\TODO: Save Prepared query to history
        addTxDigest(newId.transactionId,transactionDigest);
    }

    private void addTxDigest(UUID digestId, ByteBuffer transactionDigest) throws MDBCServiceException{
        PreparedQueryObject query = new PreparedQueryObject();
        String cql = String.format("INSERT INTO %s.%s (txid,transactiondigest,compressed ) VALUES (?,?,?);",this.music_ns,
            this.musicTxDigestTableName);
        query.appendQueryString(cql);
        query.addValue(digestId);
        query.addValue(transactionDigest);
        query.addValue(useCompression);
        //\TODO check if I am not shooting on my own foot
        try {
            MusicCore.nonKeyRelatedPut(query,"critical");
        } catch (MusicServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger, "Transaction Digest serialization was invalid for digest id "+digestId.toString()+ "with error "+e.getErrorMessage());
            throw new MDBCServiceException("Transaction Digest serialization for digest id "+digestId.toString(), e);
        }
    }
    
    /**
     * Writes the Eventual transaction history to the evetxDigest
     */
    @Override
    public void addEventualTxDigest(MusicTxDigestId newId, ByteBuffer transactionDigest) throws MDBCServiceException {
        //createTxDigestRow(music_ns,musicTxDigestTable,newId,transactionDigest);
        PreparedQueryObject query = new PreparedQueryObject();
        int year = java.util.Calendar.getInstance().get(java.util.Calendar.YEAR);
        
        String cql = String.format("INSERT INTO %s.%s (txid,transactiondigest,compressed,year,txTimeId ) VALUES (?,?,?,?,now());",this.music_ns,
                this.musicEventualTxDigestTableName);
            query.appendQueryString(cql);
            query.addValue( newId.transactionId);
            query.addValue(transactionDigest);
            query.addValue(useCompression);
            query.addValue(year);
           // query.appendQueryString(cqlQuery);
        //\TODO check if I am not shooting on my own foot
        try {
            MusicCore.nonKeyRelatedPut(query,"critical");
        } catch (MusicServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger, "Transaction Digest serialization was invalid for commit "+newId.transactionId.toString()+ "with error "+e.getErrorMessage());
            throw new MDBCServiceException("Transaction Digest serialization for commit "+newId.transactionId.toString(), e);
        }
    }


    @Override
    public StagingTable getTxDigest(MusicTxDigestId id) throws MDBCServiceException {
        String cql = String.format("SELECT * FROM %s.%s WHERE txid = ?;", music_ns, musicTxDigestTableName);
        PreparedQueryObject pQueryObject = new PreparedQueryObject();
        pQueryObject.appendQueryString(cql);
        pQueryObject.addValue(id.transactionId);
        Row newRow;
        try {
            newRow = executeMusicUnlockedQuorumGet(pQueryObject);
        } catch (MDBCServiceException e) {
            logger.error("Get operation error: Failure to get row from txdigesttable with id:"+id.transactionId);
            throw new MDBCServiceException("Initialization error:Failure to add new row to transaction information", e);
        }
        ByteBuffer digest = newRow.getBytes("transactiondigest");
        Boolean compressed = newRow.getBool("compressed");
        StagingTable changes;
        try {
            if(compressed){
                digest = StagingTable.Decompress(digest);
            }
            changes = new StagingTable(digest);
        } catch (MDBCServiceException e) {
            logger.error("Deserializng digest failed with an exception:"+e.getErrorMessage());
            throw e;
        }
        return changes;
    }

    @Override
    public LinkedHashMap<UUID, StagingTable> getEveTxDigest(String nodeName) throws MDBCServiceException {
        int year = java.util.Calendar.getInstance().get(java.util.Calendar.YEAR);
        StringBuffer yearSb = new StringBuffer();
        String sep = "";
        for (int y=2019; y<=year; y++) {
            yearSb.append(sep);
            yearSb.append(y);
            sep = ",";
        }

        StagingTable changes;
        String cql;
        LinkedHashMap<UUID, StagingTable> ecDigestInformation = new LinkedHashMap<>();
        UUID musicevetxdigestNodeinfoTimeID = getTxTimeIdFromNodeInfo(nodeName);
        PreparedQueryObject pQueryObject = new PreparedQueryObject();

        if (musicevetxdigestNodeinfoTimeID != null) {
            // this will fetch only few records based on the time-stamp condition.
            cql = String.format("SELECT * FROM %s.%s WHERE year in (%s) AND txtimeid > ? LIMIT 10 ALLOW FILTERING;", music_ns, this.musicEventualTxDigestTableName, yearSb.toString());
            pQueryObject.appendQueryString(cql);
            pQueryObject.addValue(musicevetxdigestNodeinfoTimeID);
        } else {
            // This is going to Fetch all the Transactiondigest records from the musicevetxdigest table.
            cql = String.format("SELECT * FROM %s.%s WHERE year in (%s) LIMIT 10 ALLOW FILTERING;", music_ns, this.musicEventualTxDigestTableName, yearSb.toString());
            pQueryObject.appendQueryString(cql);
        }

        // I need to get a ResultSet of all the records and give each row to the below HashMap.
        ResultSet rs = executeMusicRead(pQueryObject);
        while (!rs.isExhausted()) {
            Row row = rs.one();
            ByteBuffer digest = row.getBytes("transactiondigest");
            Boolean compressed = row.getBool("compressed");
            //String txTimeId = row.getString("txtimeid"); //???
            UUID txTimeId = row.getUUID("txtimeid");

            try {
                if(compressed){
                    digest=StagingTable.Decompress(digest);
                }
                changes = new StagingTable(digest);
            } catch (MDBCServiceException e) {
                logger.error("Deserializng digest failed: "+e.getErrorMessage());
                throw e;
            }
            ecDigestInformation.put(txTimeId, changes);
        }
        return ecDigestInformation;
    }

    ResultSet getAllMriCassandraRows() throws MDBCServiceException {
        StringBuilder cqlOperation = new StringBuilder();
        cqlOperation.append("SELECT * FROM ")
            .append(music_ns)
            .append(".")
            .append(musicRangeInformationTableName);
        return executeMusicRead(cqlOperation.toString());
    }

    @Override
    public List<MusicRangeInformationRow> getAllMriRows() throws MDBCServiceException{
        List<MusicRangeInformationRow> rows = new ArrayList<>();
        final ResultSet mriCassandraRows = getAllMriCassandraRows();
        while (!mriCassandraRows.isExhausted()) {
            Row musicRow = mriCassandraRows.one();
            final MusicRangeInformationRow mriRow = getMRIRowFromCassandraRow(musicRow);
            rows.add(mriRow);
        }
        return rows;
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
        final ResultSet musicResults = getAllMriCassandraRows();
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
                        else{
                            r.addOldRow(mriRow);
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

    private void unlockKeyInMusic(String table, String key, String lockref) throws MDBCServiceException {
        String fullyQualifiedKey= music_ns+"."+ table+"."+key;
        try {
            MusicCore.voluntaryReleaseLock(fullyQualifiedKey,lockref);
        } catch (MusicLockingException e) {
            throw new MDBCServiceException(e.getMessage(), e);
        }
    }

    @Override
    public void releaseLocks(Map<UUID,LockResult> newLocks) throws MDBCServiceException{
        for(Map.Entry<UUID,LockResult> lock : newLocks.entrySet()) {
            unlockKeyInMusic(musicRangeInformationTableName, lock.getKey().toString(), lock.getValue().getLockId());
        }
    }

    private void releaseLocks(List<MusicRangeInformationRow> changed, Map<UUID,LockResult> newLocks) throws MDBCServiceException{
        
        for(MusicRangeInformationRow r : changed) {
            LockResult lock = newLocks.get(r.getPartitionIndex());
            if(lock == null)
                continue;
            unlockKeyInMusic(musicRangeInformationTableName, r.getPartitionIndex().toString(),
                lock.getLockId());
            newLocks.remove(r.getPartitionIndex());
        }
    }

    private void releaseAllLocksExcept(UUID finalRow, Map<UUID,LockResult> newLocks) throws MDBCServiceException {
        Set<UUID> toErase = new HashSet<>();
        for(Map.Entry<UUID,LockResult> lock : newLocks.entrySet()) {
            UUID id = lock.getKey();
            if(id!=finalRow){
                unlockKeyInMusic(musicRangeInformationTableName, id.toString(), lock.getValue().getLockId());
                toErase.add(id);
            }
        }
        for(UUID id:toErase){
           newLocks.remove(id);
        }
    }

    /**
     * Get a list of ranges and their range dependencies
     * @param range
     * @return
     * @throws MDBCServiceException
     */
    @Override
    public List<Range> getRangeDependencies(List<Range> range) throws MDBCServiceException{
        Set<Range> extendedRange = new HashSet<>();
        for(Range r: range){
            extendedRange.add(r);
            RangeDependency dependencies = getMusicRangeDependency(r);
            if(dependencies!=null){
               extendedRange.addAll(dependencies.dependentRanges());
            }
        }
        return new ArrayList<>(extendedRange);
    }

    @Override
    public LockResult requestLock(LockRequest request) throws MDBCServiceException{
        String fullyQualifiedKey= music_ns+"."+ musicRangeInformationTableName + "." + request.getId();
        boolean isWrite = (request.getLockType()==SQLOperationType.WRITE);
        String lockId = MusicCore.createLockReference(fullyQualifiedKey, isWrite);
        ReturnType lockReturn = acquireLock(fullyQualifiedKey,lockId);
        if(lockReturn.getResult() == ResultType.FAILURE) {
            //\TODO Improve the exponential backoff
            int n = request.getNumOfAttempts();
            int low = 1;
            int high = 1000;
            Random r = new Random();
            long backOffTimems = ((int) Math.round(Math.pow(2, n)) * 1000)
                    + (r.nextInt(high - low) + low);
            return new LockResult(false, backOffTimems);
        }
        return new LockResult(true, request.getId(),lockId,true,null);
    }

    /**
     *  fixes the DAG in case the previous owner failed while trying to own the row
     * @param latestDag
     * @param rows
     * @param ranges
     * @param locks
     * @throws MDBCServiceException
     */
    private void recoverFromFailureAndUpdateDag(Dag latestDag,List<MusicRangeInformationRow> rows,List<Range> ranges,
                                                Map<UUID,LockResult> locks) throws MDBCServiceException{
        Pair<List<Range>,Set<DagNode>> rangesAndDependents = latestDag.getIncompleteRangesAndDependents();
        if(rangesAndDependents.getKey()==null || rangesAndDependents.getKey().size()==0 ||
            rangesAndDependents.getValue()==null || rangesAndDependents.getValue().size() == 0){
            return;
        }
        MusicRangeInformationRow r = createAndAssignLock(rangesAndDependents.getKey());
        locks.put(r.getPartitionIndex(),new LockResult(r.getPartitionIndex(),r.getOwnerId(),true,rangesAndDependents.getKey()));
        latestDag.addNewNode(r,new ArrayList<>(rangesAndDependents.getValue()));
    }


    private List<MusicRangeInformationRow> setReadOnlyAnyDoubleRow(Dag latestDag,List<MusicRangeInformationRow> rows, Map<UUID,LockResult> locks)
        throws MDBCServiceException{
        List<MusicRangeInformationRow> returnInfo = new ArrayList<>();
        List<DagNode> toDisable = latestDag.getOldestDoubles();
        for(DagNode node : toDisable){
            changeIsLatestToMRI(node.getRow(),false,locks.get(node.getId()));
            latestDag.setIsLatest(node.getId(),false);
            returnInfo.add(node.getRow());
        }
        return returnInfo;
    }

    private MusicRangeInformationRow createAndAssignLock(List<Range> ranges) throws MDBCServiceException {
        UUID newUUID = MDBCUtils.generateTimebasedUniqueKey();
        DatabasePartition newPartition = new DatabasePartition(ranges,newUUID,null);
        MusicRangeInformationRow newRow = new MusicRangeInformationRow(newUUID,newPartition,new ArrayList<>(),
            null,getMyHostId(),true);
        createMusicRangeInformation(newRow);
        return newRow;
    }

    @Override
    public OwnershipReturn mergeLatestRowsIfNecessary(Dag extendedDag, List<MusicRangeInformationRow> latestRows,
            List<Range> ranges, Map<UUID, LockResult> locks, UUID ownershipId) throws MDBCServiceException {
        recoverFromFailureAndUpdateDag(extendedDag,latestRows,ranges,locks);

        if (latestRows.size()==1 && latestRows.get(0).getDBPartition().contains(ranges)) {
            //reuse current row if possible
            MusicRangeInformationRow row = latestRows.get(0);
            LockResult lockresult = locks.get(row.getPartitionIndex());
            if (lockresult!=null) {
                return new OwnershipReturn(ownershipId, lockresult.getLockId(), row.getPartitionIndex(), ranges, extendedDag);
            }
        }
        List<MusicRangeInformationRow> changed = setReadOnlyAnyDoubleRow(extendedDag, latestRows,locks);
        releaseLocks(changed, locks);
        MusicRangeInformationRow row = createAndAssignLock(ranges);
        latestRows.add(row);
        locks.put(row.getPartitionIndex(),new LockResult(row.getPartitionIndex(),row.getOwnerId(),true,ranges));
        extendedDag.addNewNodeWithSearch(row,ranges);
        Pair<List<Range>, Set<DagNode>> missing = extendedDag.getIncompleteRangesAndDependents();
        if(missing.getKey().size()!=0 && missing.getValue().size()!=0) {
            MusicRangeInformationRow newRow = createAndAssignLock(missing.getKey());
            latestRows.add(newRow);
            locks.put(newRow.getPartitionIndex(), new LockResult(newRow.getPartitionIndex(), newRow.getOwnerId(), true,
                missing.getKey()));
            extendedDag.addNewNode(newRow, new ArrayList<>(missing.getValue()));
        }
        changed = setReadOnlyAnyDoubleRow(extendedDag, latestRows,locks);
        releaseLocks(changed,locks);
        releaseAllLocksExcept(row.getPartitionIndex(),locks);
        LockResult ownRow = locks.get(row.getPartitionIndex());
        return new OwnershipReturn(ownershipId, ownRow.getLockId(), ownRow.getIndex(),ranges,extendedDag);
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


    @Override
    public void relinquish(DatabasePartition partition) throws MDBCServiceException {
        String ownerId = partition.getLockId();
        String rangeId = partition.getMRIIndex().toString();
        if(ownerId==null||ownerId.isEmpty()||rangeId==null||rangeId.isEmpty()){
            return;
        }
        unlockKeyInMusic(musicRangeInformationTableName, rangeId, ownerId);
    }

    @Override
    public void relinquish(String ownerId, String rangeId) throws MDBCServiceException{
        if(ownerId==null||ownerId.isEmpty()||rangeId==null||rangeId.isEmpty()){
            return;
        }
        unlockKeyInMusic(musicRangeInformationTableName, rangeId, ownerId);
    }

    /**
     * This function is used to rate the number of times we relinquish at the end of a transaction
     * @return true if we should try to relinquish, else should avoid relinquishing in this iteration
     */
    private boolean canTryRelinquishing(){
        //\TODO: Fix this!!!! REALLY IMPORTANT TO BE FIX
        // This should actually have some mechanism to relinquish ownership
        return true;
    }

    @Override
    public void relinquishIfRequired(DatabasePartition partition) throws MDBCServiceException {
        if(!canTryRelinquishing() || !partition.isLocked()){
            return;
        }
        long lockQueueSize;
        try {
            String fullyQualifiedKey= music_ns+"."+ this.musicRangeInformationTableName+"."+partition.getMRIIndex().toString();
            lockQueueSize = MusicCore.getLockQueueSize(fullyQualifiedKey);
        } catch (MusicServiceException|MusicQueryException|MusicLockingException e) {
            logger.error("Error obtaining the lock queue size");
            throw new MDBCServiceException("Error obtaining lock queue size: " + e.getMessage(), e);
        }
        if(lockQueueSize> 1){
            //If there is any other node waiting, we just relinquish ownership
            try {
                relinquish(partition);
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
        if(lock != null && !lock.isEmpty()) {
            try {
                result = MusicCore.criticalGet(keyspace, table, primaryKey, cqlObject, lock);
            } catch (MusicServiceException e) {
                e.printStackTrace();
                throw new MDBCServiceException("Error executing critical get", e);
            }
        }
        else{
            try {
                result = MusicCore.atomicGet(keyspace,table,primaryKey,cqlObject);
            } catch (MusicServiceException|MusicLockingException|MusicQueryException e) {
                e.printStackTrace();
                throw new MDBCServiceException("Error executing atomic get", e);
            }
        }
        if(result==null){
            throw new MDBCServiceException("Error executing atomic get for primary key: "+primaryKey);
        }
        if(result.isExhausted()){
            return null;
        }
        return result.one();
    }

    private static Row executeMusicUnlockedQuorumGet(PreparedQueryObject cqlObject) throws MDBCServiceException{
        ResultSet result = MusicCore.quorumGet(cqlObject);
        if(result == null || result.isExhausted()){
            throw new MDBCServiceException("There is not a row that matches the query: ["+cqlObject.getQuery()+"]");
        }
        return result.one();
    }

    private static void executeMusicLockedPut(String namespace, String tableName,
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
    public void replayTransaction(StagingTable digest) throws MDBCServiceException{
        //\TODO: implement logic to move data from digests to Music Data Tables
        //throw new NotImplementedException("Error, replay transaction in music mixin needs to be implemented");
        return;
    }

    @Override
    public void deleteOldMriRows(Map<UUID, String> oldRowsAndLocks) throws MDBCServiceException {
        //\TODO Do this operations in parallel or combine in only query to cassandra
        for(Map.Entry<UUID,String> rows : oldRowsAndLocks.entrySet()){
            //\TODO handle music delete correctly so we can delete the other rows
            executeMusicLockedDelete(music_ns,musicRangeInformationTableName,rows.getKey().toString(),rows.getValue());
        }
    }
    
    @Override
    public void updateNodeInfoTableWithTxTimeIDKey(UUID txTimeID, String nodeName) throws MDBCServiceException{
        
           String cql = String.format("UPDATE %s.%s SET txtimeid = %s, txupdatedatetime = now() WHERE nodename = ?;", music_ns, this.musicNodeInfoTableName, txTimeID);
            PreparedQueryObject pQueryObject = new PreparedQueryObject();
            pQueryObject.appendQueryString(cql);
            pQueryObject.addValue(nodeName);
            
            ReturnType rt = MusicCore.eventualPut(pQueryObject);
            if(rt.getResult().getResult().toLowerCase().equals("failure")) {
                logger.error(EELFLoggerDelegate.errorLogger, "Failure while eventualPut...: "+rt.getMessage());
            }
            else logger.info("Successfully updated nodeinfo table with txtimeid value: " + txTimeID + " against the node:" + nodeName);
            
        
    }
    
    public void createMusicNodeInfoTable() throws MDBCServiceException {
        createMusicNodeInfoTable(musicNodeInfoTableName,music_ns,-1);
    }
    
    /**
     * This function creates the NodeInfo table. It contain information related
     * to the nodes along with the updated transactionDigest details.
     *   * The schema of the table is
     *      * nodeId, uuid. 
     *      * nodeName, text or varchar?? for now I am going ahead with "text".
     *      * createDateTime, TIMEUUID.
     *      * TxUpdateDateTime, TIMEUUID.
     *      * TxTimeID, TIMEUUID.
     *      * LastTxDigestID, uuid. (not needed as of now!!)
     */
    public static void createMusicNodeInfoTable(String musicNodeInfoTableName, String musicNamespace, int nodeInfoTableNumber) throws MDBCServiceException {
        String tableName = musicNodeInfoTableName;
        if(nodeInfoTableNumber >= 0) {
            tableName = tableName +
                "-" +
                Integer.toString(nodeInfoTableNumber);
        }

        String priKey = "nodename";
        StringBuilder fields = new StringBuilder();
        fields.append("nodename text, ");
        fields.append("createdatetime TIMEUUID, ");
        fields.append("txupdatedatetime TIMEUUID, ");
        fields.append("txtimeid TIMEUUID ");
        //fields.append("LastTxDigestID uuid ");// Not needed as of now!     
        
        String cql = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s));",
                musicNamespace,
                tableName,
                fields,
                priKey);
        
        try {
            executeMusicWriteQuery(musicNamespace,tableName,cql);
        } catch (MDBCServiceException e) {
            logger.error("Initialization error: Failure to create node information table");
            throw(e);
        }
    }
    
    public UUID getTxTimeIdFromNodeInfo(String nodeName) throws MDBCServiceException {
            // expecting NodeName from base-0.json file: which is : NJNode
            //String nodeName = MdbcServer.stateManager.getMdbcServerName(); 
            // this retrieves the NJNode row from Cassandra's NodeInfo table so that I can retrieve TimeStamp for further processing.
        String cql = String.format("SELECT txtimeid FROM %s.%s WHERE nodeName = ?;", music_ns, musicNodeInfoTableName);  
        PreparedQueryObject pQueryObject = new PreparedQueryObject();
        pQueryObject.appendQueryString(cql);
        pQueryObject.addValue(nodeName);
        Row newRow;
        try {
            newRow = executeMusicUnlockedQuorumGet(pQueryObject);
        } catch (MDBCServiceException e) {
            logger.error("Get operation error: Failure to get row from nodeinfo with nodename:"+nodeName);
            // TODO check underlying exception if no data and return empty string
            return null;
            //throw new MDBCServiceException("error:Failure to retrive nodeinfo details information", e);
        }
        
        return newRow.getUUID("txtimeid");

    }


    @Override
    public void deleteMriRow(MusicRangeInformationRow row) throws MDBCServiceException{
        String cql = String.format("DELETE FROM %s.%s WHERE rangeid = ?;", music_ns, musicRangeInformationTableName);
        PreparedQueryObject pQueryObject = new PreparedQueryObject();
        pQueryObject.appendQueryString(cql);
        pQueryObject.addValue(row.getPartitionIndex());
        ReturnType rt ;
        try {
            rt = MusicCore.atomicPut(music_ns, musicRangeInformationTableName, row.getPartitionIndex().toString(),
                pQueryObject, null);
        } catch (MusicLockingException|MusicQueryException|MusicServiceException e) {
            logger.error("Failure when deleting mri row");
            new MDBCServiceException("Error deleting mri row",e);
        }
    }

	public StateManager getStateManager() {
		return stateManager;
	}
    
    
}
