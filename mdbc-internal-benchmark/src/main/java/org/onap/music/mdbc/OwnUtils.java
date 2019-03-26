/*
 * ============LICENSE_START====================================================
 * org.onap.music.mdbc
 * =============================================================================
 * Copyright (C) 2019 AT&T Intellectual Property. All rights reserved.
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
package org.onap.music.mdbc;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.datastax.driver.core.SocketOptions;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.MusicLockState;
import org.onap.music.main.MusicCore;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.mdbc.configurations.NodeConfiguration;
import org.onap.music.mdbc.mixins.DBInterface;
import org.onap.music.mdbc.mixins.MusicInterface;
import org.onap.music.mdbc.mixins.MusicMixin;
import org.onap.music.mdbc.tables.MriRowComparator;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.MusicTxDigestId;
import org.onap.music.mdbc.tables.StagingTable;

public class OwnUtils {

    public static final int SQL_PORT= 3306;

    public static final String SQL_USER = "root";
    public static final String SQL_PASSWORD = "metriccluster";
    public static final String CASSANDRA_USER = "metric";
    public static final String CASSANDRA_PASSWORD = "metriccluster";
    public static final String KEYSPACE ="namespace";
    public static final String MRI_TABLE_NAME = "musicrangeinformation";
    public static final String MTD_TABLE_NAME = "musictxdigest";
    public static final String MDBC_SERVER_NAME = "name";
    public static final String DATABASE = "test";
    public static final String TABLE= "PERSONS";
    public static final int REPLICATION_FACTOR = 3;
    public static final String SQL_URL="jdbc:mariadb://localhost:"+OwnUtils.SQL_PORT;
    final static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS "+TABLE+" (\n" +
        "    PersonID int,\n" +
        "    Counter int,\n" +
        "    LastName varchar(255),\n" +
        "    FirstName varchar(255),\n" +
        "    Address varchar(255),\n" +
        "    City varchar(255),\n" +
        "    PRIMARY KEY(PersonID)"+
        ");";

    public static final String UPDATE = new StringBuilder()
        .append("UPDATE PERSONS ")
        .append("SET Counter = Counter + 1,")
        .append("City = 'Sandy Springs'")
        .append(";").toString();
    public static final String PURGE = "PURGE BINARY LOGS BEFORE '";
    public static final String DROP_TABLE = "DROP TABLE IF EXISTS " + TABLE + ";";
    private static Lock sessionLock = new ReentrantLock();
    private static Boolean sessionReady= false;
    private static Cluster cluster;
    private static Session session;

    private static String getPurgeCommand(){
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
       return PURGE+timeStamp+"';";
    }

    public static Session getSession(String CASSANDRA_HOST){
        sessionLock.lock();
        try {
            if(!sessionReady){
                SocketOptions options = new SocketOptions();
                options.setConnectTimeoutMillis(30000);
                options.setReadTimeoutMillis(300000);
                options.setTcpNoDelay(true);
                final Builder builder = Cluster.builder();
                builder.addContactPoint(CASSANDRA_HOST);
                builder.withCredentials(CASSANDRA_USER, CASSANDRA_PASSWORD);
                builder.withSocketOptions(options);
                cluster = builder.build();
                session=cluster.newSession();
            }
            return session;
            // access the resource protected by this lock
        } finally {
            sessionLock.unlock();
        }
    }

    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName("org.mariadb.jdbc.Driver");
        Properties connectionProps = new Properties();
        connectionProps.put("user", SQL_USER);
        connectionProps.put("password", SQL_PASSWORD);
        Connection connection = DriverManager.getConnection(SQL_URL+"/"+DATABASE, connectionProps);
        connection.setAutoCommit(false);
        return connection;
    }

    public static void purgeBinLogs(Connection conn) throws SQLException{
        final Statement dropStatement = conn.createStatement();
        dropStatement.execute(getPurgeCommand());
        dropStatement.close();
    }

    public static void dropTable(Connection conn) throws SQLException {
        final Statement dropStatement = conn.createStatement();
        dropStatement.execute(DROP_TABLE);
        dropStatement.close();
    }

    private static void createTable(Connection conn) throws SQLException {
        final Statement createStatement = conn.createStatement();
        createStatement.execute(CREATE_TABLE);
        createStatement.close();
    }

    public static void dropAndCreateTable(DBInterface dbMixin) throws SQLException {
        dbMixin.dropSQLTriggers(TABLE);
        dropTable(dbMixin.getSQLConnection());
        createTable(dbMixin.getSQLConnection());
        dbMixin.createSQLTriggers(TABLE);
    }

    public static void unlockRow(String keyspace, String mriTableName, DatabasePartition partition)
        throws MusicLockingException {
        String fullyQualifiedMriKey = keyspace+"."+ mriTableName+"."+partition.getMRIIndex().toString();
        MusicLockState musicLockState = MusicCore.voluntaryReleaseLock(fullyQualifiedMriKey, partition.getLockId());
    }

    public static DatabasePartition createBasicRow(Range range, MusicInterface mixin, String mdbcServerName)
        throws MDBCServiceException {
        List<Range> ranges = new ArrayList<>();
        ranges.add(range);
        final UUID uuid = MDBCUtils.generateTimebasedUniqueKey();
        DatabasePartition dbPartition = new DatabasePartition(ranges,uuid,null);
        MusicRangeInformationRow newRow = new MusicRangeInformationRow(uuid,dbPartition, new ArrayList<MusicTxDigestId>(), "",
            mdbcServerName, true);
        DatabasePartition partition=mixin.createMusicRangeInformation(newRow);
        return partition;
    }

    public static void initMriTable(MusicMixin musicMixin, Range range)
        throws MDBCServiceException, SQLException, MusicLockingException {
        final DatabasePartition partition = createBasicRow(range, musicMixin, MDBC_SERVER_NAME);
        unlockRow(KEYSPACE,MRI_TABLE_NAME,partition);
    }

    public static MusicRangeInformationRow getLastRow(MusicMixin musicMixin){
        List<MusicRangeInformationRow> allMriRows;
        try {
             allMriRows = musicMixin.getAllMriRows();
        } catch (MDBCServiceException e) {
            e.printStackTrace();
            System.exit(1);
            allMriRows=null;//Just to avoid IDE annoyance
        }
        Collections.sort(allMriRows, new MriRowComparator());
        MusicRangeInformationRow musicRangeInformationRow = allMriRows.get(allMriRows.size() - 1);
        return musicRangeInformationRow;
    }

    public static void deleteLastMriRow(MusicMixin musicMixin){
        MusicRangeInformationRow musicRangeInformationRow = getLastRow(musicMixin);
        try {
            musicMixin.deleteMriRow(musicRangeInformationRow);
        } catch (MDBCServiceException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void changeRows(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        Boolean execute = stmt.execute(UPDATE);
        conn.commit();
        stmt.close();
    }

    private static void addRowsToTable(int totalNumberOfRows, Connection testConnection) throws SQLException {
        for (int i = 0; i < totalNumberOfRows; i++) {
            final StringBuilder insertSQLBuilder = new StringBuilder()
                .append("INSERT INTO PERSONS VALUES (")
                .append(i)
                .append(", ")
                .append(0)
                .append(", '")
                .append("Last-")
                .append(i)
                .append("', '")
                .append("First-")
                .append(i)
                .append("', 'KACB', 'ATLANTA');");
            Statement stmt = testConnection.createStatement();
            Boolean execute = stmt.execute(insertSQLBuilder.toString());
            stmt.close();
        }
        testConnection.commit();
    }

    public static void createHistory(MdbcServerLogic meta,int rows, int updates,int transitions)
        throws SQLException {
        final StateManager stateManager = meta.getStateManager();
        String id = UUID.randomUUID().toString();
        Connection connection = stateManager.getConnection(id);
        createTable(connection);
        addRowsToTable(rows, connection);
        connection.close();
        stateManager.closeConnection(id);
        for(int mriRow=0;mriRow<transitions;mriRow++) {
            final String finalId = UUID.randomUUID().toString();
            connection = stateManager.getConnection(finalId);
            for(int depth=0;depth<updates;depth++){
                changeRows(connection);
            }
            connection.close();
            stateManager.closeConnection(finalId);
        }
    }

    public static MdbcServerLogic setupServer(String user, String password){
        MdbcServerLogic meta;
        NodeConfiguration config = new NodeConfiguration("","",null,OwnUtils.DATABASE,
            OwnUtils.MDBC_SERVER_NAME);
        //\TODO Add configuration file with Server Info
        Properties connectionProps = new Properties();
        connectionProps.setProperty("user", user);
        connectionProps.setProperty("password", password);
        connectionProps.setProperty(MusicMixin.KEY_MUSIC_RFACTOR,Integer.toString(OwnUtils.REPLICATION_FACTOR));
        connectionProps.setProperty(MusicMixin.KEY_MUSIC_NAMESPACE,OwnUtils.KEYSPACE);
        try {
            meta = new MdbcServerLogic(OwnUtils.SQL_URL,connectionProps,config);
        } catch (SQLException e) {
            e.printStackTrace();
            meta=null;
            System.exit(1);
        } catch (MDBCServiceException e) {
            e.printStackTrace();
            meta=null;
            System.exit(1);
        }
        return meta;
    }

    public static void dropAll(String ip){
        Session session=OwnUtils.getSession(ip);
        session.execute("DROP KEYSPACE IF EXISTS "+OwnUtils.KEYSPACE+";");
        try {
            Connection connection = OwnUtils.getConnection();
            OwnUtils.dropTable(connection);
            connection.close();
        } catch (SQLException|ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void initMri(MusicMixin musicMixin,Range range,MdbcServerLogic meta, int rows, int updates, int transitions){
        try {
            OwnUtils.initMriTable(musicMixin,range);
        } catch (MusicLockingException|SQLException|MDBCServiceException e) {
            e.printStackTrace();
            System.exit(1);
        }
        try {
            OwnUtils.createHistory(meta, rows, updates, transitions);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void hardcodedAddtransaction(int size){
        final UUID uuid = MDBCUtils.generateTimebasedUniqueKey();
        ByteBuffer serializedTransactionDigest = ByteBuffer.allocate(size);
        for(int i=0;i<size;i++){
            serializedTransactionDigest.put((byte)i);
        }
        PreparedQueryObject query = new PreparedQueryObject();
        String cql = String.format("INSERT INTO %s.%s (txid,transactiondigest,compressed ) VALUES (?,?,?);",KEYSPACE,
            MTD_TABLE_NAME);
        query.appendQueryString(cql);
        query.addValue(uuid);
        query.addValue(serializedTransactionDigest);
        query.addValue(false);
        //\TODO check if I am not shooting on my own foot
        try {
            MusicCore.nonKeyRelatedPut(query,"critical");
        } catch (MusicServiceException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void hardcodedAppendToRedo(UUID mriId, String lockId){
        final UUID uuid = MDBCUtils.generateTimebasedUniqueKey();
        PreparedQueryObject query = new PreparedQueryObject();
        StringBuilder appendBuilder = new StringBuilder();
        appendBuilder.append("UPDATE ")
            .append(KEYSPACE)
            .append(".")
            .append(MRI_TABLE_NAME)
            .append(" SET txredolog = txredolog +[('")
            .append(MTD_TABLE_NAME)
            .append("',")
            .append(uuid)
            .append(")] WHERE rangeid = ")
            .append(mriId)
            .append(";");
        query.appendQueryString(appendBuilder.toString());
        ReturnType returnType = MusicCore.criticalPut(KEYSPACE, MRI_TABLE_NAME, mriId.toString(),
            query, lockId, null);
        //returnType.getExecutionInfo()
        if (returnType.getResult().compareTo(ResultType.SUCCESS) != 0) {
            System.exit(1);
        }
    }

    public static void addTransactionDigest(StagingTable transactionDigest, MusicTxDigestId digestId, MusicMixin music){
        try {
            music.createAndAddTxDigest(transactionDigest, digestId.transactionId);
        } catch (MDBCServiceException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void appendToRedo(MusicMixin music, UUID MRIIndex, String lockId, MusicTxDigestId digestId){
        try {
            music.appendToRedoLog(KEYSPACE,MRIIndex,digestId.transactionId,lockId,OwnUtils.MTD_TABLE_NAME,
            OwnUtils.MRI_TABLE_NAME);
        } catch (MDBCServiceException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static MusicTxDigestId setupCommit(DatabasePartition partition, StagingTable transactionDigest){
        UUID mriIndex = partition.getMRIIndex();

        if(transactionDigest == null || transactionDigest.isEmpty()) {
            System.err.println("Transaction digest is empty");
            System.exit(1);
        }

        MusicTxDigestId digestId = new MusicTxDigestId(mriIndex, MDBCUtils.generateUniqueKey(), -1);
        return digestId;
    }
}
