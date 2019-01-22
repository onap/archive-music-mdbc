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
package org.onap.music.mdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.*;
import java.util.concurrent.Executor;

import org.apache.commons.lang3.NotImplementedException;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.exceptions.QueryException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.logging.format.AppMessages;
import org.onap.music.logging.format.ErrorSeverity;
import org.onap.music.logging.format.ErrorTypes;
import org.onap.music.mdbc.mixins.DBInterface;
import org.onap.music.mdbc.mixins.LockResult;
import org.onap.music.mdbc.mixins.MixinFactory;
import org.onap.music.mdbc.mixins.MusicInterface;
import org.onap.music.mdbc.mixins.MusicInterface.OwnershipReturn;
import org.onap.music.mdbc.ownership.Dag;
import org.onap.music.mdbc.ownership.DagNode;
import org.onap.music.mdbc.ownership.OwnershipAndCheckpoint;
import org.onap.music.mdbc.query.QueryProcessor;
import org.onap.music.mdbc.tables.MusicTxDigest;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.StagingTable;
import org.onap.music.mdbc.tables.TxCommitProgress;


/**
 * ProxyConnection is a proxy to a JDBC driver Connection.  It uses the MusicSqlManager to copy
 * data to and from Cassandra and the underlying JDBC database as needed.  It will notify the underlying
 * MusicSqlManager of any calls to <code>commit(), rollback()</code> or <code>setAutoCommit()</code>.
 * Otherwise it just forwards all requests to the underlying Connection of the 'real' database.
 *
 * @author Robert Eby
 */
public class MdbcConnection implements Connection {
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MdbcConnection.class);

    private final String id;            // This is the transaction id, assigned to this connection. There is no need to change the id, if connection is reused
    private final Connection jdbcConn;      // the JDBC Connection to the actual underlying database
    private final MusicInterface mi;
    private final TxCommitProgress progressKeeper;
    private final DBInterface dbi;
    private final HashMap<Range,StagingTable> transactionDigest;
    private final Set<String> table_set;
    private final StateManager statemanager;
    private DatabasePartition partition;

    public MdbcConnection(String id, String url, Connection c, Properties info, MusicInterface mi,
            TxCommitProgress progressKeeper, DatabasePartition partition, StateManager statemanager) throws MDBCServiceException {
        this.id = id;
        this.table_set = Collections.synchronizedSet(new HashSet<String>());
        this.transactionDigest = new HashMap<Range,StagingTable>();

        if (c == null) {
            throw new MDBCServiceException("Connection is null");
        }
        this.jdbcConn = c;
        info.putAll(MDBCUtils.getMdbcProperties());
        String mixinDb  = info.getProperty(Configuration.KEY_DB_MIXIN_NAME, Configuration.DB_MIXIN_DEFAULT);
        this.dbi       = MixinFactory.createDBInterface(mixinDb, mi, url, jdbcConn, info);
        this.mi        = mi;
        try {
            this.setAutoCommit(c.getAutoCommit());
        } catch (SQLException e) {
            logger.error("Failure in autocommit");
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
        }

        // Verify the tables in MUSIC match the tables in the database
        // and create triggers on any tables that need them
        try {
            this.synchronizeTables();
        } catch (QueryException e) {
            logger.error("Error syncrhonizing tables");
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
        }
        this.progressKeeper = progressKeeper;
        this.partition = partition;
        this.statemanager = statemanager;

        logger.debug("Mdbc connection created with id: "+id);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        logger.error(EELFLoggerDelegate.errorLogger, "proxyconn unwrap: " + iface.getName());
        return jdbcConn.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        logger.error(EELFLoggerDelegate.errorLogger, "proxystatement iswrapperfor: " + iface.getName());
        return jdbcConn.isWrapperFor(iface);
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new MdbcCallableStatement(jdbcConn.createStatement(), this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        //TODO: grab the sql call from here and all the other preparestatement calls
        return new MdbcPreparedStatement(jdbcConn.prepareStatement(sql), sql, this);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return new MdbcCallableStatement(jdbcConn.prepareCall(sql), this);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return jdbcConn.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        boolean b = jdbcConn.getAutoCommit();
        if (b != autoCommit) {
            if(progressKeeper!=null) progressKeeper.commitRequested(id);
            logger.debug(EELFLoggerDelegate.applicationLogger,"autocommit changed to "+b);
            if (b) {
                // My reading is that turning autoCOmmit ON should automatically commit any outstanding transaction
                if(id == null || id.isEmpty()) {
                    logger.error(EELFLoggerDelegate.errorLogger, "Connection ID is null",AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
                    throw new SQLException("tx id is null");
                }
                try {
                    mi.commitLog(partition, statemanager.getEventualRanges(), transactionDigest, id, progressKeeper);
                } catch (MDBCServiceException e) {
                    // TODO Auto-generated catch block
                    logger.error("Cannot commit log to music" + e.getStackTrace());
                    throw new SQLException(e.getMessage(), e);
                }
            }
            if(progressKeeper!=null) {
                progressKeeper.setMusicDone(id);
            }
            jdbcConn.setAutoCommit(autoCommit);
            if(progressKeeper!=null) {
                progressKeeper.setSQLDone(id);
            }
            if(progressKeeper!=null&&progressKeeper.isComplete(id)){
                progressKeeper.reinitializeTxProgress(id);
            }
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return jdbcConn.getAutoCommit();
    }

    /**
     * Perform a commit, as requested by the JDBC driver.  If any row updates have been delayed,
     * they are performed now and copied into MUSIC.
     * @throws SQLException
     */
    @Override
    public void commit() throws SQLException {
        if(progressKeeper.isComplete(id)) {
            return;
        }
        if(progressKeeper != null) {
            progressKeeper.commitRequested(id);
        }

        try {
            logger.debug(EELFLoggerDelegate.applicationLogger, " commit ");
            // transaction was committed -- add all the updates into the REDO-Log in MUSIC
            mi.commitLog(partition, statemanager.getEventualRanges(), transactionDigest, id, progressKeeper);
        } catch (MDBCServiceException e) {
            //If the commit fail, then a new commitId should be used
            logger.error(EELFLoggerDelegate.errorLogger, "Commit to music failed", AppMessages.UNKNOWNERROR, ErrorTypes.UNKNOWN, ErrorSeverity.FATAL);
            throw new SQLException("Failure commiting to MUSIC", e);
        }

        if(progressKeeper != null) {
            progressKeeper.setMusicDone(id);
        }

        jdbcConn.commit();

        if(progressKeeper != null) {
            progressKeeper.setSQLDone(id);
        }
        //MusicMixin.releaseZKLocks(MusicMixin.currentLockMap.get(getConnID()));
        if(progressKeeper.isComplete(id)){
            progressKeeper.reinitializeTxProgress(id);
        }

        //\TODO try to execute outside of the critical path of commit
        try {
            if(partition != null)
                relinquishIfRequired(partition);
        } catch (MDBCServiceException e) {
            logger.warn("Error trying to relinquish: "+partition.toString());
        }
    }

    /**
     * Perform a rollback, as requested by the JDBC driver.  If any row updates have been delayed,
     * they are discarded.
     */
    @Override
    public void rollback() throws SQLException {
        logger.debug(EELFLoggerDelegate.applicationLogger, "Rollback");;
        transactionDigest.clear();
        jdbcConn.rollback();
        progressKeeper.reinitializeTxProgress(id);
    }

    /**
     * Close this MdbcConnection.
     */
    @Override
    public void close() throws SQLException {
        logger.debug("Closing mdbc connection with id:"+id);
        if (dbi != null) {
            dbi.close();
        }
        if (jdbcConn != null && !jdbcConn.isClosed()) {
            logger.debug("Closing jdbc from mdbc with id:"+id);
            jdbcConn.close();
            logger.debug("Connection was closed for id:" + id);
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return jdbcConn.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return jdbcConn.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        jdbcConn.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return jdbcConn.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        jdbcConn.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return jdbcConn.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        jdbcConn.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return jdbcConn.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return jdbcConn.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        jdbcConn.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new MdbcCallableStatement(jdbcConn.createStatement(resultSetType, resultSetConcurrency), this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        return new MdbcCallableStatement(jdbcConn.prepareStatement(sql, resultSetType, resultSetConcurrency), sql, this);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new MdbcCallableStatement(jdbcConn.prepareCall(sql, resultSetType, resultSetConcurrency), this);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return jdbcConn.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        jdbcConn.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        jdbcConn.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return jdbcConn.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return jdbcConn.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return jdbcConn.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        jdbcConn.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        jdbcConn.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        return new MdbcCallableStatement(jdbcConn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability), this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        return new MdbcCallableStatement(jdbcConn.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability), sql, this);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        return new MdbcCallableStatement(jdbcConn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability), this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new MdbcPreparedStatement(jdbcConn.prepareStatement(sql, autoGeneratedKeys), sql, this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return new MdbcPreparedStatement(jdbcConn.prepareStatement(sql, columnIndexes), sql, this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return new MdbcPreparedStatement(jdbcConn.prepareStatement(sql, columnNames), sql, this);
    }

    @Override
    public Clob createClob() throws SQLException {
        return jdbcConn.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return jdbcConn.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return jdbcConn.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return jdbcConn.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return jdbcConn.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        jdbcConn.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        jdbcConn.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return jdbcConn.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return jdbcConn.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return jdbcConn.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return jdbcConn.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        jdbcConn.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return jdbcConn.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        jdbcConn.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        jdbcConn.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return jdbcConn.getNetworkTimeout();
    }

    /**
     * Code to be run within the DB driver before a SQL statement is executed.  This is where tables
     * can be synchronized before a SELECT, for those databases that do not support SELECT triggers.
     * @param sql the SQL statement that is about to be executed
     */
    public void preStatementHook(final String sql) throws MDBCServiceException {
        //TODO: verify ownership of keys here
        //Parse tables from the sql query
        Map<String, List<String>> tableToInstruction = QueryProcessor.extractTableFromQuery(sql);
        //Check ownership of keys
        List<Range> queryTables = MDBCUtils.getTables(tableToInstruction);
        if(this.partition!=null ){
            List<Range> snapshot = this.partition.getSnapshot();
            if(snapshot!=null){
                queryTables.addAll(snapshot);
            }
        }
        // filter out ranges that fall under Eventually consistent
        // category as these tables do not need ownership
        List<Range> scQueryTables = filterEveTables( queryTables);
        DatabasePartition tempPartition = own(scQueryTables);
        if(tempPartition!=null && tempPartition != partition) {
            this.partition.updateDatabasePartition(tempPartition);
        }
      dbi.preStatementHook(sql);
    }


    private List<Range> filterEveTables(List<Range> queryTables) {
        queryTables.removeAll(statemanager.getEventualRanges());
        return queryTables;
    }

    /**
     * Code to be run within the DB driver after a SQL statement has been executed.  This is where remote
     * statement actions can be copied back to Cassandra/MUSIC.
     * @param sql the SQL statement that was executed
     */
    public void postStatementHook(String sql) {
        dbi.postStatementHook(sql, transactionDigest);
    }

    /**
     * Synchronize the list of tables in SQL with the list in MUSIC. This function should be called when the
     * proxy first starts, and whenever there is the possibility that tables were created or dropped.  It is synchronized
     * in order to prevent multiple threads from running this code in parallel.
     */
    public void synchronizeTables() throws QueryException {
        Set<String> set1 = dbi.getSQLTableSet();    // set of tables in the database
        logger.debug(EELFLoggerDelegate.applicationLogger, "synchronizing tables:" + set1);
        for (String tableName : set1) {
            // This map will be filled in if this table was previously discovered
            tableName = tableName.toUpperCase();
            if (!table_set.contains(tableName) && !dbi.getReservedTblNames().contains(tableName)) {
                logger.info(EELFLoggerDelegate.applicationLogger, "New table discovered: "+tableName);
                try {
                    TableInfo ti = dbi.getTableInfo(tableName);
                    mi.initializeMusicForTable(ti,tableName);
                    //\TODO Verify if table info can be modify in the previous step, if not this step can be deleted
                    ti = dbi.getTableInfo(tableName);
                    mi.createDirtyRowTable(ti,tableName);
                    dbi.createSQLTriggers(tableName);
                    table_set.add(tableName);
                    dbi.synchronizeData(tableName);
                    logger.debug(EELFLoggerDelegate.applicationLogger, "synchronized tables:" +
                        table_set.size() + "/" + set1.size() + "tables uploaded");
                } catch (Exception e) {
                    logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
                    //logger.error(EELFLoggerDelegate.errorLogger, "Exception synchronizeTables: "+e);
                    throw new QueryException(e);
                }
            }
        }
    }

    public DBInterface getDBInterface() {
        return this.dbi;
    }

    private DatabasePartition own(List<Range> ranges) throws MDBCServiceException {
        if(ranges==null||ranges.isEmpty()){
            return null;
        }
        DatabasePartition newPartition = null;
        OwnershipAndCheckpoint ownAndCheck = mi.getOwnAndCheck();
        UUID ownOpId = MDBCUtils.generateTimebasedUniqueKey();
        try {
            final OwnershipReturn ownershipReturn = mi.own(ranges, partition, ownOpId);
            if(ownershipReturn==null){
                return null;
            }
            Dag dag = ownershipReturn.getDag();
            if(dag!=null) {
                DagNode node = dag.getNode(ownershipReturn.getRangeId());
                MusicRangeInformationRow row = node.getRow();
                Map<MusicRangeInformationRow, LockResult> lock = new HashMap<>();
                lock.put(row, new LockResult(row.getPartitionIndex(), ownershipReturn.getOwnerId(), true, ranges));
                ownAndCheck.checkpoint(this.mi, this.dbi, dag, ranges, lock, ownershipReturn.getOwnershipId());
                newPartition = new DatabasePartition(ownershipReturn.getRanges(), ownershipReturn.getRangeId(),
                    ownershipReturn.getOwnerId());
            }
        }
        finally{
            ownAndCheck.stopOwnershipTimeoutClock(ownOpId);
        }
        return newPartition;
    }

    public void relinquishIfRequired(DatabasePartition partition) throws MDBCServiceException {
        mi.relinquishIfRequired(partition);
    }

}
