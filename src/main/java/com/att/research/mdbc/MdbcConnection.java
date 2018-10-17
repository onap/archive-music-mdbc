package com.att.research.mdbc;

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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.att.research.exceptions.MDBCServiceException;
import com.att.research.exceptions.QueryException;
import com.att.research.logging.EELFLoggerDelegate;
import com.att.research.logging.format.AppMessages;
import com.att.research.logging.format.ErrorSeverity;
import com.att.research.logging.format.ErrorTypes;
import com.att.research.mdbc.mixins.MusicInterface;
import com.att.research.mdbc.tables.TxCommitProgress;


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
	
	private final String id;			// This is the transaction id, assigned to this connection. There is no need to change the id, if connection is reused
	private final Connection conn;		// the JDBC Connection to the actual underlying database
	private final MusicSqlManager mgr;	// there should be one MusicSqlManager in use per Connection
	private final TxCommitProgress progressKeeper;
	private final DatabasePartition partition;

	public MdbcConnection(String id, String url, Connection c, Properties info, MusicInterface mi, TxCommitProgress progressKeeper, DatabasePartition partition) throws MDBCServiceException {
		this.id = id;
		if (c == null) {
			throw new MDBCServiceException("Connection is null");
		}
		this.conn = c;
		try {
			this.mgr = new MusicSqlManager(url, c, info, mi);
		} catch (MDBCServiceException e) {
		    logger.error("Failure in creating Music SQL Manager");
			logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw e;
		}
		try {
			this.mgr.setAutoCommit(c.getAutoCommit(),null,null,null);
		} catch (SQLException e) {
            logger.error("Failure in autocommit");
			logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
		}

		// Verify the tables in MUSIC match the tables in the database
		// and create triggers on any tables that need them
		//mgr.synchronizeTableData();
		if ( mgr != null ) try {
			mgr.synchronizeTables();
		} catch (QueryException e) {
		    logger.error("Error syncrhonizing tables");
			logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
		}
		else {
			logger.error(EELFLoggerDelegate.errorLogger, "MusicSqlManager was not correctly created", AppMessages.UNKNOWNERROR, ErrorTypes.UNKNOWN, ErrorSeverity.FATAL);
			throw new MDBCServiceException("Music SQL Manager object is null or invalid");
		}
		this.progressKeeper = progressKeeper;
		this.partition = partition;
        logger.debug("Mdbc connection created with id: "+id);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		logger.error(EELFLoggerDelegate.errorLogger, "proxyconn unwrap: " + iface.getName());
		return conn.unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		logger.error(EELFLoggerDelegate.errorLogger, "proxystatement iswrapperfor: " + iface.getName());
		return conn.isWrapperFor(iface);
	}

	@Override
	public Statement createStatement() throws SQLException {
		return new MdbcCallableStatement(conn.createStatement(), mgr);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		//TODO: grab the sql call from here and all the other preparestatement calls
		return new MdbcPreparedStatement(conn.prepareStatement(sql), sql, mgr);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		return new MdbcCallableStatement(conn.prepareCall(sql), mgr);
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		return conn.nativeSQL(sql);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		boolean b = conn.getAutoCommit();
		if (b != autoCommit) {
		    if(progressKeeper!=null) progressKeeper.commitRequested(id);
			try {
				mgr.setAutoCommit(autoCommit,id,progressKeeper,partition);
				if(progressKeeper!=null)
                    progressKeeper.setMusicDone(id);
			} catch (MDBCServiceException e) {
				logger.error(EELFLoggerDelegate.errorLogger, "Commit to music failed", AppMessages.UNKNOWNERROR, ErrorTypes.UNKNOWN, ErrorSeverity.FATAL);
				throw new SQLException("Failure commiting to MUSIC");
			}
			conn.setAutoCommit(autoCommit);
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
		return conn.getAutoCommit();
	}

	@Override
	public void commit() throws SQLException {
		if(progressKeeper.isComplete(id)) {
			return;
		}
		if(progressKeeper != null) {
			progressKeeper.commitRequested(id);
		}

		try {
			mgr.commit(id,progressKeeper,partition);
		} catch (MDBCServiceException e) {
			//If the commit fail, then a new commitId should be used 
			logger.error(EELFLoggerDelegate.errorLogger, "Commit to music failed", AppMessages.UNKNOWNERROR, ErrorTypes.UNKNOWN, ErrorSeverity.FATAL);
			throw new SQLException("Failure commiting to MUSIC");
		}

		if(progressKeeper != null) {
			progressKeeper.setMusicDone(id);
		}

		conn.commit();

		if(progressKeeper != null) {
			progressKeeper.setSQLDone(id);
		}
		//MusicMixin.releaseZKLocks(MusicMixin.currentLockMap.get(getConnID()));
        if(progressKeeper.isComplete(id)){
		    progressKeeper.reinitializeTxProgress(id);
        }
	}

	@Override
	public void rollback() throws SQLException {
		mgr.rollback();
		conn.rollback();
		progressKeeper.reinitializeTxProgress(id);
	}

	@Override
	public void close() throws SQLException {
	    logger.debug("Closing mdbc connection with id:"+id);
		if (mgr != null) {
            logger.debug("Closing mdbc manager with id:"+id);
			mgr.close();
		}
		if (conn != null && !conn.isClosed()) {
            logger.debug("Closing jdbc from mdbc with id:"+id);
			conn.close();
			logger.debug("Connection was closed for id:" + id);
		}
	}

	@Override
	public boolean isClosed() throws SQLException {
		return conn.isClosed();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return conn.getMetaData();
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		conn.setReadOnly(readOnly);
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return conn.isReadOnly();
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		conn.setCatalog(catalog);
	}

	@Override
	public String getCatalog() throws SQLException {
		return conn.getCatalog();
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		conn.setTransactionIsolation(level);
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return conn.getTransactionIsolation();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return conn.getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		conn.clearWarnings();
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		return new MdbcCallableStatement(conn.createStatement(resultSetType, resultSetConcurrency), mgr);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		return new MdbcCallableStatement(conn.prepareStatement(sql, resultSetType, resultSetConcurrency), sql, mgr);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return new MdbcCallableStatement(conn.prepareCall(sql, resultSetType, resultSetConcurrency), mgr);
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return conn.getTypeMap();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		conn.setTypeMap(map);
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		conn.setHoldability(holdability);
	}

	@Override
	public int getHoldability() throws SQLException {
		return conn.getHoldability();
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		return conn.setSavepoint();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		return conn.setSavepoint(name);
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		conn.rollback(savepoint);
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		conn.releaseSavepoint(savepoint);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return new MdbcCallableStatement(conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability), mgr);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		return new MdbcCallableStatement(conn.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability), sql, mgr);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		return new MdbcCallableStatement(conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability), mgr);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		return new MdbcPreparedStatement(conn.prepareStatement(sql, autoGeneratedKeys), sql, mgr);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		return new MdbcPreparedStatement(conn.prepareStatement(sql, columnIndexes), sql, mgr);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		return new MdbcPreparedStatement(conn.prepareStatement(sql, columnNames), sql, mgr);
	}

	@Override
	public Clob createClob() throws SQLException {
		return conn.createClob();
	}

	@Override
	public Blob createBlob() throws SQLException {
		return conn.createBlob();
	}

	@Override
	public NClob createNClob() throws SQLException {
		return conn.createNClob();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		return conn.createSQLXML();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return conn.isValid(timeout);
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		conn.setClientInfo(name, value);
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		conn.setClientInfo(properties);
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return conn.getClientInfo(name);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		return conn.getClientInfo();
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		return conn.createArrayOf(typeName, elements);
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		return conn.createStruct(typeName, attributes);
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		conn.setSchema(schema);
	}

	@Override
	public String getSchema() throws SQLException {
		return conn.getSchema();
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		conn.abort(executor);
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		conn.setNetworkTimeout(executor, milliseconds);
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		return conn.getNetworkTimeout();
	}
}
