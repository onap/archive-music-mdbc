package com.att.research.mdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import com.att.research.exceptions.QueryException;
import com.att.research.logging.EELFLoggerDelegate;
import com.att.research.logging.format.AppMessages;
import com.att.research.logging.format.ErrorSeverity;
import com.att.research.logging.format.ErrorTypes;

/**
 * ProxyStatement is a proxy Statement that front ends Statements from the underlying JDBC driver.  It passes all operations through,
 * and invokes the MusicSqlManager when there is the possibility that database tables have been created or dropped.
 *
 * @author Robert Eby
 */
public class MdbcStatement implements Statement {
	private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MdbcStatement.class);
	private static final String DATASTAX_PREFIX = "com.datastax.driver";

	final Statement stmt;		// the Statement that we are proxying
	final MusicSqlManager mgr;
	//\TODO We may need to all pass the connection object to support autocommit

	public MdbcStatement(Statement s, MusicSqlManager m) {
		this.stmt = s;
		this.mgr = m;
	}

	public MdbcStatement(Statement stmt, String sql, MusicSqlManager mgr) {
		//\TODO why there is a constructor with a sql parameter in a not PreparedStatement
		this.stmt = stmt;
		this.mgr = mgr;
	}
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		logger.error(EELFLoggerDelegate.errorLogger, "proxystatement unwrap: " + iface.getName());
		return stmt.unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		logger.error(EELFLoggerDelegate.errorLogger, "proxystatement isWrapperFor: " + iface.getName());
		return stmt.isWrapperFor(iface);
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		logger.debug(EELFLoggerDelegate.applicationLogger,"executeQuery: "+sql);
		ResultSet r = null;
		try {
			mgr.preStatementHook(sql);
			r = stmt.executeQuery(sql);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.error(EELFLoggerDelegate.errorLogger, "executeQuery: exception "+nm);
			if (!nm.startsWith(DATASTAX_PREFIX))
				throw e;
		}
		return r;
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		logger.debug(EELFLoggerDelegate.applicationLogger,"executeUpdate: "+sql);
		
		int n = 0;
		try {
			mgr.preStatementHook(sql);
			n = stmt.executeUpdate(sql);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.error(EELFLoggerDelegate.errorLogger, "executeUpdate: exception "+nm+" "+e);
			if (!nm.startsWith(DATASTAX_PREFIX))
				throw e;
		}
		return n;
	}

	@Override
	public void close() throws SQLException {
		logger.debug(EELFLoggerDelegate.applicationLogger,"Statement close: ");
		stmt.close();
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		logger.debug(EELFLoggerDelegate.applicationLogger,"getMaxFieldSize");
		return stmt.getMaxFieldSize();
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		stmt.setMaxFieldSize(max);
	}

	@Override
	public int getMaxRows() throws SQLException {
		return stmt.getMaxRows();
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		stmt.setMaxRows(max);
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		stmt.setEscapeProcessing(enable);
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return stmt.getQueryTimeout();
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		//\TODO: we also need to implement a higher level timeout in MDBC 
		logger.debug(EELFLoggerDelegate.applicationLogger,"setQueryTimeout seconds "+ seconds);
		stmt.setQueryTimeout(seconds);
	}

	@Override
	public void cancel() throws SQLException {
		stmt.cancel();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return stmt.getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		stmt.clearWarnings();
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		stmt.setCursorName(name);
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		logger.debug(EELFLoggerDelegate.applicationLogger,"execute: "+sql);
		boolean b = false;
		//\TODO Add the result of the postStatementHook to b
		try {
			mgr.preStatementHook(sql);
			b = stmt.execute(sql);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.error(EELFLoggerDelegate.errorLogger, "execute: exception "+nm+" "+e);
			// Note: this seems to be the only call Camunda uses, so it is the only one I am fixing for now.
			boolean ignore = nm.startsWith(DATASTAX_PREFIX);
//			ignore |= (nm.startsWith("org.h2.jdbc.JdbcSQLException") && e.getMessage().contains("already exists"));
			if (ignore) {
				logger.warn("execute: exception (IGNORED) "+nm);
			} else {
				logger.error(EELFLoggerDelegate.errorLogger, " Exception "+nm+" "+e);
				throw e;
			}
		}
		return b;
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return stmt.getResultSet();
	}

	@Override
	public int getUpdateCount() throws SQLException {
		return stmt.getUpdateCount();
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		return stmt.getMoreResults();
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		stmt.setFetchDirection(direction);
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return stmt.getFetchDirection();
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		stmt.setFetchSize(rows);
	}

	@Override
	public int getFetchSize() throws SQLException {
		return stmt.getFetchSize();
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		return stmt.getResultSetConcurrency();
	}

	@Override
	public int getResultSetType() throws SQLException {
		return stmt.getResultSetType();
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		stmt.addBatch(sql);
	}

	@Override
	public void clearBatch() throws SQLException {
		stmt.clearBatch();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		logger.debug(EELFLoggerDelegate.applicationLogger,"executeBatch: ");
		int[] n = null;
		try {
			logger.debug(EELFLoggerDelegate.applicationLogger,"executeBatch() is not supported by MDBC; your results may be incorrect as a result.");
			n = stmt.executeBatch();
			synchronizeTables(null);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.error(EELFLoggerDelegate.errorLogger,"executeBatch: exception "+nm);
			if (!nm.startsWith(DATASTAX_PREFIX))
				throw e;
		}
		return n;
	}

	@Override
	public Connection getConnection() throws SQLException {
		return stmt.getConnection();
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		return stmt.getMoreResults(current);
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		return stmt.getGeneratedKeys();
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		logger.debug(EELFLoggerDelegate.applicationLogger,"executeUpdate: "+sql);
		int n = 0;
		try {
			mgr.preStatementHook(sql);
			n = stmt.executeUpdate(sql, autoGeneratedKeys);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.error(EELFLoggerDelegate.errorLogger,"executeUpdate: exception "+nm);
			if (!nm.startsWith(DATASTAX_PREFIX))
				throw e;
		}
		return n;
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		logger.debug(EELFLoggerDelegate.applicationLogger,"executeUpdate: "+sql);
		int n = 0;
		try {
			mgr.preStatementHook(sql);
			n = stmt.executeUpdate(sql, columnIndexes);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.error(EELFLoggerDelegate.errorLogger,"executeUpdate: exception "+nm);
			if (!nm.startsWith(DATASTAX_PREFIX))
				throw e;
		}
		return n;
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		logger.debug(EELFLoggerDelegate.applicationLogger,"executeUpdate: "+sql);
		int n = 0;
		try {
			mgr.preStatementHook(sql);
			n = stmt.executeUpdate(sql, columnNames);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.error(EELFLoggerDelegate.errorLogger,"executeUpdate: exception "+nm);
			if (!nm.startsWith(DATASTAX_PREFIX))
				throw e;
		}
		return n;
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		logger.debug(EELFLoggerDelegate.applicationLogger,"execute: "+sql);
		boolean b = false;
		try {
			mgr.preStatementHook(sql);
			b = stmt.execute(sql, autoGeneratedKeys);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.error(EELFLoggerDelegate.errorLogger,"execute: exception "+nm);
			if (!nm.startsWith(DATASTAX_PREFIX))
				throw e;
		}
		return b;
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		logger.debug(EELFLoggerDelegate.applicationLogger,"execute: "+sql);
		boolean b = false;
		try {
			mgr.preStatementHook(sql);
			b = stmt.execute(sql, columnIndexes);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.error(EELFLoggerDelegate.errorLogger,"execute: exception "+nm);
			if (!nm.startsWith(DATASTAX_PREFIX))
				throw e;
		}
		return b;
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		logger.debug(EELFLoggerDelegate.applicationLogger,"execute: "+sql);
		//\TODO Idem to the other execute without columnNames
		boolean b = false;
		try {
			mgr.preStatementHook(sql);
			b = stmt.execute(sql, columnNames);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.error(EELFLoggerDelegate.errorLogger,"execute: exception "+nm);
			if (!nm.startsWith(DATASTAX_PREFIX))
				throw e;
		}
		return b;
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return stmt.getResultSetHoldability();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return stmt.isClosed();
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		stmt.setPoolable(poolable);
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return stmt.isPoolable();
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		stmt.closeOnCompletion();
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		return stmt.isCloseOnCompletion();
	}
	
	protected void synchronizeTables(String sql)  {
		if (sql == null || sql.trim().toLowerCase().startsWith("create")) {
			if (mgr != null) {
				try {
					mgr.synchronizeTables();
				} catch (QueryException e) {
					logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
				}
			}
		}
	}
}
