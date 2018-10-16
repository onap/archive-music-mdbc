package com.att.research.mdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import org.apache.log4j.Logger;

import com.att.research.exceptions.QueryException;

/**
 * ProxyStatement is a proxy Statement that front ends Statements from the underlying JDBC driver.  It passes all operations through,
 * and invokes the MusicSqlManager when there is the possibility that database tables have been created or dropped.
 *
 * @author Robert Eby
 */
public class ProxyStatement implements CallableStatement {
	private static final Logger logger = Logger.getLogger(ProxyStatement.class);
	private static final String DATASTAX_PREFIX = "com.datastax.driver";

	private final Statement stmt;		// the Statement that we are proxying
	private final MusicSqlManager mgr;

	public ProxyStatement(Statement s, MusicSqlManager m) {
		this.stmt = s;
		this.mgr = m;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return stmt.unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return stmt.isWrapperFor(iface);
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		logger.debug("executeQuery: "+sql);
		ResultSet r = null;
		try {
			mgr.preStatementHook(sql);
			r = stmt.executeQuery(sql);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.warn("executeQuery: exception "+nm);
			if (!nm.startsWith(DATASTAX_PREFIX))
				throw e;
		}
		return r;
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		logger.debug("executeUpdate: "+sql);
		int n = 0;
		try {
			mgr.preStatementHook(sql);
			n = stmt.executeUpdate(sql);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.warn("executeUpdate: exception "+nm);
			if (!nm.startsWith(DATASTAX_PREFIX))
				throw e;
		}
		return n;
	}

	@Override
	public void close() throws SQLException {
		stmt.close();
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
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
		logger.debug("execute: "+sql);
		boolean b = false;
		try {
			mgr.preStatementHook(sql);
			b = stmt.execute(sql);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			// Note: this seems to be the only call Camunda uses, so it is the only one I am fixing for now.
			boolean ignore = nm.startsWith(DATASTAX_PREFIX);
//			ignore |= (nm.startsWith("org.h2.jdbc.JdbcSQLException") && e.getMessage().contains("already exists"));
			if (ignore) {
				logger.warn("execute: exception (IGNORED) "+nm);
			} else {
				logger.warn("execute: exception "+nm);
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
		logger.debug("executeBatch");
		int[] n = null;
		try {
			logger.warn("executeBatch() is not supported by MDBC; your results may be incorrect as a result.");
			n = stmt.executeBatch();
			synchronizeTables(null);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.warn("executeBatch: exception "+nm);
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
		logger.debug("executeUpdate: "+sql);
		int n = 0;
		try {
			mgr.preStatementHook(sql);
			n = stmt.executeUpdate(sql, autoGeneratedKeys);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.warn("executeUpdate: exception "+nm);
			if (!nm.startsWith(DATASTAX_PREFIX))
				throw e;
		}
		return n;
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		logger.debug("executeUpdate: "+sql);
		int n = 0;
		try {
			mgr.preStatementHook(sql);
			n = stmt.executeUpdate(sql, columnIndexes);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.warn("executeUpdate: exception "+nm);
			if (!nm.startsWith(DATASTAX_PREFIX))
				throw e;
		}
		return n;
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		logger.debug("executeUpdate: "+sql);
		int n = 0;
		try {
			mgr.preStatementHook(sql);
			n = stmt.executeUpdate(sql, columnNames);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.warn("executeUpdate: exception "+nm);
			if (!nm.startsWith(DATASTAX_PREFIX))
				throw e;
		}
		return n;
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		logger.debug("execute: "+sql);
		boolean b = false;
		try {
			mgr.preStatementHook(sql);
			b = stmt.execute(sql, autoGeneratedKeys);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.warn("execute: exception "+nm);
			if (!nm.startsWith(DATASTAX_PREFIX))
				throw e;
		}
		return b;
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		logger.debug("execute: "+sql);
		boolean b = false;
		try {
			mgr.preStatementHook(sql);
			b = stmt.execute(sql, columnIndexes);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.warn("execute: exception "+nm);
			if (!nm.startsWith(DATASTAX_PREFIX))
				throw e;
		}
		return b;
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		logger.debug("execute: "+sql);
		boolean b = false;
		try {
			mgr.preStatementHook(sql);
			b = stmt.execute(sql, columnNames);
			mgr.postStatementHook(sql);
			synchronizeTables(sql);
		} catch (Exception e) {
			String nm = e.getClass().getName();
			logger.warn("execute: exception "+nm);
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

	@Override
	public ResultSet executeQuery() throws SQLException {
		logger.debug("executeQuery");
		return ((PreparedStatement)stmt).executeQuery();
	}

	@Override
	public int executeUpdate() throws SQLException {
		logger.debug("executeUpdate");
		return ((PreparedStatement)stmt).executeUpdate();
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		((PreparedStatement)stmt).setNull(parameterIndex, sqlType);
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		((PreparedStatement)stmt).setBoolean(parameterIndex, x);
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		((PreparedStatement)stmt).setByte(parameterIndex, x);
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		((PreparedStatement)stmt).setShort(parameterIndex, x);
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		((PreparedStatement)stmt).setInt(parameterIndex, x);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		((PreparedStatement)stmt).setLong(parameterIndex, x);
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		((PreparedStatement)stmt).setFloat(parameterIndex, x);
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		((PreparedStatement)stmt).setDouble(parameterIndex, x);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		((PreparedStatement)stmt).setBigDecimal(parameterIndex, x);
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		((PreparedStatement)stmt).setString(parameterIndex, x);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		((PreparedStatement)stmt).setBytes(parameterIndex, x);
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		((PreparedStatement)stmt).setDate(parameterIndex, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		((PreparedStatement)stmt).setTime(parameterIndex, x);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		((PreparedStatement)stmt).setTimestamp(parameterIndex, x);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		((PreparedStatement)stmt).setAsciiStream(parameterIndex, x, length);
	}

	@SuppressWarnings("deprecation")
	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		((PreparedStatement)stmt).setUnicodeStream(parameterIndex, x, length);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		((PreparedStatement)stmt).setBinaryStream(parameterIndex, x, length);
	}

	@Override
	public void clearParameters() throws SQLException {
		((PreparedStatement)stmt).clearParameters();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		((PreparedStatement)stmt).setObject(parameterIndex, x, targetSqlType);
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		((PreparedStatement)stmt).setObject(parameterIndex, x);
	}

	@Override
	public boolean execute() throws SQLException {
		return ((PreparedStatement)stmt).execute();
	}

	@Override
	public void addBatch() throws SQLException {
		((PreparedStatement)stmt).addBatch();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		((PreparedStatement)stmt).setCharacterStream(parameterIndex, reader, length);
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		((PreparedStatement)stmt).setRef(parameterIndex, x);
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		((PreparedStatement)stmt).setBlob(parameterIndex, x);
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		((PreparedStatement)stmt).setClob(parameterIndex, x);
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		((PreparedStatement)stmt).setArray(parameterIndex, x);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return ((PreparedStatement)stmt).getMetaData();
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		((PreparedStatement)stmt).setDate(parameterIndex, x, cal);
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		((PreparedStatement)stmt).setTime(parameterIndex, x, cal);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		((CallableStatement)stmt).setTimestamp(parameterIndex, x, cal);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		((CallableStatement)stmt).setNull(parameterIndex, sqlType, typeName);
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		((CallableStatement)stmt).setURL(parameterIndex, x);
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		return ((CallableStatement)stmt).getParameterMetaData();
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		((CallableStatement)stmt).setRowId(parameterIndex, x);
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		((CallableStatement)stmt).setNString(parameterIndex, value);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		((CallableStatement)stmt).setNCharacterStream(parameterIndex, value, length);
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		((CallableStatement)stmt).setNClob(parameterIndex, value);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		((CallableStatement)stmt).setClob(parameterIndex, reader, length);
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		((CallableStatement)stmt).setBlob(parameterIndex, inputStream, length);
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		((CallableStatement)stmt).setNClob(parameterIndex, reader, length);
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		((CallableStatement)stmt).setSQLXML(parameterIndex, xmlObject);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		((CallableStatement)stmt).setObject(parameterIndex, x, targetSqlType, scaleOrLength);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		((CallableStatement)stmt).setAsciiStream(parameterIndex, x, length);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		((CallableStatement)stmt).setBinaryStream(parameterIndex, x, length);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		((CallableStatement)stmt).setCharacterStream(parameterIndex, reader, length);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		((CallableStatement)stmt).setAsciiStream(parameterIndex, x);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		((CallableStatement)stmt).setBinaryStream(parameterIndex, x);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		((CallableStatement)stmt).setCharacterStream(parameterIndex, reader);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		((CallableStatement)stmt).setNCharacterStream(parameterIndex, value);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		((CallableStatement)stmt).setClob(parameterIndex, reader);
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		((CallableStatement)stmt).setBlob(parameterIndex, inputStream);
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		((CallableStatement)stmt).setNClob(parameterIndex, reader);
	}

	@Override
	public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
		((CallableStatement)stmt).registerOutParameter(parameterIndex, sqlType);
	}

	@Override
	public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
		((CallableStatement)stmt).registerOutParameter(parameterIndex, sqlType, scale);
	}

	@Override
	public boolean wasNull() throws SQLException {
		return ((CallableStatement)stmt).wasNull();
	}

	@Override
	public String getString(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getString(parameterIndex);
	}

	@Override
	public boolean getBoolean(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getBoolean(parameterIndex);
	}

	@Override
	public byte getByte(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getByte(parameterIndex);
	}

	@Override
	public short getShort(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getShort(parameterIndex);
	}

	@Override
	public int getInt(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getInt(parameterIndex);
	}

	@Override
	public long getLong(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getLong(parameterIndex);
	}

	@Override
	public float getFloat(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getFloat(parameterIndex);
	}

	@Override
	public double getDouble(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getDouble(parameterIndex);
	}

	@SuppressWarnings("deprecation")
	@Override
	public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
		return ((CallableStatement)stmt).getBigDecimal(parameterIndex, scale);
	}

	@Override
	public byte[] getBytes(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getBytes(parameterIndex);
	}

	@Override
	public Date getDate(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getDate(parameterIndex);
	}

	@Override
	public Time getTime(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getTime(parameterIndex);
	}

	@Override
	public Timestamp getTimestamp(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getTimestamp(parameterIndex);
	}

	@Override
	public Object getObject(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getObject(parameterIndex);
	}

	@Override
	public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getBigDecimal(parameterIndex);
	}

	@Override
	public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
		return ((CallableStatement)stmt).getObject(parameterIndex, map);
	}

	@Override
	public Ref getRef(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getRef(parameterIndex);
	}

	@Override
	public Blob getBlob(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getBlob(parameterIndex);
	}

	@Override
	public Clob getClob(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getClob(parameterIndex);
	}

	@Override
	public Array getArray(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getArray(parameterIndex);
	}

	@Override
	public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
		return ((CallableStatement)stmt).getDate(parameterIndex, cal);
	}

	@Override
	public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
		return ((CallableStatement)stmt).getTime(parameterIndex, cal);
	}

	@Override
	public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
		return ((CallableStatement)stmt).getTimestamp(parameterIndex, cal);
	}

	@Override
	public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
		((CallableStatement)stmt).registerOutParameter(parameterIndex, sqlType, typeName);
	}

	@Override
	public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
		((CallableStatement)stmt).registerOutParameter(parameterName, sqlType);
	}

	@Override
	public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
		((CallableStatement)stmt).registerOutParameter(parameterName, sqlType, scale);
	}

	@Override
	public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
		((CallableStatement)stmt).registerOutParameter(parameterName, sqlType, typeName);
	}

	@Override
	public URL getURL(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getURL(parameterIndex);
	}

	@Override
	public void setURL(String parameterName, URL val) throws SQLException {
		((CallableStatement)stmt).setURL(parameterName, val);
	}

	@Override
	public void setNull(String parameterName, int sqlType) throws SQLException {
		((CallableStatement)stmt).setNull(parameterName, sqlType);
	}

	@Override
	public void setBoolean(String parameterName, boolean x) throws SQLException {
		((CallableStatement)stmt).setBoolean(parameterName, x);
	}

	@Override
	public void setByte(String parameterName, byte x) throws SQLException {
		((CallableStatement)stmt).setByte(parameterName, x);
	}

	@Override
	public void setShort(String parameterName, short x) throws SQLException {
		((CallableStatement)stmt).setShort(parameterName, x);
	}

	@Override
	public void setInt(String parameterName, int x) throws SQLException {
		((CallableStatement)stmt).setInt(parameterName, x);
	}

	@Override
	public void setLong(String parameterName, long x) throws SQLException {
		((CallableStatement)stmt).setLong(parameterName, x);
	}

	@Override
	public void setFloat(String parameterName, float x) throws SQLException {
		((CallableStatement)stmt).setFloat(parameterName, x);
	}

	@Override
	public void setDouble(String parameterName, double x) throws SQLException {
		((CallableStatement)stmt).setDouble(parameterName, x);
	}

	@Override
	public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
		((CallableStatement)stmt).setBigDecimal(parameterName, x);
	}

	@Override
	public void setString(String parameterName, String x) throws SQLException {
		((CallableStatement)stmt).setString(parameterName, x);
	}

	@Override
	public void setBytes(String parameterName, byte[] x) throws SQLException {
		((CallableStatement)stmt).setBytes(parameterName, x);
	}

	@Override
	public void setDate(String parameterName, Date x) throws SQLException {
		((CallableStatement)stmt).setDate(parameterName, x);
	}

	@Override
	public void setTime(String parameterName, Time x) throws SQLException {
		((CallableStatement)stmt).setTime(parameterName, x);
	}

	@Override
	public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
		((CallableStatement)stmt).setTimestamp(parameterName, x);
	}

	@Override
	public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
		((CallableStatement)stmt).setAsciiStream(parameterName, x, length);
	}

	@Override
	public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
		((CallableStatement)stmt).setBinaryStream(parameterName, x, length);
	}

	@Override
	public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
		((CallableStatement)stmt).setObject(parameterName, x, targetSqlType, scale);
	}

	@Override
	public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
		((CallableStatement)stmt).setObject(parameterName, x, targetSqlType);
	}

	@Override
	public void setObject(String parameterName, Object x) throws SQLException {
		((CallableStatement)stmt).setObject(parameterName, x);
	}

	@Override
	public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
		((CallableStatement)stmt).setCharacterStream(parameterName, reader, length);
	}

	@Override
	public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
		((CallableStatement)stmt).setDate(parameterName, x, cal);
	}

	@Override
	public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
		((CallableStatement)stmt).setTime(parameterName, x, cal);
	}

	@Override
	public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
		((CallableStatement)stmt).setTimestamp(parameterName, x, cal);
	}

	@Override
	public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
		((CallableStatement)stmt).setNull(parameterName, sqlType, typeName);
	}

	@Override
	public String getString(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getString(parameterName);
	}

	@Override
	public boolean getBoolean(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getBoolean(parameterName);
	}

	@Override
	public byte getByte(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getByte(parameterName);
	}

	@Override
	public short getShort(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getShort(parameterName);
	}

	@Override
	public int getInt(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getInt(parameterName);
	}

	@Override
	public long getLong(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getLong(parameterName);
	}

	@Override
	public float getFloat(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getFloat(parameterName);
	}

	@Override
	public double getDouble(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getDouble(parameterName);
	}

	@Override
	public byte[] getBytes(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getBytes(parameterName);
	}

	@Override
	public Date getDate(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getDate(parameterName);
	}

	@Override
	public Time getTime(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getTime(parameterName);
	}

	@Override
	public Timestamp getTimestamp(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getTimestamp(parameterName);
	}

	@Override
	public Object getObject(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getObject(parameterName);
	}

	@Override
	public BigDecimal getBigDecimal(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getBigDecimal(parameterName);
	}

	@Override
	public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
		return ((CallableStatement)stmt).getObject(parameterName, map);
	}

	@Override
	public Ref getRef(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getRef(parameterName);
	}

	@Override
	public Blob getBlob(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getBlob(parameterName);
	}

	@Override
	public Clob getClob(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getClob(parameterName);
	}

	@Override
	public Array getArray(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getArray(parameterName);
	}

	@Override
	public Date getDate(String parameterName, Calendar cal) throws SQLException {
		return ((CallableStatement)stmt).getDate(parameterName, cal);
	}

	@Override
	public Time getTime(String parameterName, Calendar cal) throws SQLException {
		return ((CallableStatement)stmt).getTime(parameterName, cal);
	}

	@Override
	public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
		return ((CallableStatement)stmt).getTimestamp(parameterName, cal);
	}

	@Override
	public URL getURL(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getURL(parameterName);
	}

	@Override
	public RowId getRowId(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getRowId(parameterIndex);
	}

	@Override
	public RowId getRowId(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getRowId(parameterName);
	}

	@Override
	public void setRowId(String parameterName, RowId x) throws SQLException {
		((CallableStatement)stmt).setRowId(parameterName, x);
	}

	@Override
	public void setNString(String parameterName, String value) throws SQLException {
		((CallableStatement)stmt).setNString(parameterName, value);
	}

	@Override
	public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
		((CallableStatement)stmt).setNCharacterStream(parameterName, value, length);
	}

	@Override
	public void setNClob(String parameterName, NClob value) throws SQLException {
		((CallableStatement)stmt).setNClob(parameterName, value);
	}

	@Override
	public void setClob(String parameterName, Reader reader, long length) throws SQLException {
		((CallableStatement)stmt).setClob(parameterName, reader, length);
	}

	@Override
	public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
		((CallableStatement)stmt).setBlob(parameterName, inputStream, length);
	}

	@Override
	public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
		((CallableStatement)stmt).setNClob(parameterName, reader, length);
	}

	@Override
	public NClob getNClob(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getNClob(parameterIndex);
	}

	@Override
	public NClob getNClob(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getNClob(parameterName);
	}

	@Override
	public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
		((CallableStatement)stmt).setSQLXML(parameterName, xmlObject);
	}

	@Override
	public SQLXML getSQLXML(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getSQLXML(parameterIndex);
	}

	@Override
	public SQLXML getSQLXML(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getSQLXML(parameterName);
	}

	@Override
	public String getNString(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getNString(parameterIndex);
	}

	@Override
	public String getNString(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getNString(parameterName);
	}

	@Override
	public Reader getNCharacterStream(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getNCharacterStream(parameterIndex);
	}

	@Override
	public Reader getNCharacterStream(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getNCharacterStream(parameterName);
	}

	@Override
	public Reader getCharacterStream(int parameterIndex) throws SQLException {
		return ((CallableStatement)stmt).getCharacterStream(parameterIndex);
	}

	@Override
	public Reader getCharacterStream(String parameterName) throws SQLException {
		return ((CallableStatement)stmt).getCharacterStream(parameterName);
	}

	@Override
	public void setBlob(String parameterName, Blob x) throws SQLException {
		((CallableStatement)stmt).setBlob(parameterName, x);
	}

	@Override
	public void setClob(String parameterName, Clob x) throws SQLException {
		((CallableStatement)stmt).setClob(parameterName, x);
	}

	@Override
	public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
		((CallableStatement)stmt).setAsciiStream(parameterName, x, length);
	}

	@Override
	public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
		((CallableStatement)stmt).setBinaryStream(parameterName, x, length);
	}

	@Override
	public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
		((CallableStatement)stmt).setCharacterStream(parameterName, reader, length);
	}

	@Override
	public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
		((CallableStatement)stmt).setAsciiStream(parameterName, x);
	}

	@Override
	public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
		((CallableStatement)stmt).setBinaryStream(parameterName, x);
	}

	@Override
	public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
		((CallableStatement)stmt).setCharacterStream(parameterName, reader);
	}

	@Override
	public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
		((CallableStatement)stmt).setNCharacterStream(parameterName, value);
	}

	@Override
	public void setClob(String parameterName, Reader reader) throws SQLException {
		((CallableStatement)stmt).setClob(parameterName, reader);
	}

	@Override
	public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
		((CallableStatement)stmt).setBlob(parameterName, inputStream);
	}

	@Override
	public void setNClob(String parameterName, Reader reader) throws SQLException {
		((CallableStatement)stmt).setNClob(parameterName, reader);
	}

	@Override
	public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
		return ((CallableStatement)stmt).getObject(parameterIndex, type);
	}

	@Override
	public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
		return ((CallableStatement)stmt).getObject(parameterName, type);
	}

	private void synchronizeTables(String sql) {
		if (sql == null || sql.trim().toLowerCase().startsWith("create")) {
			if (mgr != null) {
				try {
					mgr.synchronizeTables();
				} catch (QueryException e) {
					
					e.printStackTrace();
				}
			}
		}
	}
}
