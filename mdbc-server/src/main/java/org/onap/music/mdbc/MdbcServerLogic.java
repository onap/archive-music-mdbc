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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.mdbc.configurations.NodeConfiguration;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.TypedValue;

import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.logging.format.AppMessages;
import org.onap.music.logging.format.ErrorSeverity;
import org.onap.music.logging.format.ErrorTypes;

public class MdbcServerLogic extends JdbcMeta{

	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MdbcServerLogic.class);

	StateManager manager;
	private DatabasePartition ranges;
	String name;

	//TODO: Delete this properties after debugging
	private final Properties info;
	private final Cache<String, Connection> connectionCache;

	public MdbcServerLogic(String Url, Properties info,NodeConfiguration config) throws SQLException, MDBCServiceException {
		super(Url,info);
		this.setRanges(config.partition);
		this.name = config.nodeName;
		this.manager = new StateManager(Url,info,this.getRanges(),"test"); //FIXME: db name should not be passed in ahead of time
		this.info = info;
        int concurrencyLevel = Integer.parseInt(
                info.getProperty(ConnectionCacheSettings.CONCURRENCY_LEVEL.key(),
                        ConnectionCacheSettings.CONCURRENCY_LEVEL.defaultValue()));
        int initialCapacity = Integer.parseInt(
                info.getProperty(ConnectionCacheSettings.INITIAL_CAPACITY.key(),
                        ConnectionCacheSettings.INITIAL_CAPACITY.defaultValue()));
        long maxCapacity = Long.parseLong(
                info.getProperty(ConnectionCacheSettings.MAX_CAPACITY.key(),
                        ConnectionCacheSettings.MAX_CAPACITY.defaultValue()));
        long connectionExpiryDuration = Long.parseLong(
                info.getProperty(ConnectionCacheSettings.EXPIRY_DURATION.key(),
                        ConnectionCacheSettings.EXPIRY_DURATION.defaultValue()));
        TimeUnit connectionExpiryUnit = TimeUnit.valueOf(
                info.getProperty(ConnectionCacheSettings.EXPIRY_UNIT.key(),
                        ConnectionCacheSettings.EXPIRY_UNIT.defaultValue()));
        this.connectionCache = CacheBuilder.newBuilder()
                .concurrencyLevel(concurrencyLevel)
                .initialCapacity(initialCapacity)
                .maximumSize(maxCapacity)
                .expireAfterAccess(connectionExpiryDuration, connectionExpiryUnit)
                .removalListener(new ConnectionExpiryHandler())
                .build();
	}
	
	public StateManager getStateManager() {
		return this.manager;
	}
	
	@Override
    protected Connection getConnection(String id) throws SQLException {
        if (id == null) {
            throw new NullPointerException("Connection id is null");
        }
        //\TODO: don't use connectionCache, use this.manager internal state
        Connection conn = connectionCache.getIfPresent(id);
        if (conn == null) {
            this.manager.CloseConnection(id);
            logger.error(EELFLoggerDelegate.errorLogger,"Connection not found: invalid id, closed, or expired: "
                    + id);
            throw new RuntimeException(" Connection not found: invalid id, closed, or expired: " + id);
        }
        return conn;
    }

	@Override
	public void openConnection(ConnectionHandle ch, Map<String, String> information) {
        Properties fullInfo = new Properties();
        fullInfo.putAll(this.info);
        if (information != null) {
            fullInfo.putAll(information);
		}

        final ConcurrentMap<String, Connection> cacheAsMap = this.connectionCache.asMap();
        if (cacheAsMap.containsKey(ch.id)) {
            throw new RuntimeException("Connection already exists: " + ch.id);
        }
        // Avoid global synchronization of connection opening
        try {
            this.manager.OpenConnection(ch.id, info);
            Connection conn = this.manager.GetConnection(ch.id);
            if(conn == null) {
                logger.error(EELFLoggerDelegate.errorLogger, "Connection created was null");
                throw new RuntimeException("Connection created was null for connection: " + ch.id);
            }
            Connection loadedConn = cacheAsMap.putIfAbsent(ch.id, conn);
            logger.info("connection created with id {}", ch.id);
            // Race condition: someone beat us to storing the connection in the cache.
            if (loadedConn != null) {
                //\TODO check if we added an additional race condition for this
                this.manager.CloseConnection(ch.id);
                conn.close();
                throw new RuntimeException("Connection already exists: " + ch.id);
            }
        } catch (SQLException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
            throw new RuntimeException(e);
        }
    }

	@Override
	public void closeConnection(ConnectionHandle ch) {
	    //\TODO use state connection instead
        Connection conn = connectionCache.getIfPresent(ch.id);
        if (conn == null) {
            logger.debug("client requested close unknown connection {}", ch);
            return;
        }
        logger.trace("closing connection {}", ch);
        try {
            conn.close();
        } catch (SQLException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
            throw new RuntimeException(e.getMessage());
        } finally {
            connectionCache.invalidate(ch.id);
            this.manager.CloseConnection(ch.id);
            logger.info("connection closed with id {}", ch.id);
        }
	}

    @Override
    public void commit(ConnectionHandle ch) {
        try {
            super.commit(ch);
            logger.debug("connection commited with id {}", ch.id);
        } catch (Exception err ) {
            logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
            throw(err);
        }
    }
	
	//\TODO All the following functions can be deleted
	// Added for two reasons: debugging and logging
	@Override
	public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
		StatementHandle h;
		try {
			h = super.prepare(ch, sql, maxRowCount);
			logger.debug("prepared statement {}", h);
		} catch (Exception e ) {
			logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(e);
		}
		return h;
	}

	@Override
	public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount, int maxRowsInFirstFrame,
			PrepareCallback callback) throws NoSuchStatementException {
		ExecuteResult e;
		try {
			e = super.prepareAndExecute(h, sql, maxRowCount,maxRowsInFirstFrame,callback);
			logger.debug("prepare and execute statement {}", h);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}
		return e;
	}

	@Override
	public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle h, List<String> sqlCommands)
			throws NoSuchStatementException {
		ExecuteBatchResult e;
		try {
			e = super.prepareAndExecuteBatch(h, sqlCommands);
			logger.debug("prepare and execute batch statement {}", h);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}
		return e;
	}

	@Override
	public ExecuteBatchResult executeBatch(StatementHandle h, List<List<TypedValue>> parameterValues)
			throws NoSuchStatementException {
		ExecuteBatchResult e;
		try {
			e = super.executeBatch(h, parameterValues);
			logger.debug("execute batch statement {}", h);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}
		return e;
	}

	@Override
	public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount)
			throws NoSuchStatementException, MissingResultsException {
		Frame f;
		try {
			f = super.fetch(h, offset, fetchMaxRowCount);
			logger.debug("fetch statement {}", h);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}
		return f;
	}

	@Override
	public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues, long maxRowCount)
			throws NoSuchStatementException {
		ExecuteResult e;
		try {
			e = super.execute(h, parameterValues, maxRowCount);
			logger.debug("fetch statement {}", h);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}
		return e;
	}

	@Override
	public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame)
			throws NoSuchStatementException {
		ExecuteResult e;
		try {
			e = super.execute(h, parameterValues, maxRowsInFirstFrame);
			logger.debug("fetch statement {}", h);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}
		return e;
	}

	@Override
	public StatementHandle createStatement(ConnectionHandle ch) {
		StatementHandle h;
		try {
			h = super.createStatement(ch);
			logger.debug("create statement {}", h);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}
		return h;
	}

	@Override
	public void closeStatement(StatementHandle h) {
		try {
			super.closeStatement(h);
			logger.debug("statement closed {}", h);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}		
	}







	@Override
	public void rollback(ConnectionHandle ch) {
		try {
			super.rollback(ch);
			logger.debug("connection rollback with id {}", ch.id);
		} catch (Exception err ) {
			logger.error(EELFLoggerDelegate.errorLogger, err.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
			throw(err);
		}				
	}

	public DatabasePartition getRanges() {
		return ranges;
	}

	public void setRanges(DatabasePartition ranges) {
		this.ranges = ranges;
	}

	private class ConnectionExpiryHandler
            implements RemovalListener<String, Connection> {

        public void onRemoval(RemovalNotification<String, Connection> notification) {
            String connectionId = notification.getKey();
            Connection doomed = notification.getValue();
            logger.debug("Expiring connection {} because {}", connectionId, notification.getCause());
            try {
                if (doomed != null) {
                    doomed.close();
                }
            } catch (Throwable t) {
                logger.warn("Exception thrown while expiring connection {}", connectionId, t);
            }
        }
    }
}


