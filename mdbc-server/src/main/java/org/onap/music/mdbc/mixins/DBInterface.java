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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.TableInfo;
import org.onap.music.mdbc.tables.Operation;
import org.onap.music.mdbc.tables.StagingTable;

/**
 * This Interface defines the methods that MDBC needs in order to mirror data to/from a Database instance.
 *
 * @author Robert P. Eby
 */
public interface DBInterface {
	/**
	 * Get the name of this DBnterface mixin object.
	 * @return the name
	 */
	String getMixinName();
	/**
	 * Do what is needed to close down the database connection.
	 */
	void close();
	/**
	 * Get a set of the table names in the database. The table names should be returned in UPPER CASE.
	 * @return the set
	 */
	Set<String> getSQLTableSet();
	/**
	 * Return the name of the database that the driver is connected to
	 * @return
	 */
	String getDatabaseName();
	/**
	 * Return a TableInfo object for the specified table.
	 * @param tableName the table to look up
	 * @return a TableInfo object containing the info we need, or null if the table does not exist
	 */
	TableInfo getTableInfo(String tableName);
	/**
	 * This method should create triggers in the database to be called for each row after every INSERT,
	 * UPDATE and DELETE, and before every SELECT.
	 * @param tableName this is the table on which triggers are being created.
	 */
	void createSQLTriggers(String tableName);
	/**
	 * This method should drop all triggers previously created in the database for the table.
	 * @param tableName this is the table on which triggers are being dropped.
	 */
	void dropSQLTriggers(String tableName);
	/**
	 * This method inserts a row into the SQL database, defined via a map of column names and values.
	 * @param tableName the table to insert the row into
	 * @param map map of column names &rarr; values to use for the keys when inserting the row
	 */
	void insertRowIntoSqlDb(String tableName, Map<String, Object> map);
	/**
	 * This method deletes a row from the SQL database, defined via a map of column names and values.
	 * @param tableName the table to delete the row from
	 * @param map map of column names &rarr; values to use for the keys when deleting the row
	 */
	void deleteRowFromSqlDb(String tableName, Map<String, Object> map);
	/**
	 * Code to be run within the DB driver before a SQL statement is executed.  This is where tables
	 * can be synchronized before a SELECT, for those databases that do not support SELECT triggers.
	 * @param sql the SQL statement that is about to be executed
	 */
	void preStatementHook(final String sql);
	/**
	 * Code to be run within the DB driver after a SQL statement has been executed.  This is where remote
	 * statement actions can be copied back to Cassandra/MUSIC.
	 * @param sql the SQL statement that was executed
	 * @param transactionDigest
	 */
	void postStatementHook(final String sql,Map<Range,StagingTable> transactionDigest);
	/**
	 * This method executes a read query in the SQL database.  Methods that call this method should be sure
	 * to call resultset.getStatement().close() when done in order to free up resources.
	 * @param sql the query to run
	 * @return a ResultSet containing the rows returned from the query
	 */
	ResultSet executeSQLRead(String sql);
	
	void synchronizeData(String tableName);
	
	List<String> getReservedTblNames();
	
	String getPrimaryKey(String sql, String tableName);
	
	/**
	 * Replay a given TxDigest into the local DB
	 * @param digest
	 * @throws SQLException if replay cannot occur correctly
	 */
	public void replayTransaction(HashMap<Range,StagingTable> digest) throws SQLException;
}
