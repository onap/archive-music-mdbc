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

import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.json.JSONObject;
import org.json.JSONTokener;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;
import org.onap.music.main.ReturnType;

import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.TableInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * This class provides the methods that MDBC needs to access Cassandra directly in order to provide persistence
 * to calls to the user's DB.  It stores dirty row references in one table (called DIRTY____) rather than one dirty
 * table per real table (as {@link org.onap.music.mdbc.mixins.CassandraMixin} does).
 *
 * @author Robert P. Eby
 */
public class Cassandra2Mixin extends CassandraMixin {
	private static final String DIRTY_TABLE = "DIRTY____";	// it seems Cassandra won't allow __DIRTY__
	private boolean dirty_table_created = false;
	
	private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(Cassandra2Mixin.class);

	public Cassandra2Mixin() {
		super();
	}

	public Cassandra2Mixin(String url, Properties info) throws MDBCServiceException {
		super(url, info);
	}

	/**
	 * Get the name of this MusicInterface mixin object.
	 * @return the name
	 */
	@Override
	public String getMixinName() {
		return "cassandra2";
	}
	/**
	 * Do what is needed to close down the MUSIC connection.
	 */
	@Override
	public void close() {
		super.close();
	}

	/**
	 * This method creates a keyspace in Music/Cassandra to store the data corresponding to the SQL tables.
	 * The keyspace name comes from the initialization properties passed to the JDBC driver.
	 * @throws MusicServiceException 
	 */
	@Override
	public void createKeyspace() throws MDBCServiceException {
		super.createKeyspace();
	}

	/**
	 * This method performs all necessary initialization in Music/Cassandra to store the table <i>tableName</i>.
	 * @param tableName the table to initialize MUSIC for
	 */
	@Override
	public void initializeMusicForTable(TableInfo ti, String tableName) {
		super.initializeMusicForTable(ti, tableName);
	}

	/**
	 * Create a <i>dirty row</i> table for the real table <i>tableName</i>.  The primary keys columns from the real table are recreated in
	 * the dirty table, along with a "REPLICA__" column that names the replica that should update it's internal state from MUSIC.
	 * @param tableName the table to create a "dirty" table for
	 */
	@Override
	public void createDirtyRowTable(TableInfo ti, String tableName) {
		if (!dirty_table_created) {
			String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (tablename TEXT, replica TEXT, keyset TEXT, PRIMARY KEY(tablename, replica, keyset));", music_ns, DIRTY_TABLE);
			executeMusicWriteQuery(cql);
			dirty_table_created = true;
		}
	}
	/**
	 * Drop the dirty row table for <i>tableName</i> from MUSIC.
	 * @param tableName the table being dropped
	 */
	@Override
	public void dropDirtyRowTable(String tableName) {
		// no-op
	}

	private String buildJSON(TableInfo ti, String tableName, Object[] keys) {
		// Build JSON string representing this keyset
		JSONObject jo = new JSONObject();
		int j = 0;
		for (int i = 0; i < ti.columns.size(); i++) {
			if (ti.iskey.get(i)) {
				jo.put(ti.columns.get(i), keys[j++]);
			}
		}
		return jo.toString();
	}
	/**
	 * Remove the entries from the dirty row (for this replica) that correspond to a set of primary keys
	 * @param tableName the table we are removing dirty entries from
	 * @param keys the primary key values to use in the DELETE.  Note: this is *only* the primary keys, not a full table row.
	 */
	@Override
	public void cleanDirtyRow(TableInfo ti, String tableName, JSONObject keys) {
		String cql = String.format("DELETE FROM %s.%s WHERE tablename = ? AND replica = ? AND keyset = ?;", music_ns, DIRTY_TABLE);
		//Session sess = getMusicSession();
		//PreparedStatement ps = getPreparedStatementFromCache(cql);
		Object[] values = new Object[] { tableName, myId, keys };
		logger.debug(EELFLoggerDelegate.applicationLogger,"Executing MUSIC write:"+ cql + " with values " + values[0] + " " + values[1] + " " + values[2]);
		
		PreparedQueryObject pQueryObject = new PreparedQueryObject();
		pQueryObject.appendQueryString(cql);
		pQueryObject.addValue(tableName);
		pQueryObject.addValue(myId);
		pQueryObject.addValue(keys);
		ReturnType rt = MusicCore.eventualPut(pQueryObject);
		if(rt.getResult().getResult().toLowerCase().equals("failure")) {
			logger.error(EELFLoggerDelegate.errorLogger, "Failure while eventualPut...: "+rt.getMessage());
		}
		/*BoundStatement bound = ps.bind(values);
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
	@SuppressWarnings("deprecation")
	@Override
	public List<Map<String,Object>> getDirtyRows(TableInfo ti, String tableName) {
		String cql = String.format("SELECT keyset FROM %s.%s WHERE tablename = ? AND replica = ?;", music_ns, DIRTY_TABLE);
		logger.debug(EELFLoggerDelegate.applicationLogger,"Executing MUSIC write:"+ cql + " with values " + tableName + " " + myId);
		
		PreparedQueryObject pQueryObject = new PreparedQueryObject();
		pQueryObject.appendQueryString(cql);
		pQueryObject.addValue(tableName);
		pQueryObject.addValue(myId);
		ResultSet results = null;
		try {
			results = MusicCore.get(pQueryObject);
		} catch (MusicServiceException e) {
			e.printStackTrace();
		}
		/*Session sess = getMusicSession();
		PreparedStatement ps = getPreparedStatementFromCache(cql);
		BoundStatement bound = ps.bind(new Object[] { tableName, myId });
		bound.setReadTimeoutMillis(60000);
		ResultSet results = null;
		synchronized (sess) {
			results = sess.execute(bound);
		}*/
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		for (Row row : results) {
			String json = row.getString("keyset");
			JSONObject jo = new JSONObject(new JSONTokener(json));
			Map<String,Object> objs = new HashMap<String,Object>();
			for (String colname : jo.keySet()) {
				int coltype = ti.getColType(colname);
				switch (coltype) {
				case Types.BIGINT:
					objs.put(colname, jo.getLong(colname));
					break;
				case Types.BOOLEAN:
					objs.put(colname, jo.getBoolean(colname));
					break;
				case Types.BLOB:
					logger.error(EELFLoggerDelegate.errorLogger,"WE DO NOT SUPPORT BLOBS AS PRIMARY KEYS!! COLUMN NAME="+colname);
					// throw an exception here???
					break;
				case Types.DOUBLE:
					objs.put(colname, jo.getDouble(colname));
					break;
				case Types.INTEGER:
					objs.put(colname, jo.getInt(colname));
					break;
				case Types.TIMESTAMP:
					objs.put(colname, new Date(jo.getString(colname)));
					break;
				case Types.VARCHAR:
				default:
					objs.put(colname, jo.getString(colname));
					break;
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
		super.clearMusicForTable(tableName);
	}
	/**
	 * This function is called whenever there is a DELETE to a row on a local SQL table, wherein it updates the
	 * MUSIC/Cassandra tables (both dirty bits and actual data) corresponding to the SQL write. MUSIC propagates
	 * it to the other replicas.
	 *
	 * @param tableName This is the table that has changed.
	 * @param oldRow This is a copy of the old row being deleted
	 */
	public void deleteFromEntityTableInMusic(TableInfo ti, String tableName, JSONObject oldRow) {
		super.deleteFromEntityTableInMusic(ti, tableName, oldRow);
	}
	/**
	 * This method is called whenever there is a SELECT on a local SQL table, wherein it first checks the local
	 * dirty bits table to see if there are any keys in Cassandra whose value has not yet been sent to SQL
	 * @param tableName This is the table on which the select is being performed
	 */
	@Override
	public void readDirtyRowsAndUpdateDb(DBInterface dbi, String tableName) {
		super.readDirtyRowsAndUpdateDb(dbi, tableName);
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
		super.updateDirtyRowAndEntityTableInMusic(ti, tableName, changedRow);
	}
	
	/**
	 * Mark rows as "dirty" in the dirty rows table for <i>tableName</i>.  Rows are marked for all replicas but
	 * this one (this replica already has the up to date data).
	 * @param tableName the table we are marking dirty
	 * @param keys an ordered list of the values being put into the table.  The values that correspond to the tables'
	 * primary key are copied into the dirty row table.
	 */
	@Deprecated
	public void markDirtyRow(TableInfo ti, String tableName, Object[] keys) {
		String cql = String.format("INSERT INTO %s.%s (tablename, replica, keyset) VALUES (?, ?, ?);", music_ns, DIRTY_TABLE);
		/*Session sess = getMusicSession();
		PreparedStatement ps = getPreparedStatementFromCache(cql);*/
		@SuppressWarnings("unused")
		Object[] values = new Object[] { tableName, "", buildJSON(ti, tableName, keys) };
		PreparedQueryObject pQueryObject = null;
		for (String repl : allReplicaIds) {
			/*if (!repl.equals(myId)) {
				values[1] = repl;
				logger.info(EELFLoggerDelegate.applicationLogger,"Executing MUSIC write:"+ cql + " with values " + values[0] + " " + values[1] + " " + values[2]);
				
				BoundStatement bound = ps.bind(values);
				bound.setReadTimeoutMillis(60000);
				synchronized (sess) {
					sess.execute(bound);
				}
			}*/
			pQueryObject = new PreparedQueryObject();
			pQueryObject.appendQueryString(cql);
			pQueryObject.addValue(tableName);
			pQueryObject.addValue(repl);
			pQueryObject.addValue(buildJSON(ti, tableName, keys));
			ReturnType rt = MusicCore.eventualPut(pQueryObject);
			if(rt.getResult().getResult().toLowerCase().equals("failure")) {
				System.out.println("Failure while critical put..."+rt.getMessage());
			}
		}
	}
}
