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

import org.json.JSONObject;

import org.onap.music.mdbc.mixins.DBInterface;
import org.onap.music.mdbc.mixins.MusicInterface;

public class ArchiveProcess {
	protected MusicInterface mi;
	protected DBInterface dbi;

	//TODO: This is a place holder for taking snapshots and moving data from redo record into actual tables
	
	/**
	 * This method is called whenever there is a DELETE on the transaction digest and should be called when ownership changes, if required
	 *  It updates the MUSIC/Cassandra tables (both dirty bits and actual data) corresponding to the SQL DELETE.
	 * Music propagates it to the other replicas.
	 * @param tableName This is the table on which the select is being performed
	 * @param oldRow This is information about the row that is being deleted
	 */
	@SuppressWarnings("unused")
	private void deleteFromEntityTableInMusic(String tableName, JSONObject oldRow) {
			TableInfo ti = dbi.getTableInfo(tableName);
			mi.deleteFromEntityTableInMusic(ti,tableName, oldRow);
	}
	
	/**
	 * This method is called whenever there is an INSERT or UPDATE to a the transaction digest, and should be called by an
	 * ownership chance. It updates the MUSIC/Cassandra tables (both dirty bits and actual data) corresponding to the SQL write.
	 * Music propagates it to the other replicas.  If the local database is in the middle of a transaction, the updates to MUSIC are
	 * delayed until the transaction is either committed or rolled back.
	 *
	 * @param tableName This is the table that has changed.
	 * @param changedRow This is information about the row that has changed, an array of objects representing the data being inserted/updated
	 */
	@SuppressWarnings("unused")
	private void updateDirtyRowAndEntityTableInMusic(String tableName, JSONObject changedRow) {
		//TODO: is this right? should we be saving updates at the client? we should leverage JDBC to handle this
			TableInfo ti = dbi.getTableInfo(tableName);
			mi.updateDirtyRowAndEntityTableInMusic(ti,tableName, changedRow);
	}
}
