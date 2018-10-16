package com.att.research.mdbc;

import org.json.JSONObject;

import com.att.research.mdbc.mixins.DBInterface;
import com.att.research.mdbc.mixins.MusicInterface;

public class ArchiveProcess {
	protected MusicInterface mi;
	protected DBInterface dbi;

	//TODO: This is a place holder for taking snapshots and moving data from redo record into actual tables
	
	/**
	 * This method is called whenever there is a DELETE on a local SQL table, and should be called by the underlying databases
	 * triggering mechanism. It updates the MUSIC/Cassandra tables (both dirty bits and actual data) corresponding to the SQL DELETE.
	 * Music propagates it to the other replicas.  If the local database is in the middle of a transaction, the DELETEs to MUSIC are
	 * delayed until the transaction is either committed or rolled back.
	 * @param tableName This is the table on which the select is being performed
	 * @param oldRow This is information about the row that is being deleted
	 */
	@SuppressWarnings("unused")
	private void deleteFromEntityTableInMusic(String tableName, JSONObject oldRow) {
			TableInfo ti = dbi.getTableInfo(tableName);
			mi.deleteFromEntityTableInMusic(ti,tableName, oldRow);
	}
	
	/**
	 * This method is called whenever there is an INSERT or UPDATE to a local SQL table, and should be called by the underlying databases
	 * triggering mechanism. It updates the MUSIC/Cassandra tables (both dirty bits and actual data) corresponding to the SQL write.
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
