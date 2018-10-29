package org.onap.music.mdbc.mixins;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.json.JSONObject;

import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.TableInfo;
import org.onap.music.mdbc.tables.PartitionInformation;
import org.onap.music.mdbc.tables.MusicTxDigestId;
import org.onap.music.mdbc.tables.StagingTable;
import org.onap.music.mdbc.tables.MriReference;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.TxCommitProgress;

/**
 * This Interface defines the methods that MDBC needs for a class to provide access to the persistence layer of MUSIC.
 *
 * @author Robert P. Eby
 */
public interface MusicInterface {	
	/**
	 * This function is used to created all the required data structures, both local  
	 * \TODO Check if this function is required in the MUSIC interface or could be just created on the constructor
	 */
	void initializeMetricDataStructures() throws MDBCServiceException;
	/**
	 * Get the name of this MusicInterface mixin object.
	 * @return the name
	 */
	String getMixinName();
	/**
	 * Gets the name of this MusicInterface mixin's default primary key name
	 * @return default primary key name
	 */
	String getMusicDefaultPrimaryKeyName();
	/**
	 * generates a key or placeholder for what is required for a primary key
	 * @return a primary key
	 */
	String generateUniqueKey();

	/**
	 * Find the key used with Music for a table that was created without a primary index
	 * Name is long to avoid developers using it. For cassandra performance in this operation
	 * is going to be really bad
	 * @param ti information of the table in the SQL layer
	 * @param table name of the table
	 * @param dbRow row obtained from the SQL layer
	 * @return key associated with the row
	 */
	String getMusicKeyFromRowWithoutPrimaryIndexes(TableInfo ti, String table, JSONObject dbRow);
	/**
	 * Do what is needed to close down the MUSIC connection.
	 */
	void close();
	/**
	 * This method creates a keyspace in Music/Cassandra to store the data corresponding to the SQL tables.
	 * The keyspace name comes from the initialization properties passed to the JDBC driver.
	 */
	void createKeyspace();
	/**
	 * This method performs all necessary initialization in Music/Cassandra to store the table <i>tableName</i>.
	 * @param tableName the table to initialize MUSIC for
	 */
	void initializeMusicForTable(TableInfo ti, String tableName);
	/**
	 * Create a <i>dirty row</i> table for the real table <i>tableName</i>.  The primary keys columns from the real table are recreated in
	 * the dirty table, along with a "REPLICA__" column that names the replica that should update it's internal state from MUSIC.
	 * @param tableName the table to create a "dirty" table for
	 */
	void createDirtyRowTable(TableInfo ti, String tableName);
	/**
	 * Drop the dirty row table for <i>tableName</i> from MUSIC.
	 * @param tableName the table being dropped
	 */
	void dropDirtyRowTable(String tableName);
	/**
	 * Drops the named table and its dirty row table (for all replicas) from MUSIC.  The dirty row table is dropped first.
	 * @param tableName This is the table that has been dropped
	 */
	void clearMusicForTable(String tableName);
	/**
	 * Mark rows as "dirty" in the dirty rows table for <i>tableName</i>.  Rows are marked for all replicas but
	 * this one (this replica already has the up to date data).
	 * @param tableName the table we are marking dirty
	 * @param keys an ordered list of the values being put into the table.  The values that correspond to the tables'
	 * primary key are copied into the dirty row table.
	 */
	void markDirtyRow(TableInfo ti, String tableName, JSONObject keys);
	/**
	 * Remove the entries from the dirty row (for this replica) that correspond to a set of primary keys
	 * @param tableName the table we are removing dirty entries from
	 * @param keys the primary key values to use in the DELETE.  Note: this is *only* the primary keys, not a full table row.
	 */
	void cleanDirtyRow(TableInfo ti, String tableName, JSONObject keys);
	/**
	 * Get a list of "dirty rows" for a table.  The dirty rows returned apply only to this replica,
	 * and consist of a Map of primary key column names and values.
	 * @param tableName the table we are querying for
	 * @return a list of maps; each list item is a map of the primary key names and values for that "dirty row".
	 */
	List<Map<String,Object>> getDirtyRows(TableInfo ti, String tableName);
	/**
	 * This method is called whenever there is a DELETE to a row on a local SQL table, wherein it updates the
	 * MUSIC/Cassandra tables (both dirty bits and actual data) corresponding to the SQL write. MUSIC propagates
	 * it to the other replicas.
	 * @param tableName This is the table that has changed.
	 * @param oldRow This is a copy of the old row being deleted
	 */
	void deleteFromEntityTableInMusic(TableInfo ti,String tableName, JSONObject oldRow);
	/**
	 * This method is called whenever there is a SELECT on a local SQL table, wherein it first checks the local
	 * dirty bits table to see if there are any rows in Cassandra whose value needs to be copied to the local SQL DB.
	 * @param tableName This is the table on which the select is being performed
	 */
	void readDirtyRowsAndUpdateDb(DBInterface dbi, String tableName);
	/**
	 * This method is called whenever there is an INSERT or UPDATE to a local SQL table, wherein it updates the
	 * MUSIC/Cassandra tables (both dirty bits and actual data) corresponding to the SQL write. Music propagates
	 * it to the other replicas.
	 * @param tableName This is the table that has changed.
	 * @param changedRow This is information about the row that has changed
	 */
	void updateDirtyRowAndEntityTableInMusic(TableInfo ti, String tableName, JSONObject changedRow);
	
	Object[] getObjects(TableInfo ti, String tableName, JSONObject row);
	/**
	 * Returns the primary key associated with the given row 
	 * @param ti info of the table that is associated with the row
	 * @param tableName name of the table that contains the row
	 * @param changedRow row that is going to contain the information associated with the primary key
	 * @return primary key of the row
	 */
	String getMusicKeyFromRow(TableInfo ti, String tableName, JSONObject changedRow);

	/**
	 * Commits the corresponding REDO-log into MUSIC
	 *
	 * @param dbi, the database interface use in the local SQL cache, where the music interface is being used
	 * @param partition
	 * @param transactionDigest digest of the transaction that is being committed into the Redo log in music. It has to be a HashMap, because it is required to be serializable
	 * @param txId id associated with the log being send
	 * @param progressKeeper data structure that is used to handle to detect failures, and know what to do
	 * @throws MDBCServiceException
	 */
	void commitLog(DBInterface dbi, DatabasePartition partition, HashMap<Range,StagingTable> transactionDigest, String txId,TxCommitProgress progressKeeper) throws MDBCServiceException;
	
	MusicRangeInformationRow getMusicRangeInformation(DatabasePartition partition) throws MDBCServiceException;

	DatabasePartition createMusicRangeInformation(MusicRangeInformationRow info) throws MDBCServiceException;
	
	void appendToRedoLog(MriReference mriRowId, DatabasePartition partition, MusicTxDigestId newRecord) throws MDBCServiceException;
	
	void addTxDigest(String musicTxDigestTable, MusicTxDigestId newId, String transactionDigest) throws MDBCServiceException;

	PartitionInformation getPartitionInformation(DatabasePartition partition) throws MDBCServiceException;
	
	HashMap<Range,StagingTable> getTransactionDigest(MusicTxDigestId id) throws MDBCServiceException;

	void own(List<Range> ranges);

	void appendRange(String rangeId, List<Range> ranges);

	void relinquish(String ownerId, String rangeId);

}

