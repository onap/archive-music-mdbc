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

import com.datastax.driver.core.ResultSet;
import java.util.*;
import org.json.JSONObject;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.TableInfo;
import org.onap.music.mdbc.query.SQLOperationType;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.MusicTxDigestId;
import org.onap.music.mdbc.tables.StagingTable;
import org.onap.music.mdbc.tables.TxCommitProgress;
import org.onap.music.mdbc.ownership.Dag;
import org.onap.music.mdbc.ownership.OwnershipAndCheckpoint;
import org.onap.music.mdbc.tables.*;

/**
 * This Interface defines the methods that MDBC needs for a class to provide access to the persistence layer of MUSIC.
 *
 * @author Robert P. Eby
 */
public interface MusicInterface {
	class OwnershipReturn{
	    private final UUID ownershipId;
		private final String ownerId;
		private final UUID rangeId;
		private final List<Range> ranges;
		private final Dag dag;
		public OwnershipReturn(UUID ownershipId, String ownerId, UUID rangeId, List<Range> ranges, Dag dag){
		    this.ownershipId=ownershipId;
			this.ownerId=ownerId;
			this.rangeId=rangeId;
			this.ranges=ranges;
			this.dag=dag;
		}
		public String getOwnerId(){
			return ownerId;
		}
		public UUID getRangeId(){
			return rangeId;
		}
		public List<Range> getRanges(){  return ranges; }
		public Dag getDag(){return dag;}
		public UUID getOwnershipId() { return ownershipId; }
	}
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
	UUID generateUniqueKey();

	/**
	 * Find the key used with Music for a table that was created without a primary index
	 * Name is long to avoid developers using it. For cassandra performance in this operation
	 * is going to be really bad
	 * @param ti information of the table in the SQL layer
	 * @param table name of the table
	 * @param dbRow row obtained from the SQL layer
	 * @return key associated with the row
	 */
	String getMusicKeyFromRowWithoutPrimaryIndexes(TableInfo ti, String table, JSONObject dbRow)
	;
	/**
	 * Do what is needed to close down the MUSIC connection.
	 */
	void close();
	/**
	 * This method creates a keyspace in Music/Cassandra to store the data corresponding to the SQL tables.
	 * The keyspace name comes from the initialization properties passed to the JDBC driver.
	 * @throws MusicServiceException 
	 */
	void createKeyspace() throws MDBCServiceException;
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
	 * @param partition information related to ownership of partitions, used to verify ownership when commiting the Tx
	 * @param eventualRanges 
	 * @param transactionDigest digest of the transaction that is being committed into the Redo log in music. It has to
     * be a HashMap, because it is required to be serializable
	 * @param txId id associated with the log being send
	 * @param progressKeeper data structure that is used to handle to detect failures, and know what to do
	 * @throws MDBCServiceException
	 */
	void commitLog(DatabasePartition partition, List<Range> eventualRanges, HashMap<Range,StagingTable> transactionDigest, String txId,TxCommitProgress progressKeeper) throws MDBCServiceException;
	

    /**
     * This function is used to obtain the information related to a specific row in the MRI table
     * @param partitionIndex index of the row that is going to be retrieved
     * @return all the information related to the table
     * @throws MDBCServiceException
     */
	MusicRangeInformationRow getMusicRangeInformation(UUID partitionIndex) throws MDBCServiceException;

    /**
     * This function is used to get the dependencies of a given range
     * @param baseRange range for which we search the dependencies
     * @return dependencies
     * @throws MDBCServiceException
     */
	RangeDependency getMusicRangeDependency(Range baseRange) throws MDBCServiceException;

	/**
     * This function is used to create a new row in the MRI table
     * @param info the information used to create the row
     * @return the new partition object that contain the new information used to create the row
     * @throws MDBCServiceException
     */
	DatabasePartition createMusicRangeInformation(MusicRangeInformationRow info) throws MDBCServiceException;

    /**
     * This function is used to create all the required music dependencies
     * @param rangeAndDependencies
     * @throws MDBCServiceException
     */
	void createMusicRangeDependency(RangeDependency rangeAndDependencies) throws MDBCServiceException;

	/**
     * This function is used to append an index to the redo log in a MRI row
     * @param partition information related to ownership of partitions, used to verify ownership
     * @param newRecord index of the new record to be appended to the redo log
     * @throws MDBCServiceException
     */
	void appendToRedoLog( DatabasePartition partition, MusicTxDigestId newRecord) throws MDBCServiceException;

    /**
     * This functions adds the tx digest to
     * @param newId id used as index in the MTD table
     * @param transactionDigest digest that contains all the changes performed in the transaction
     * @throws MDBCServiceException
     */
	void addTxDigest(MusicTxDigestId newId, String transactionDigest) throws MDBCServiceException;
	
	/**
     * This functions adds the eventual tx digest to
     * @param newId id used as index in the MTD table
     * @param transactionDigest digest that contains all the changes performed in the transaction
     * @throws MDBCServiceException
     */
	
    void addEventualTxDigest(MusicTxDigestId newId, String transactionDigest)
            throws MDBCServiceException;

    /**
     * Function used to retrieve a given transaction digest and deserialize it
     * @param id of the transaction digest to be retrieved
     * @return the deserialize transaction digest that can be applied to the local SQL database
     * @throws MDBCServiceException
     */
	HashMap<Range,StagingTable> getTxDigest(MusicTxDigestId id) throws MDBCServiceException;
	
	/**
     * Function used to retrieve a given eventual transaction digest for the current node and deserialize it
     * @param nodeName that identifies a node
     * @return the deserialize transaction digest that can be applied to the local SQL database
     * @throws MDBCServiceException
     */
	
	public LinkedHashMap<UUID, HashMap<Range,StagingTable>> getEveTxDigest(String nodeName) throws MDBCServiceException;

    /**
     * Use this functions to verify ownership, and own new ranges
     * @param ranges the ranges that should be own after calling this function
     * @param partition current information of the ownership in the system
     * @param ownOpId is the id used to describe this ownership operation (it is not used to create the new row, if any is
     *                required
     * @param lockType - the type of ownership that is needed, read or write lock
	 * @return an object indicating the status of the own function result
     * @throws MDBCServiceException
     */
	public OwnershipReturn own(List<Range> ranges, DatabasePartition partition, UUID ownOpId, SQLOperationType lockType) throws MDBCServiceException;

    /**
     * This function relinquish ownership, if it is time to do it, it should be used at the end of a commit operation
     * @param partition information of the partition that is currently being owned
     * @throws MDBCServiceException
     */
	void relinquishIfRequired(DatabasePartition partition) throws MDBCServiceException;

    /**
	 * This function is in charge of owning all the ranges requested and creating a new row that show the ownership of all
	 * those ranges.
	 * @param rangeId new id to be used in the new row
	 * @param ranges ranges to be owned by the end of the function called
	 * @param partition current ownership status
	 * @return
	 * @throws MDBCServiceException
	 */
	//OwnershipReturn appendRange(String rangeId, List<Range> ranges, DatabasePartition partition) throws MDBCServiceException;

	/**
     * This functions relinquishes a range
     * @param ownerId id of the current ownerh
     * @param rangeId id of the range to be relinquished
     * @throws MusicLockingException
     */
	void relinquish(String ownerId, String rangeId) throws MDBCServiceException;

    /**
     * This function return all the range indexes that are currently hold by any of the connections in the system
     * @return list of ids of rows in MRI
     */
	List<UUID> getPartitionIndexes() throws MDBCServiceException;

    /**
     * This function is in charge of applying the transaction digests to the MUSIC tables.
     * @param digest this contain all the changes that were perfomed in this digest
     * @throws MDBCServiceException
     */
    void replayTransaction(HashMap<Range,StagingTable> digest) throws MDBCServiceException;

    /**
     * This function is in charge of deleting old mri rows that are not longer contain
     * @param oldRowsAndLocks is a map
     * @throws MDBCServiceException
     */
    void deleteOldMriRows(Map<UUID,String> oldRowsAndLocks) throws MDBCServiceException;

    List<MusicRangeInformationRow> getAllMriRows() throws MDBCServiceException;

    OwnershipAndCheckpoint getOwnAndCheck();

    void reloadAlreadyApplied(DatabasePartition partition) throws MDBCServiceException;
    
    public void updateNodeInfoTableWithTxTimeIDKey(UUID txTimeID, String nodeName) throws MDBCServiceException;
}

