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

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import org.onap.music.mdbc.LockId;
import org.json.JSONObject;
import org.onap.music.exceptions.MusicLockingException;

import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.TableInfo;
import org.onap.music.mdbc.tables.MusicTxDigestId;
import org.onap.music.mdbc.tables.StagingTable;
import org.onap.music.mdbc.tables.MriReference;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.TxCommitProgress;

import org.onap.music.main.MusicCore;

/**

 *
 */
public class MusicMixin implements MusicInterface {

	public static Map<Integer, Set<String>> currentLockMap = new HashMap<>();
	public static List<String> criticalTables = new ArrayList<>();

	@Override
	public String getMixinName() {
		// 
		return null;
	}

	@Override
	public String getMusicDefaultPrimaryKeyName() {
		// 
		return null;
	}

	@Override
	public String generateUniqueKey() {
		// 
		return null;
	}

	@Override
	public String getMusicKeyFromRow(TableInfo ti, String table, JSONObject dbRow) {
		// 
		return null;
	}

	@Override
	public void close() {
		// 

	}

	@Override
	public void createKeyspace() {
		// 

	}

	@Override
	public void initializeMusicForTable(TableInfo ti, String tableName) {
		// 

	}

	@Override
	public void createDirtyRowTable(TableInfo ti, String tableName) {
		// 

	}

	@Override
	public void dropDirtyRowTable(String tableName) {
		// 

	}

	@Override
	public void clearMusicForTable(String tableName) {
		// 

	}

	@Override
	public void markDirtyRow(TableInfo ti, String tableName, JSONObject keys) {
		// 

	}

	@Override
	public void cleanDirtyRow(TableInfo ti, String tableName, JSONObject keys) {
		// 

	}

	@Override
	public List<Map<String, Object>> getDirtyRows(TableInfo ti, String tableName) {
		// 
		return null;
	}

	@Override
	public void deleteFromEntityTableInMusic(TableInfo ti, String tableName, JSONObject oldRow) {
		// 

	}

	@Override
	public void readDirtyRowsAndUpdateDb(DBInterface dbi, String tableName) {
		// 

	}

	@Override
	public void updateDirtyRowAndEntityTableInMusic(TableInfo ti, String tableName, JSONObject changedRow) {
		updateDirtyRowAndEntityTableInMusic(tableName, changedRow, false);

	}

	public void updateDirtyRowAndEntityTableInMusic(String tableName, JSONObject changedRow, boolean isCritical) {
	}

	public static void releaseZKLocks(Set<LockId> lockIds) {
		for (LockId lockId : lockIds) {
			System.out.println("Releasing lock: " + lockId);
			try {
				MusicCore.voluntaryReleaseLock(lockId.getFullyQualifiedLockKey(), lockId.getLockReference());
				MusicCore.destroyLockRef(lockId.getFullyQualifiedLockKey(), lockId.getLockReference());
			} catch (MusicLockingException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public String getMusicKeyFromRowWithoutPrimaryIndexes(TableInfo ti, String tableName, JSONObject changedRow) {
		// 
		return null;
	}

	@Override
	public void initializeMetricDataStructures() {
		// 

	}

	@Override
	public Object[] getObjects(TableInfo ti, String tableName, JSONObject row) {
		return null;
	}

	@Override
	public void commitLog(DBInterface dbi, DatabasePartition partition, HashMap<Range, StagingTable> transactionDigest, String txId, TxCommitProgress progressKeeper)
			throws MDBCServiceException {
		// TODO Auto-generated method stub
	}

	@Override
	public HashMap<Range, StagingTable> getTxDigest(MusicTxDigestId id) {
		return null;
	}


	@Override
	public DatabasePartition createMusicRangeInformation(MusicRangeInformationRow info) {
		return null;
	}

	@Override
	public void appendToRedoLog(UUID mriRowId, DatabasePartition partition, MusicTxDigestId newRecord) {
	}

	@Override
	public void addTxDigest(MusicTxDigestId newId, String transactionDigest) {
	}

	@Override
	public void own(List<Range> ranges) {
		throw new java.lang.UnsupportedOperationException("function not implemented yet");
	}

	@Override
	public void appendRange(String rangeId, List<Range> ranges) {
		throw new java.lang.UnsupportedOperationException("function not implemented yet");
	}

	@Override
	public void relinquish(String ownerId, String rangeId) {
		throw new java.lang.UnsupportedOperationException("function not implemented yet");
	}

	@Override
	public List<UUID> getPartitionIndexes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MusicRangeInformationRow getMusicRangeInformation(UUID partitionIndex) throws MDBCServiceException {
		// TODO Auto-generated method stub
		return null;
	}
}
