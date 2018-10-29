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
import org.onap.music.mdbc.tables.PartitionInformation;
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


	public static void loadProperties() {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = MusicMixin.class.getClassLoader().getResourceAsStream("mdbc.properties");
			prop.load(input);
			String crTable = prop.getProperty("critical.tables");
			String[] tableArr = crTable.split(",");
			criticalTables = Arrays.asList(tableArr);

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
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
	public HashMap<Range, StagingTable> getTransactionDigest(MusicTxDigestId id) {
		return null;
	}

	@Override
	public PartitionInformation getPartitionInformation(DatabasePartition partition) {
		return null;
	}

	@Override
	public DatabasePartition createMusicRangeInformation(MusicRangeInformationRow info) {
		return null;
	}

	@Override
	public void appendToRedoLog(MriReference mriRowId, DatabasePartition partition, MusicTxDigestId newRecord) {
	}

	@Override
	public void addTxDigest(String musicTxDigestTable, MusicTxDigestId newId, String transactionDigest) {
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
	public MusicRangeInformationRow getMusicRangeInformation(DatabasePartition partition){
		return null;
	}
}
