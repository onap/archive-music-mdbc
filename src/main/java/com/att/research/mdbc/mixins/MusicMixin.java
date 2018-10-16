package com.att.research.mdbc.mixins;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.att.research.mdbc.LockId;
import org.json.JSONObject;
import org.onap.music.exceptions.MusicLockingException;

import com.att.research.exceptions.MDBCServiceException;
import com.att.research.mdbc.DatabasePartition;
import com.att.research.mdbc.Range;
import com.att.research.mdbc.TableInfo;
import org.onap.music.main.MusicPureCassaCore;

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
	public void markDirtyRow(TableInfo ti, String tableName,  JSONObject keys) {
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
	
	public void updateDirtyRowAndEntityTableInMusic(String tableName, JSONObject changedRow, boolean isCritical) { }
	

	public static void loadProperties() {
	    Properties prop = new Properties();
	    InputStream input = null;
	    try {
	      input = MusicMixin.class.getClassLoader().getResourceAsStream("mdbc.properties");
	      prop.load(input);
	      String crTable = prop.getProperty("critical.tables");
	      String[] tableArr = crTable.split(",");
	      criticalTables = Arrays.asList(tableArr);
	      
	    }
	    catch (Exception ex) {
	      ex.printStackTrace();
	    }
	    finally {
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
		for(LockId lockId: lockIds) {
			System.out.println("Releasing lock: "+lockId);
			try {
				MusicPureCassaCore.voluntaryReleaseLock(lockId.getFullyQualifiedLockKey(),lockId.getLockReference());
				MusicPureCassaCore.destroyLockRef(lockId.getFullyQualifiedLockKey(),lockId.getLockReference());
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
	public void initializeMdbcDataStructures() {
		// 
		
	}

	@Override
	public Object[] getObjects(TableInfo ti, String tableName, JSONObject row) {
		return null;
	}

	@Override
	public void commitLog(DBInterface dbi, DatabasePartition partition, HashMap<Range,StagingTable> transactionDigest, String txId,TxCommitProgress progressKeeper) 
			throws MDBCServiceException{
		// TODO Auto-generated method stub
	}

	@Override
    public TablePartitionInformation getTablePartitionInformation(String table){
	    return null;
    }

    @Override
    public HashMap<Range,StagingTable> getTransactionDigest(RedoRecordId id){
	    return null;
    }

    @Override
    public 	TransactionInformationElement getTransactionInformation(String id){
	    return null;
    }

    @Override
    public 	void updateTitReference(String partition, TitReference tit){}

    @Override
    public 	List<RedoHistoryElement> getHistory(DatabasePartition partition){
	   return null;
    }

    @Override
    public 	void addRedoHistory(DatabasePartition partition, TitReference newTit, List<TitReference> old){
    }

    @Override
    public 	TitReference createPartition(List<String> tables, int replicationFactor, String currentOwner){
	   return null;
    }

    @Override
    public 	List<PartitionInformation> getPartitionInformation(DatabasePartition partition){
	   return null;
    }

    @Override
	public TitReference createTransactionInformationRow(TransactionInformationElement info){
	   return null;
    }

    @Override
	public void appendToRedoLog(TitReference titRow, DatabasePartition partition, RedoRecordId newRecord){
    }

    @Override
	public void appendRedoRecord(String redoRecordTable, RedoRecordId newRecord, String transactionDigest){
    }

    @Override
	public void updateTablePartition(String table, DatabasePartition partition){}

	@Override
	public void updatePartitionOwner(String partition, String owner){}

	@Override
	public void updatePartitionReplicationFactor(String partition, int replicationFactor){}
}
