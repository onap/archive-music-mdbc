package com.att.research.mdbc.tables;

import java.math.BigInteger;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import com.datastax.driver.core.utils.UUIDs;

import com.att.research.logging.EELFLoggerDelegate;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicReference;


public class TxCommitProgress{
	private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(TxCommitProgress.class);

	private Map<String, CommitProgress> transactionInfo;

	public TxCommitProgress(){
		transactionInfo = new ConcurrentHashMap<>();
	}
	
	public boolean containsTx(String txId) {
		return transactionInfo.containsKey(txId);
	}
	
	public UUID getCommitId(String txId) {
		CommitProgress prog = transactionInfo.get(txId);
		if(prog.isCommitIdAssigned()) {
			return prog.getCommitId();
		}
		UUID commitId = UUIDs.random();
		prog.setCommitId(commitId);
		return commitId;
	}
	
	public void createNewTransactionTracker(String id, Connection conn) {
		transactionInfo.put(id, new CommitProgress(id,conn));
	}
	
	public void commitRequested(String txId) {
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when storing commit request",txId);
		}
		prog.setCommitRequested();
	}
	
	public void setSQLDone(String txId) {
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when storing saving completion of SQL",txId);
		}
		prog.setSQLCompleted();
	}

	public void setMusicDone(String txId) {
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when storing saving completion of Music",txId);
		}
		prog.setMusicCompleted();
	}
	
	public Connection getConnection(String txId){
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when retrieving statement",txId);
		}
		return prog.getConnection();
	}
	
	public void setRecordId(String txId, MusixTxDigestId recordId){
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when setting record Id",txId);
		}
		prog.setRecordId(recordId);
	}
	
	public MusixTxDigestId getRecordId(String txId) {
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when getting record Id",txId);
		}
		return prog.getRecordId();
	}
	
	public boolean isRecordIdAssigned(String txId) {
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when checking record",txId);
		}
		return prog.isRedoRecordAssigned();
	}
	
	public boolean isComplete(String txId) {
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when checking completion",txId);
		}
		return prog.isComplete();
	}
	
	public void reinitializeTxProgress(String txId) {
		CommitProgress prog = transactionInfo.get(txId);
		if(prog == null){
			logger.error(EELFLoggerDelegate.errorLogger, "Transaction doesn't exist: [%l], failure when reinitializing tx progress",txId);
		}
		prog.reinitialize();
	}

	public void deleteTxProgress(String txId){
		transactionInfo.remove(txId);
	}
}

final class CommitProgress{
	private String lTxId; // local transaction id  
	private UUID commitId; // commit id
	private boolean commitRequested; //indicates if the user tried to commit the request already.
	private boolean SQLDone; // indicates if SQL was already committed 
	private boolean MusicDone; // indicates if music commit was already performed, atomic bool
	private Connection connection;// reference to a connection object. This is used to complete a commit if it failed in the original thread.
	private Long timestamp; // last time this data structure was updated
	private MusixTxDigestId musixTxDigestId;// record id for each partition

	public CommitProgress(String id,Connection conn){
		musixTxDigestId =null;
		lTxId = id;
		commitRequested = false;
		SQLDone = false;
		MusicDone = false;
		connection = conn;
	    commitId = null;
		timestamp = System.currentTimeMillis();
	}
	
	public synchronized boolean isComplete() {
		return commitRequested && SQLDone && MusicDone;
	}
	
	public synchronized void setCommitId(UUID commitId) {
		this.commitId = commitId;
		timestamp = System.currentTimeMillis();
	}
	
	public synchronized void reinitialize() {
		commitId = null;
		musixTxDigestId =null;
		commitRequested = false;
		SQLDone = false;
		MusicDone = false;
		timestamp = System.currentTimeMillis();
	}

	public synchronized void setCommitRequested() {
		commitRequested = true;
		timestamp = System.currentTimeMillis();
	}

	public synchronized void setSQLCompleted() {
		SQLDone = true;
		timestamp = System.currentTimeMillis();
	}
	
	public synchronized void setMusicCompleted() {
		MusicDone = true;
		timestamp = System.currentTimeMillis();
	}
	
	public Connection getConnection() {
		timestamp = System.currentTimeMillis();
		return connection;
	} 
	
	public long getTimestamInMillis() {
		return timestamp;
	}

	public synchronized void setRecordId(MusixTxDigestId id) {
		musixTxDigestId =  id;
		timestamp = System.currentTimeMillis();
	}
	
	public synchronized boolean isRedoRecordAssigned() {
		return this.musixTxDigestId !=null;
	} 

	public synchronized MusixTxDigestId getRecordId() {
		return musixTxDigestId;
	} 
	
	public synchronized UUID getCommitId() {
		return commitId;
	}
	
	public synchronized String getId() {
		return this.lTxId;
	}
	
	public synchronized boolean isCommitIdAssigned() {
		return this.commitId!= null;
	}
}