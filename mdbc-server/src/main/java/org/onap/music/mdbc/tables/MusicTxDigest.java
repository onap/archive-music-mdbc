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
package org.onap.music.mdbc.tables;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.MDBCUtils;
import org.onap.music.mdbc.MdbcServerLogic;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.StateManager;
import org.onap.music.mdbc.configurations.NodeConfiguration;
import org.onap.music.mdbc.mixins.MusicMixin;
import org.onap.music.mdbc.mixins.MusicInterface;

import com.datastax.driver.core.Row;

public class MusicTxDigest {
	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicTxDigest.class);
	
	//private MdbcServerLogic mdbcServer;
	//private NodeConfiguration config;
	private StateManager stateManager;
	
	public MusicTxDigest(StateManager stateManager) {
		this.stateManager = stateManager;
	}

	/**
	 * Parse the transaction digest into individual events
	 * @param digest - base 64 encoded, serialized digest
	 */
	public void replayTxDigest(HashMap<Range,StagingTable> digest) {
		for (Map.Entry<Range,StagingTable> entry: digest.entrySet()) {
    		Range r = entry.getKey();
    		StagingTable st = entry.getValue();
    		ArrayList<Operation> opList = st.getOperationList();
			
    		for (Operation op: opList) {
    			replayOperation(r, op);
    		}
    	}
    }
    
    /**
     * Replays operation into local database
     * @param r
     * @param op
     */
    private void replayOperation(Range r, Operation op) {
		logger.info("Operation: " + op.getOperationType() + "->" + op.getNewVal());
		JSONObject jsonOp = op.getNewVal();
		JSONObject key = op.getKey();
		
		ArrayList<String> cols = new ArrayList<String>();
		ArrayList<Object> vals = new ArrayList<Object>();
		Iterator<String> colIterator = jsonOp.keys();
		while(colIterator.hasNext()) {
			String col = colIterator.next();
			//FIXME: should not explicitly refer to cassandramixin
			if (col.equals(MusicMixin.MDBC_PRIMARYKEY_NAME)) {
				//reserved name
				continue;
			}
			cols.add(col);
			vals.add(jsonOp.get(col));
		}
		
		//build the queries
		StringBuilder sql = new StringBuilder();
		String sep = "";
		switch (op.getOperationType()) {
		case INSERT:
			sql.append(op.getOperationType() + " INTO ");
			sql.append(r.table + " (") ;
			sep = "";
			for (String col: cols) {
				sql.append(sep + col);
				sep = ", ";
			}	
			sql.append(") VALUES (");
			sep = "";
			for (Object val: vals) {
				sql.append(sep + "\"" + val + "\"");
				sep = ", ";
			}
			sql.append(");");
			logger.info(sql.toString());
			break;
		case UPDATE:
			sql.append(op.getOperationType() + " ");
			sql.append(r.table + " SET ");
			sep="";
			for (int i=0; i<cols.size(); i++) {
				sql.append(sep + cols.get(i) + "=\"" + vals.get(i) +"\"");
				sep = ", ";
			}
			sql.append(" WHERE ");
			sql.append(getPrimaryKeyConditional(op.getKey()));
			sql.append(";");
			logger.info(sql.toString());
			break;
		case DELETE:
			sql.append(op.getOperationType() + " FROM ");
			sql.append(r.table + " WHERE ");
			sql.append(getPrimaryKeyConditional(op.getKey()));
			sql.append(";");
			logger.info(sql.toString());
			break;
		case SELECT:
			//no update happened, do nothing
			break;
		default:
			logger.error(op.getOperationType() + "not implemented for replay");
		}
    }
    
    private String getPrimaryKeyConditional(JSONObject primaryKeys) {
    	StringBuilder keyCondStmt = new StringBuilder();
    	String and = "";
    	for (String key: primaryKeys.keySet()) {
    		Object val = primaryKeys.get(key);
    		keyCondStmt.append(and + key + "=\"" + val + "\"");
    		and = " AND ";
    	}
		return keyCondStmt.toString();
	}

    /**
     * Runs the body of the background daemon
     * @param daemonSleepTimeS time, in seconds, between updates
     * @throws InterruptedException
     */
	public void backgroundDaemon(int daemonSleepTimeS) throws InterruptedException {
		MusicInterface mi = stateManager.getMusicInterface();
		while (true) {
			//update
			logger.info(String.format("[%s] Background MusicTxDigest daemon updating local db",
					new Timestamp(System.currentTimeMillis())));
			
			//1) get all other partitions from musicrangeinformation
			List<UUID> partitions = mi.getPartitionIndexes();
			//2) for each partition I don't own
			DatabasePartition myPartition = stateManager.getRanges();
			for (UUID partition: partitions) {
				if (!partition.equals(myPartition.getMusicRangeInformationIndex())){
					try {
						replayDigestForPartition(mi, partition);
					} catch (MDBCServiceException e) {
						logger.error("Unable to update for partition : " + partition + ". " + e.getMessage());
						continue;
					}
				}
			}
			Thread.sleep(TimeUnit.SECONDS.toMillis(daemonSleepTimeS));
		}
	}
	
	public void replayDigestForPartition(MusicInterface mi, UUID partitionId) throws MDBCServiceException {
		List<MusicTxDigestId> redoLogTxIds = mi.getMusicRangeInformation(partitionId).getRedoLog();
		for (MusicTxDigestId txId: redoLogTxIds) {
			HashMap<Range, StagingTable> digest = mi.getTxDigest(txId);
			replayTxDigest(digest);
		}
		//todo, keep track of where I am in pointer
	}

	/**
	 * Start the background daemon defined by this object
	 * Spawns a new thread and runs "backgroundDaemon"
	 * @param daemonSleepTimeS time, in seconds, between updates run by daemon
	 */
	public void startBackgroundDaemon(int daemonSleepTimeS) {
		class MusicTxBackgroundDaemon implements Runnable {
		      public void run() {
		    	  while (true) {
		    		  try {
		    			  logger.info("MusicTxDigest background daemon started");
		    			  backgroundDaemon(daemonSleepTimeS);
		    		  } catch (InterruptedException e) {
		    			  logger.error("MusicTxDigest background daemon stopped " + e.getMessage());
		    		  }
		    	  }
		      }
		   }
		   Thread t = new Thread(new MusicTxBackgroundDaemon());
		   t.start();
		
	}


}
