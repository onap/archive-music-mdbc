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
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.MDBCUtils;
import org.onap.music.mdbc.MdbcConnection;
import org.onap.music.mdbc.MdbcServerLogic;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.StateManager;
import org.onap.music.mdbc.configurations.NodeConfiguration;
import org.onap.music.mdbc.mixins.MusicMixin;
import org.onap.music.mdbc.mixins.DBInterface;
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
     * Runs the body of the background daemon
     * @param daemonSleepTimeS time, in seconds, between updates
     * @throws InterruptedException
     */
	public void backgroundDaemon(int daemonSleepTimeS) throws InterruptedException {
		MusicInterface mi = stateManager.getMusicInterface();
		DBInterface dbi = ((MdbcConnection) stateManager.getConnection("daemon")).getDBInterface();

		while (true) {
			Thread.sleep(TimeUnit.SECONDS.toMillis(daemonSleepTimeS));
			//update
			logger.info(String.format("[%s] Background MusicTxDigest daemon updating local db",
					new Timestamp(System.currentTimeMillis())));
			
			//1) get all other partitions from musicrangeinformation
			List<UUID> partitions = null;
			try {
				partitions = mi.getPartitionIndexes();
			} catch (MDBCServiceException e) {
			    logger.error("Error obtainting partition indexes, trying again next iteration");
			    continue;
			}
			List<DatabasePartition> ranges = stateManager.getRanges();
			if(ranges.size()!=0) {
				DatabasePartition myPartition = ranges.get(0);
				for (UUID partition : partitions) {
					if (!partition.equals(myPartition.getMRIIndex())) {
				        //2) for each partition I don't own
						try {
							replayDigestForPartition(mi, partition, dbi);
						} catch (MDBCServiceException e) {
							logger.error("Unable to update for partition : " + partition + ". " + e.getMessage());
							continue;
						}
					}
				}
			}
		}
	}
	
	/**
	 * Replay the digest for a given partition
	 * @param mi music interface
	 * @param partitionId the partition to be replayed
	 * @param dbi interface to the database that will replay the operations
	 * @throws MDBCServiceException
	 */
	public static void replayDigestForPartition(MusicInterface mi, UUID partitionId, DBInterface dbi) throws MDBCServiceException {
	    MusicRangeInformationRow mriRow = mi.getMusicRangeInformation(partitionId);
	    List<MusicTxDigestId> partitionsRedoLogTxIds = mriRow.getRedoLog();
		for (MusicTxDigestId txId: partitionsRedoLogTxIds) {
			HashMap<Range, StagingTable> transaction = mi.getTxDigest(txId);
			try {
				dbi.replayTransaction(transaction);
			} catch (SQLException e) {
				logger.error("Rolling back the entire digest replay. " + partitionId);
				return;
			}
	        logger.info("Successfully replayed transaction " + txId);
            mi.updateProgressKeeperTable(txId, mriRow.getRanges());
		}
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
