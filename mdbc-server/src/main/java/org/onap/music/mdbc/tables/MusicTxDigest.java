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

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.MdbcConnection;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.StateManager;
import org.onap.music.mdbc.mixins.DBInterface;
import org.onap.music.mdbc.mixins.MusicInterface;

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
			//2) for each partition I don't own
            final List<Range> warmuplist = stateManager.getWarmupRanges();
			if(warmuplist!=null) {
                final Set<Range> warmupRanges = new HashSet(warmuplist);
                final List<DatabasePartition> currentPartitions = stateManager.getPartitions();
                List<Range> missingRanges = new ArrayList<>();
                if (currentPartitions.size() != 0) {
                    for (DatabasePartition part : currentPartitions) {
                        List<Range> partitionRanges = part.getSnapshot();
                        warmupRanges.removeAll(partitionRanges);
                    }
                    try {
                        mi.getOwnAndCheck().warmup(mi, dbi, new ArrayList<>(warmupRanges));
                    } catch (MDBCServiceException e) {
                        logger.error("Unable to update for partition : " + warmupRanges + ". " + e.getMessage());
                        continue;
                    }
                }
            }

    //Step 3: ReplayDigest() for E.C conditions
			try {
				replayDigest(mi,dbi);
			} catch (MDBCServiceException e) {
				logger.error("Unable to perform Eventual Consistency operations" + e.getMessage());
				continue;
			}	
			
		}
	}

	/**
	 * Replay the digest for eventual consistency.
	 * @param mi music interface
	 * @param partitionId the partition to be replayed
	 * @param dbi interface to the database that will replay the operations
	 * @throws MDBCServiceException
	 */
	public static void replayDigest(MusicInterface mi, DBInterface dbi) throws MDBCServiceException {
					//List<MusicTxDigestId> partitionsRedoLogTxIds = mi.getMusicRangeInformation(partitionId).getRedoLog();
					//From where should I fetch TransactionsIDs ??? from NEW TABLE ?? or EXISING TABLE ?? << what the new SITE_TABLE details??
					// --> It is a new table called ECTxDigest
					//I should sort/ call a method which gives all the entires of  a table based on the time-stamp from Low to High

		ArrayList<HashMap<Range, StagingTable>> ecTxDigest = mi.getEveTxDigest();
		
					//for (MusicTxDigestId txId: partitionsRedoLogTxIds) { // partitionsRedoLogTxIds --> this comes from new table where timeStamp > currentTimeStamp  ( THIS SHOULD BE lessthan.. which is ASC order)
					//HashMap<Range, StagingTable> transaction = mi2.getEcTxDigest();  // Getting records from musictxdigest TABLE.
		for (HashMap<Range, StagingTable> transaction: ecTxDigest) {
			try {
				dbi.replayTransaction(transaction); // I think this Might change if the data is coming from a new table.. ( what is the new table structure??)
			} catch (SQLException e) {
				logger.error("EC:Rolling back the entire digest replay.");
				return;
			}
			logger.info("EC: Successfully replayed transaction ");
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
        List<MusicTxDigestId> partitionsRedoLogTxIds = mi.getMusicRangeInformation(partitionId).getRedoLog();
        for (MusicTxDigestId txId: partitionsRedoLogTxIds) {
            HashMap<Range, StagingTable> transaction = mi.getTxDigest(txId);
            try {
                //\TODO do this two operations in parallel
                dbi.replayTransaction(transaction);
                mi.replayTransaction(transaction);
            } catch (SQLException e) {
                logger.error("Rolling back the entire digest replay. " + partitionId);
                return;
            }
            logger.info("Successfully replayed transaction " + txId);
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
