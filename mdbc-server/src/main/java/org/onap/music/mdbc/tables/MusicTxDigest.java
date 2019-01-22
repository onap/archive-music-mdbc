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

public class MusicTxDigest implements Runnable {

	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicTxDigest.class);

	private StateManager stateManager;
	private int daemonSleepTimeS;

	public MusicTxDigest(int daemonSleepTimeS, StateManager stateManager) {
		this.stateManager = stateManager;
		this.daemonSleepTimeS = daemonSleepTimeS;
	}

	/**
	 * Replay the digest for eventual consistency.
	 *
	 * @param mi music interface
	 * @param dbi interface to the database that will replay the operations
	 * @param ranges only these ranges will be applied from the digests
	 */
	public void replayDigest(MusicInterface mi, DBInterface dbi, List<Range> ranges) throws MDBCServiceException {
		StagingTable transaction;
		String nodeName = stateManager.getMdbcServerName();

		LinkedHashMap<UUID, StagingTable> ecDigestInformation = mi.getEveTxDigest(nodeName);
		Set<UUID> keys = ecDigestInformation.keySet();
		for (UUID txTimeID : keys) {
			transaction = ecDigestInformation.get(txTimeID);
			try {
				dbi.replayTransaction(transaction,
					ranges); // I think this Might change if the data is coming from a new table.. ( what is the new table structure??)
			} catch (SQLException e) {
				logger.error("EC:Rolling back the entire digest replay.");
				return;
			}
			logger.info("EC: Successfully replayed transaction ");

			try {
				mi.updateNodeInfoTableWithTxTimeIDKey(txTimeID, nodeName);
			} catch (MDBCServiceException e) {
				logger.error("EC:Rolling back the entire digest replay.");
			}
		}
	}

	@Override
	public void run() {
		logger.info("MusicTxDigest background daemon started");
		if (stateManager == null) {
			logger.error("State manager is null in background daemon");
			return;
		}
		MusicInterface mi = stateManager.getMusicInterface();

		if (mi == null) {
			logger.error("Music interface or DB interface is null in background daemon");
			return;
		}
		while (true) {
			try {
				MdbcConnection conn = (MdbcConnection) stateManager.getConnection("daemon");
				if (conn == null) {
					logger.error("Connection created is null in background daemon");
					return;
				}
				DBInterface dbi = (conn).getDBInterface();
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
				if (warmuplist != null) {
					final Set<Range> warmupRanges = new HashSet<>(warmuplist);
					final List<DatabasePartition> currentPartitions = stateManager.getPartitions();
					List<Range> missingRanges = new ArrayList<>();
					if (currentPartitions.size() != 0) {
						for (DatabasePartition part : currentPartitions) {
							List<Range> partitionRanges = part.getSnapshot();
							warmupRanges.removeAll(partitionRanges);
						}
						try {
							stateManager.getOwnAndCheck().warmup(mi, dbi, new ArrayList<>(warmupRanges));
						} catch (MDBCServiceException e) {
							logger.error("Unable to update for partition : " + warmupRanges + ". " + e.getMessage());
							continue;
						}
					}
				}

				//Step 3: ReplayDigest() for E.C conditions
				try {
					replayDigest(mi, dbi, stateManager.getEventualRanges());
				} catch (MDBCServiceException e) {
					logger.error("Unable to perform Eventual Consistency operations" + e.getMessage());
					continue;
				}
				conn.close();
				Thread.sleep(TimeUnit.SECONDS.toMillis(this.daemonSleepTimeS));
			} catch (InterruptedException | SQLException e) {
				logger.error("MusicTxDigest background daemon stopped " + e.getMessage(), e);
				Thread.currentThread().interrupt();
			}
		}
	}
}
