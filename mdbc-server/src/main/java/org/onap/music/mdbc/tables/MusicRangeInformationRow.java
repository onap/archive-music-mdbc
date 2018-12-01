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

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

import org.onap.music.mdbc.DatabasePartition;

public final class MusicRangeInformationRow implements Comparable<MusicRangeInformationRow>{
	private final DatabasePartition dbPartition;
	private final UUID partitionIndex;
	private final List<MusicTxDigestId> redoLog;
	private String ownerId;
	private final String metricProcessId;

	public MusicRangeInformationRow (UUID partitionIndex, DatabasePartition dbPartition, List<MusicTxDigestId> redoLog,
		String ownerId, String metricProcessId) {
	    this.partitionIndex=partitionIndex;
		this.dbPartition = dbPartition;
		this.redoLog = redoLog;
		this.ownerId = ownerId;
		this.metricProcessId = metricProcessId;
	}

	public UUID getPartitionIndex() {
		return dbPartition.getMusicRangeInformationIndex();
	}

	public DatabasePartition getDBPartition() {
		return this.dbPartition;
	}

	public List<MusicTxDigestId> getRedoLog() {
		return redoLog;
	}

	public String getOwnerId() {
		return ownerId;
	}

	public String getMetricProcessId() {
		return metricProcessId;
	}

	public long getTimestamp(){
	    return partitionIndex.timestamp();
    }

    public void setOwnerId(String newOwnerId){
	   this.ownerId=newOwnerId;
    }

    @Override
    public int compareTo(MusicRangeInformationRow o) {
	    long thisTimestamp = this.getTimestamp();
	    long oTimestamp = o.getTimestamp();
	    //descending order
	    int returnVal = (thisTimestamp>oTimestamp)?-1:(thisTimestamp==oTimestamp)?0:1;
	    return returnVal;
    }
}
