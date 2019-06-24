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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.onap.music.mdbc.DatabasePartition;

public final class MusicRangeInformationRow implements Comparable<MusicRangeInformationRow>{
	private final DatabasePartition dbPartition;
	private final List<MusicTxDigestId> redoLog;
	private boolean isLatest;
	private Set<UUID> prevRowIndexes;

	public MusicRangeInformationRow (DatabasePartition dbPartition, List<MusicTxDigestId> redoLog,
            boolean isLatest) {
            this.dbPartition = dbPartition;
            this.redoLog = redoLog;
            this.isLatest = isLatest;
            this.prevRowIndexes = new HashSet<>();
    }
	
	public MusicRangeInformationRow (DatabasePartition dbPartition, List<MusicTxDigestId> redoLog,
	        boolean isLatest, Set<UUID> prevPartitions) {
	        this.dbPartition = dbPartition;
	        this.redoLog = redoLog;
	        this.isLatest = isLatest;
	        this.prevRowIndexes = prevPartitions;
	}

	public UUID getPartitionIndex() {
		return dbPartition.getMRIIndex();
	}

	public boolean getIsLatest(){ return isLatest; }

	public void setIsLatest(boolean isLatest){ this.isLatest = isLatest; }

	public DatabasePartition getDBPartition() {
		return this.dbPartition;
	}

	public List<MusicTxDigestId> getRedoLog() {
		return redoLog;
	}

	public long getTimestamp(){
	    return dbPartition.getMRIIndex().timestamp();
    }
	
	public Set<UUID> getPrevRowIndexes() {
	    return this.prevRowIndexes;
	}

    @Override
    public int compareTo(MusicRangeInformationRow o) {
	    long thisTimestamp = this.getTimestamp();
	    long oTimestamp = o.getTimestamp();
	    //descending order
	    int returnVal = (thisTimestamp>oTimestamp)?-1:(thisTimestamp==oTimestamp)?0:1;
	    return returnVal;
    }

	@Override
    public boolean equals(Object o){
        if (this == o) return true;
        if(o == null) return false;
        if(!(o instanceof MusicRangeInformationRow)) return false;
        MusicRangeInformationRow other = (MusicRangeInformationRow) o;
        return other.getPartitionIndex().equals(this.getPartitionIndex());
    }

    @Override
    public int hashCode(){
        return dbPartition.hashCode();
    }
}
