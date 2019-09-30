/*
 * ============LICENSE_START====================================================
 * org.onap.music.mdbc
 * =============================================================================
 * Copyright (C) 2019 AT&T Intellectual Property. All rights reserved.
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

import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.query.SQLOperationType;

public class LockResult{
    private boolean successful;
    private UUID musicRangeInformationIndex;
    private String lockId;
    private SQLOperationType lockType;
    private Set<Range> ranges;
    private boolean newLock;
    /** back off time in milliseconds */
    private long backOffPeriodms;

    public LockResult(boolean succesful, UUID rowId, String lockId, SQLOperationType lockType, boolean newLock, Set<Range> ranges){
        this.successful = succesful;
        this.musicRangeInformationIndex = rowId;
        this.lockId=lockId;
        this.lockType = lockType;
        this.newLock=newLock;
        this.ranges=ranges;
    }
    
    public LockResult(boolean succesful, UUID rowId, String lockId, boolean newLock, Set<Range> ranges){
        this.successful = succesful;
        this.musicRangeInformationIndex = rowId;
        this.lockId=lockId;
        this.newLock=newLock;
        this.ranges=ranges;
    }
    /**
     * Please use constructor which specifies whether lock result was succesful
     * @param rowId
     * @param lockId
     * @param newLock
     * @param ranges
     */
    @Deprecated
    public LockResult(UUID rowId, String lockId, boolean newLock, Set<Range> ranges){
        this.successful = true;
        this.musicRangeInformationIndex = rowId;
        this.lockId=lockId;
        this.newLock=newLock;
        this.ranges=ranges;
    }
    public LockResult(boolean successful, long backOffTimems) {
        this.successful = successful;
        this.backOffPeriodms = backOffTimems;
    }
    
    public boolean wasSuccessful() {
        return successful;
    }
    
    public String getLockId(){
        return lockId;
    }
    public boolean isNewLock(){
        return newLock;
    }
    public UUID getIndex() {
        return musicRangeInformationIndex;
    }
    
    public Set<Range> getRanges() {
        return ranges;
    }
    
    public void addRange(Range range) {
        ranges.add(range);
    }
    
    /**
     * Get the backOffPeriod, in milliseconds, requested by mixin
     * @return
     */
    public long getBackOffPeriod() {
        return this.backOffPeriodms;
    }
    
}