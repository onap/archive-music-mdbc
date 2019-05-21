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
import java.util.UUID;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.query.SQLOperationType;

public class LockRequest {
    private final UUID id;
    private final List<Range> toLockRanges;
    private SQLOperationType lockType;
    private int numOfAttempts;

    /**
     * 
     * @param table
     * @param id
     * @param toLockRanges
     */
    public LockRequest(UUID id, List<Range> toLockRanges) {
        this.id = id;
        this.toLockRanges = toLockRanges;
        lockType = SQLOperationType.WRITE;
        numOfAttempts = 1;
    }
    
    /**
     * 
     * @param table
     * @param id
     * @param toLockRanges
     * @param lockType
     */
    public LockRequest(UUID id, List<Range> toLockRanges, SQLOperationType lockType) {
        this.id = id;
        this.toLockRanges = toLockRanges;
        this.lockType = lockType;
        numOfAttempts = 1;
    }

    public UUID getId() {
        return id;
    }

    public List<Range> getToLockRanges() {
        return toLockRanges;
    }
    
    /**
     * Number of times you've requested this lock
     * @return
     */
    public int getNumOfAttempts() {
        return numOfAttempts;
    }

    public void incrementAttempts() {
        numOfAttempts++;        
    }
    
    public SQLOperationType getLockType() {
        return lockType;
    }
}