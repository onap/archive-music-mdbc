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

public class LockResult{
    private final UUID musicRangeInformationIndex;
    private final String ownerId;
    private List<Range> ranges;
    private final boolean newLock;

    public LockResult(UUID rowId, String ownerId, boolean newLock, List<Range> ranges){
        this.musicRangeInformationIndex = rowId;
        this.ownerId=ownerId;
        this.newLock=newLock;
        this.ranges=ranges;
    }
    public String getOwnerId(){
        return ownerId;
    }
    public boolean isNewLock(){
        return newLock;
    }
    public UUID getIndex() {return musicRangeInformationIndex;}
    public List<Range> getRanges() {return ranges;}
    public void addRange(Range range){ranges.add(range);}
}