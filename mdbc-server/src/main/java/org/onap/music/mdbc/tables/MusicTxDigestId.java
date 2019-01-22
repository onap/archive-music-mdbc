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

import java.util.UUID;

public final class MusicTxDigestId {
    public final UUID mriId;
	public final UUID transactionId;
	public final int index;

	public MusicTxDigestId(UUID mriRowId, UUID digestId, int index) {
	    this.mriId=mriRowId;
		this.transactionId= digestId;
		this.index=index;
	}

	public boolean isEmpty() {
		return (this.transactionId==null);
	}

    @Override
    public boolean equals(Object o){
        if (this == o) return true;
        if(o == null) return false;
        if(!(o instanceof MusicTxDigestId)) return false;
        MusicTxDigestId other = (MusicTxDigestId) o;
        return other.transactionId.equals(this.transactionId);
    }

    @Override
    public int hashCode(){
        return transactionId.hashCode();
    }
}
