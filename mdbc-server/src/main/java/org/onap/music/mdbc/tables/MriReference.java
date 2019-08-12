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

public final class MriReference {
	public final UUID index;

	public MriReference(UUID index) {
		this.index=  index;
	}
	
	public MriReference(String mrirow) {
        index = UUID.fromString(mrirow);
    }

    public long getTimestamp() {
        return index.timestamp();
    }
    
    public UUID getIndex() {
        return this.index;
    }
	
	public String toString() {
	    return index.toString();
	}
}
