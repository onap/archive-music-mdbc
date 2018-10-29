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

import java.io.Serializable;

import org.json.JSONObject;
import org.json.JSONTokener;

import static java.util.Objects.hash;

public final class Operation implements Serializable{

	private static final long serialVersionUID = -1215301985078183104L;

	final OperationType TYPE;
	final String NEW_VAL;
	final String KEY;

	public Operation(OperationType type, String newVal, String key) {
		TYPE = type;
		NEW_VAL = newVal;
		KEY = key;
	}

	public JSONObject getNewVal(){
        JSONObject newRow  = new JSONObject(new JSONTokener(NEW_VAL));
        return newRow;
    }

	public JSONObject getKey() {
		JSONObject key  = new JSONObject(new JSONTokener(KEY));
		return key;
	}
	
    public OperationType getOperationType() {
    	return this.TYPE;
    }

    @Override
    public int hashCode(){
        return hash(TYPE,NEW_VAL);
    }

    @Override
    public boolean equals(Object o){
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Operation r = (Operation) o;
        return TYPE.equals(r.TYPE) && NEW_VAL.equals(r.NEW_VAL);
    }
}
