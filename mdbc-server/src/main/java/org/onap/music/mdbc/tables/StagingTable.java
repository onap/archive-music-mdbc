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
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONObject;

import org.onap.music.logging.EELFLoggerDelegate;

public class StagingTable implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7583182634761771943L;
	private transient static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(StagingTable.class);
	//primary key -> Operation
	private HashMap<String,Deque<Operation>> operations;
	
	public StagingTable() {
		operations = new HashMap<>();
	}
	
	synchronized public void addOperation(String key, OperationType type, String newVal) {
		if(!operations.containsKey(key)) {
			operations.put(key, new LinkedList<>());
		}
		operations.get(key).add(new Operation(type,newVal));
	}
	
	synchronized public Deque<Pair<String,Operation>> getIterableSnapshot() throws NoSuchFieldException{
		Deque<Pair<String,Operation>> response=new LinkedList<Pair<String,Operation>>();
		//\TODO: check if we can just return the last change to a given key 
		Set<String> keys = operations.keySet();
		for(String key : keys) {
			Deque<Operation> ops = operations.get(key);
			if(ops.isEmpty()) {
				logger.error(EELFLoggerDelegate.errorLogger, "Invalid state of the Operation data structure when creating snapshot");
				throw new NoSuchFieldException("Invalid state of the operation data structure");
			}
			response.add(Pair.of(key,ops.getLast()));
		}
		return response;
	}
	
	synchronized public void clean() {
		operations.clear();
	}
}
