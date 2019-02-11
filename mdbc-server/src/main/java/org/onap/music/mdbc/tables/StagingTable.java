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
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONObject;

import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.query.SQLOperation;

public class StagingTable implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7583182634761771943L;
	private transient static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(StagingTable.class);
	//primary key -> Operation
	private ArrayList<Operation> operations;
	
	public StagingTable() {
		operations = new ArrayList<Operation>();
	}
	
	synchronized public void addOperation(SQLOperation type, String newVal, String key) {
		operations.add(new Operation(type,newVal, key));
	}
	
	synchronized public ArrayList<Operation> getOperationList() {
		return operations;
	}
	
	synchronized public void clean() {
		operations.clear();
	}
}
