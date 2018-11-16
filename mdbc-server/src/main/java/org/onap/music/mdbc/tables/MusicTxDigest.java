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

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.MDBCUtils;
import org.onap.music.mdbc.MdbcServerLogic;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.configurations.NodeConfiguration;
import org.onap.music.mdbc.mixins.CassandraMixin;
import org.onap.music.mdbc.mixins.MusicInterface;

import com.datastax.driver.core.Row;

public class MusicTxDigest {
	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicTxDigest.class);
	
	/**
	 * Parse the transaction digest into individual events
	 * @param digest - base 64 encoded, serialized digest
	 */
	public static void replayTxDigest(HashMap<Range,StagingTable> digest) {
		for (Map.Entry<Range,StagingTable> entry: digest.entrySet()) {
    		Range r = entry.getKey();
    		StagingTable st = entry.getValue();
    		ArrayList<Operation> opList = st.getOperationList();
			
    		for (Operation op: opList) {
    			replayOperation(r, op);
    		}
    	}
    }
    
    /**
     * Replays operation into local database
     * @param r
     * @param op
     */
    private static void replayOperation(Range r, Operation op) {
		logger.info("Operation: " + op.getOperationType() + "->" + op.getNewVal());
		JSONObject jsonOp = op.getNewVal();
		JSONObject key = op.getKey();
		
		ArrayList<String> cols = new ArrayList<String>();
		ArrayList<Object> vals = new ArrayList<Object>();
		Iterator<String> colIterator = jsonOp.keys();
		while(colIterator.hasNext()) {
			String col = colIterator.next();
			//FIXME: should not explicitly refer to cassandramixin
			if (col.equals(CassandraMixin.MDBC_PRIMARYKEY_NAME)) {
				//reserved name
				continue;
			}
			cols.add(col);
			vals.add(jsonOp.get(col));
		}
		
		//build the queries
		StringBuilder sql = new StringBuilder();
		String sep = "";
		switch (op.getOperationType()) {
		case INSERT:
			sql.append(op.getOperationType() + " INTO ");
			sql.append(r.table + " (") ;
			sep = "";
			for (String col: cols) {
				sql.append(sep + col);
				sep = ", ";
			}	
			sql.append(") VALUES (");
			sep = "";
			for (Object val: vals) {
				sql.append(sep + "\"" + val + "\"");
				sep = ", ";
			}
			sql.append(");");
			logger.info(sql.toString());
			break;
		case UPDATE:
			sql.append(op.getOperationType() + " ");
			sql.append(r.table + " SET ");
			sep="";
			for (int i=0; i<cols.size(); i++) {
				sql.append(sep + cols.get(i) + "=\"" + vals.get(i) +"\"");
				sep = ", ";
			}
			sql.append(" WHERE ");
			sql.append(getPrimaryKeyConditional(op.getKey()));
			sql.append(";");
			logger.info(sql.toString());
			break;
		case DELETE:
			sql.append(op.getOperationType() + " FROM ");
			sql.append(r.table + " WHERE ");
			sql.append(getPrimaryKeyConditional(op.getKey()));
			sql.append(";");
			logger.info(sql.toString());
			break;
		case SELECT:
			//no update happened, do nothing
			break;
		}
    }
    
    private static String getPrimaryKeyConditional(JSONObject primaryKeys) {
    	StringBuilder keyCondStmt = new StringBuilder();
    	String and = "";
    	for (String key: primaryKeys.keySet()) {
    		Object val = primaryKeys.get(key);
    		keyCondStmt.append(and + key + "=\"" + val + "\"");
    		and = " AND ";
    	}
		return keyCondStmt.toString();
	}

	public static void startBackgroundDaemon(MdbcServerLogic meta, NodeConfiguration config)
			throws InterruptedException {
		MusicInterface mi = meta.getStateManager().getMusicInterface();
		long sleepTimeoutms = TimeUnit.SECONDS.toMillis(config.getMusicTxDigestTimeout());
		while (true) {
			//update
			logger.info(String.format("[%s] Background MusicTxDigest daemon updating local db",
					new Timestamp(System.currentTimeMillis())));
			
			//1) get all other partitions from musicrangeinformation
			List<UUID> partitions = mi.getPartitionIndexes();
			//2) for each partition I don't own
			DatabasePartition myPartition = meta.getRanges();
			for (UUID partition: partitions) {
				if (!partition.equals(myPartition.getMusicRangeInformationIndex())){
					try {
						replayDigestForPartition(mi, partition);
					} catch (MDBCServiceException e) {
						logger.error("Unable to update for partition : " + partition + ". " + e.getMessage());
						continue;
					}
				}
			}
			Thread.sleep(sleepTimeoutms);
		}
	}
	
	private static void replayDigestForPartition(MusicInterface mi, UUID partitionId) throws MDBCServiceException {
		List<MusicTxDigestId> redoLogTxIds = mi.getMusicRangeInformation(partitionId).getRedoLog();
		for (MusicTxDigestId txId: redoLogTxIds) {
			HashMap<Range, StagingTable> digest = mi.getTxDigest(txId);
			replayTxDigest(digest);
		}
		//todo, keep track of where I am in pointer
	}
    
	public static void main(String[] args) throws Exception {
    	String t0 = "rO0ABXNyABFqYXZhLnV0aWwuSGFzaE1hcAUH2sHDFmDRAwACRgAKbG9hZEZhY3RvckkACXRocmVzaG9sZHhwP0AAAAAAAAx3CAAAABAAAAABc3IAGW9yZy5vbmFwLm11c2ljLm1kYmMuUmFuZ2UWWoOV+3nB2AIAAUwABXRhYmxldAASTGphdmEvbGFuZy9TdHJpbmc7eHB0AAdwZXJzb25zc3IAJ29yZy5vbmFwLm11c2ljLm1kYmMudGFibGVzLlN0YWdpbmdUYWJsZWk84G3L4tunAgABTAAKb3BlcmF0aW9uc3QAE0xqYXZhL3V0aWwvSGFzaE1hcDt4cHNxAH4AAD9AAAAAAAAMdwgAAAAQAAAAA3QAJDgxOGU3ZWI2LTA5YmYtNGZkYS1iNzI4LWYzOTljYzNmZjdlMnNyABRqYXZhLnV0aWwuTGlua2VkTGlzdAwpU11KYIgiAwAAeHB3BAAAAAFzcgAkb3JnLm9uYXAubXVzaWMubWRiYy50YWJsZXMuT3BlcmF0aW9u7yJhSJSWe0ACAAJMAAdORVdfVkFMcQB+AANMAARUWVBFdAAqTG9yZy9vbmFwL211c2ljL21kYmMvdGFibGVzL09wZXJhdGlvblR5cGU7eHB0AIx7Im1kYmNfY3VpZCI6IjgxOGU3ZWI2LTA5YmYtNGZkYS1iNzI4LWYzOTljYzNmZjdlMiIsIkFkZHJlc3MiOiJHTk9DIiwiUGVyc29uSUQiOjEsIkZpcnN0TmFtZSI6IkpPU0giLCJDaXR5IjoiQkVETUlOU1RFUiIsIkxhc3ROYW1lIjoiU21pdGgifX5yAChvcmcub25hcC5tdXNpYy5tZGJjLnRhYmxlcy5PcGVyYXRpb25UeXBlAAAAAAAAAAASAAB4cgAOamF2YS5sYW5nLkVudW0AAAAAAAAAABIAAHhwdAAGVVBEQVRFeHQAJDA5ZjhlMzc1LWYyMjMtNDczMi04M2MxLWE3NWM0ZWQ1ZDdlMnNxAH4AC3cEAAAAAXNxAH4ADXQAjHsibWRiY19jdWlkIjoiMDlmOGUzNzUtZjIyMy00NzMyLTgzYzEtYTc1YzRlZDVkN2UyIiwiQWRkcmVzcyI6IktBQ0IiLCJQZXJzb25JRCI6MSwiRmlyc3ROYW1lIjoiSnVhbiIsIkNpdHkiOiJBVExBTlRBIiwiTGFzdE5hbWUiOiJNYXJ0aW5leiJ9fnEAfgARdAAGSU5TRVJUeHQAJGZkYzEyMWY3LWMwOGEtNGE4Ni05OTlkLTczYjc5MGQ1ZjdkNHNxAH4AC3cEAAAAAXNxAH4ADXQAjHsibWRiY19jdWlkIjoiZmRjMTIxZjctYzA4YS00YTg2LTk5OWQtNzNiNzkwZDVmN2Q0IiwiQWRkcmVzcyI6IkdOT0MiLCJQZXJzb25JRCI6MSwiRmlyc3ROYW1lIjoiSk9ITiIsIkNpdHkiOiJCRURNSU5TVEVSIiwiTGFzdE5hbWUiOiJTbWl0aCJ9cQB+ABl4eHg=";
    	//arraylist version
    	String t2 = "rO0ABXNyABFqYXZhLnV0aWwuSGFzaE1hcAUH2sHDFmDRAwACRgAKbG9hZEZhY3RvckkACXRocmVzaG9sZHhwP0AAAAAAAAx3CAAAABAAAAABc3IAGW9yZy5vbmFwLm11c2ljLm1kYmMuUmFuZ2UWWoOV+3nB2AIAAUwABXRhYmxldAASTGphdmEvbGFuZy9TdHJpbmc7eHB0AAdwZXJzb25zc3IAJ29yZy5vbmFwLm11c2ljLm1kYmMudGFibGVzLlN0YWdpbmdUYWJsZWk84G3L4tunAgABTAAKb3BlcmF0aW9uc3QAFUxqYXZhL3V0aWwvQXJyYXlMaXN0O3hwc3IAE2phdmEudXRpbC5BcnJheUxpc3R4gdIdmcdhnQMAAUkABHNpemV4cAAAAAN3BAAAAANzcgAkb3JnLm9uYXAubXVzaWMubWRiYy50YWJsZXMuT3BlcmF0aW9u7yJhSJSWe0ACAAJMAAdORVdfVkFMcQB+AANMAARUWVBFdAAqTG9yZy9vbmFwL211c2ljL21kYmMvdGFibGVzL09wZXJhdGlvblR5cGU7eHB0AFl7IkFkZHJlc3MiOiJLQUNCIiwiUGVyc29uSUQiOjEsIkZpcnN0TmFtZSI6Ikp1YW4iLCJDaXR5IjoiQVRMQU5UQSIsIkxhc3ROYW1lIjoiTWFydGluZXoifX5yAChvcmcub25hcC5tdXNpYy5tZGJjLnRhYmxlcy5PcGVyYXRpb25UeXBlAAAAAAAAAAASAAB4cgAOamF2YS5sYW5nLkVudW0AAAAAAAAAABIAAHhwdAAGSU5TRVJUc3EAfgALdABZeyJBZGRyZXNzIjoiR05PQyIsIlBlcnNvbklEIjoyLCJGaXJzdE5hbWUiOiJKT0hOIiwiQ2l0eSI6IkJFRE1JTlNURVIiLCJMYXN0TmFtZSI6IlNtaXRoIn1xAH4AEXNxAH4AC3QAWXsiQWRkcmVzcyI6IkdOT0MiLCJQZXJzb25JRCI6MiwiRmlyc3ROYW1lIjoiSk9TSCIsIkNpdHkiOiJCRURNSU5TVEVSIiwiTGFzdE5hbWUiOiJTbWl0aCJ9fnEAfgAPdAAGVVBEQVRFeHg=";
    	String t3 = "rO0ABXNyABFqYXZhLnV0aWwuSGFzaE1hcAUH2sHDFmDRAwACRgAKbG9hZEZhY3RvckkACXRocmVzaG9sZHhwP0AAAAAAAAx3CAAAABAAAAABc3IAGW9yZy5vbmFwLm11c2ljLm1kYmMuUmFuZ2UWWoOV+3nB2AIAAUwABXRhYmxldAASTGphdmEvbGFuZy9TdHJpbmc7eHB0AAdwZXJzb25zc3IAJ29yZy5vbmFwLm11c2ljLm1kYmMudGFibGVzLlN0YWdpbmdUYWJsZWk84G3L4tunAgABTAAKb3BlcmF0aW9uc3QAFUxqYXZhL3V0aWwvQXJyYXlMaXN0O3hwc3IAE2phdmEudXRpbC5BcnJheUxpc3R4gdIdmcdhnQMAAUkABHNpemV4cAAAAAV3BAAAAAVzcgAkb3JnLm9uYXAubXVzaWMubWRiYy50YWJsZXMuT3BlcmF0aW9u7yJhSJSWe0ACAAJMAAdORVdfVkFMcQB+AANMAARUWVBFdAAqTG9yZy9vbmFwL211c2ljL21kYmMvdGFibGVzL09wZXJhdGlvblR5cGU7eHB0AFl7IkFkZHJlc3MiOiJLQUNCIiwiUGVyc29uSUQiOjEsIkZpcnN0TmFtZSI6Ikp1YW4iLCJDaXR5IjoiQVRMQU5UQSIsIkxhc3ROYW1lIjoiTWFydGluZXoifX5yAChvcmcub25hcC5tdXNpYy5tZGJjLnRhYmxlcy5PcGVyYXRpb25UeXBlAAAAAAAAAAASAAB4cgAOamF2YS5sYW5nLkVudW0AAAAAAAAAABIAAHhwdAAGSU5TRVJUc3EAfgALdABZeyJBZGRyZXNzIjoiS0FDQiIsIlBlcnNvbklEIjoxLCJGaXJzdE5hbWUiOiJKdWFuIiwiQ2l0eSI6IkFUTEFOVEEiLCJMYXN0TmFtZSI6Ik1hcnRpbmV6In1+cQB+AA90AAZERUxFVEVzcQB+AAt0AFl7IkFkZHJlc3MiOiJHTk9DIiwiUGVyc29uSUQiOjIsIkZpcnN0TmFtZSI6IkpPSE4iLCJDaXR5IjoiQkVETUlOU1RFUiIsIkxhc3ROYW1lIjoiU21pdGgifXEAfgARc3EAfgALdABZeyJBZGRyZXNzIjoiR05PQyIsIlBlcnNvbklEIjoyLCJGaXJzdE5hbWUiOiJKT1NIIiwiQ2l0eSI6IkJFRE1JTlNURVIiLCJMYXN0TmFtZSI6IlNtaXRoIn1+cQB+AA90AAZVUERBVEVzcQB+AAt0AFl7IkFkZHJlc3MiOiJHTk9DIiwiUGVyc29uSUQiOjIsIkZpcnN0TmFtZSI6IkpPSE4iLCJDaXR5IjoiQkVETUlOU1RFUiIsIkxhc3ROYW1lIjoiU21pdGgifXEAfgAbeHg=";
    	String t4 = "rO0ABXNyABFqYXZhLnV0aWwuSGFzaE1hcAUH2sHDFmDRAwACRgAKbG9hZEZhY3RvckkACXRocmVzaG9sZHhwP0AAAAAAAAx3CAAAABAAAAABc3IAGW9yZy5vbmFwLm11c2ljLm1kYmMuUmFuZ2UWWoOV+3nB2AIAAUwABXRhYmxldAASTGphdmEvbGFuZy9TdHJpbmc7eHB0AAdwZXJzb25zc3IAJ29yZy5vbmFwLm11c2ljLm1kYmMudGFibGVzLlN0YWdpbmdUYWJsZWk84G3L4tunAgABTAAKb3BlcmF0aW9uc3QAFUxqYXZhL3V0aWwvQXJyYXlMaXN0O3hwc3IAE2phdmEudXRpbC5BcnJheUxpc3R4gdIdmcdhnQMAAUkABHNpemV4cAAAAAV3BAAAAAVzcgAkb3JnLm9uYXAubXVzaWMubWRiYy50YWJsZXMuT3BlcmF0aW9u7yJhSJSWe0ACAANMAANLRVlxAH4AA0wAB05FV19WQUxxAH4AA0wABFRZUEV0ACpMb3JnL29uYXAvbXVzaWMvbWRiYy90YWJsZXMvT3BlcmF0aW9uVHlwZTt4cHQAJHsiUGVyc29uSUQiOjEsIkxhc3ROYW1lIjoiTWFydGluZXoifXQAWXsiQWRkcmVzcyI6IktBQ0IiLCJQZXJzb25JRCI6MSwiRmlyc3ROYW1lIjoiSnVhbiIsIkNpdHkiOiJBVExBTlRBIiwiTGFzdE5hbWUiOiJNYXJ0aW5leiJ9fnIAKG9yZy5vbmFwLm11c2ljLm1kYmMudGFibGVzLk9wZXJhdGlvblR5cGUAAAAAAAAAABIAAHhyAA5qYXZhLmxhbmcuRW51bQAAAAAAAAAAEgAAeHB0AAZJTlNFUlRzcQB+AAt0ACR7IlBlcnNvbklEIjoxLCJMYXN0TmFtZSI6Ik1hcnRpbmV6In10AFl7IkFkZHJlc3MiOiJLQUNCIiwiUGVyc29uSUQiOjEsIkZpcnN0TmFtZSI6Ikp1YW4iLCJDaXR5IjoiQVRMQU5UQSIsIkxhc3ROYW1lIjoiTWFydGluZXoifX5xAH4AEHQABkRFTEVURXNxAH4AC3QAIXsiUGVyc29uSUQiOjIsIkxhc3ROYW1lIjoiU21pdGgifXQAWXsiQWRkcmVzcyI6IkdOT0MiLCJQZXJzb25JRCI6MiwiRmlyc3ROYW1lIjoiSk9ITiIsIkNpdHkiOiJCRURNSU5TVEVSIiwiTGFzdE5hbWUiOiJTbWl0aCJ9cQB+ABJzcQB+AAt0ACF7IlBlcnNvbklEIjoyLCJMYXN0TmFtZSI6IlNtaXRoIn10AFl7IkFkZHJlc3MiOiJHTk9DIiwiUGVyc29uSUQiOjIsIkZpcnN0TmFtZSI6IkpPU0giLCJDaXR5IjoiQkVETUlOU1RFUiIsIkxhc3ROYW1lIjoiU21pdGgifX5xAH4AEHQABlVQREFURXNxAH4AC3QAIXsiUGVyc29uSUQiOjIsIkxhc3ROYW1lIjoiU21pdGgifXQAWXsiQWRkcmVzcyI6IkdOT0MiLCJQZXJzb25JRCI6MiwiRmlyc3ROYW1lIjoiSk9ITiIsIkNpdHkiOiJCRURNSU5TVEVSIiwiTGFzdE5hbWUiOiJTbWl0aCJ9cQB+AB94eA==";
    	HashMap<Range, StagingTable> digest = (HashMap<Range, StagingTable>) MDBCUtils.fromString(t4);
    	replayTxDigest(digest);
    }


}
