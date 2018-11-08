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

package org.onap.music.mdbc.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.mixins.Cassandra2Mixin;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class QueryProcessor {

	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(QueryProcessor.class);

	public List<String> tables = null;
	private static final String REGEX_SPACE = "\\s+";
	
	private static final List<String> concerned = Arrays.asList("table", "into", "join", "using", "update");
	
	public QueryProcessor() {
		
	}
	
	public QueryProcessor(String sqlQuery) {
		tables = new ArrayList<>();
		String[] tokens = sqlQuery.split(REGEX_SPACE);
		int counter = tokens.length;
		int index = 0;
		while(counter>0) {
			String currentToken = tokens[index];
			if(! "from".equals(currentToken.toLowerCase())) {
				
			} else {
				while(!"where".equals(currentToken.toLowerCase())) {
					System.out.println(currentToken);
					counter--;
					index++;
					currentToken = tokens[index];
				}
				
			}
			counter--;
			index++;
		}
	}

	public static Map<String, List<String>> extractTableFromQuery(String sqlQuery) {
		List<String> tables = null;
		Map<String, List<String>> tableOpsMap = new HashMap<>();
		try {
			net.sf.jsqlparser.statement.Statement stmt = CCJSqlParserUtil.parse(sqlQuery);
			if (stmt instanceof Insert) {
				Insert s = (Insert) stmt;
				String tbl = s.getTable().getName();
				List<String> Ops = tableOpsMap.get(tbl);
				if(Ops == null) Ops = new ArrayList<>();
				Ops.add(Operation.INSERT.getOperation());
				tableOpsMap.put(tbl, Ops);
				logger.debug(EELFLoggerDelegate.applicationLogger, "Inserting into table: "+tbl);
			} else {
				String tbl;
				String where = "";
				if (stmt instanceof Update){
					Update u = (Update) stmt;
					tbl = u.getTables().get(0).getName();
					List<String> Ops = tableOpsMap.get(tbl);
					if(Ops == null) Ops = new ArrayList<>();
					if(u.getWhere() != null) {
						where = u.getWhere().toString();
						logger.debug(EELFLoggerDelegate.applicationLogger, "Updating table: "+tbl);
						Ops.add(Operation.UPDATE.getOperation());
					} else {
						Ops.add(Operation.TABLE.getOperation());
					}
					tableOpsMap.put(tbl, Ops);
				} else if (stmt instanceof Delete) {
					Delete d = (Delete) stmt;
					tbl = d.getTable().getName();
					List<String> Ops = tableOpsMap.get(tbl);
					if(Ops == null) Ops = new ArrayList<>();
					if (d.getWhere()!=null) {
						where = d.getWhere().toString();
						Ops.add(Operation.DELETE.getOperation());
					} else {
						Ops.add(Operation.TABLE.getOperation());
					}
					tableOpsMap.put(tbl, Ops);
					logger.debug(EELFLoggerDelegate.applicationLogger, "Deleting from table: "+tbl);
				} else if (stmt instanceof Select) {
			        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
			        tables = tablesNamesFinder.getTableList(stmt);
			        for(String table : tables) {
				        List<String> Ops = tableOpsMap.get(table);
				        if(Ops == null) Ops = new ArrayList<>();
						Ops.add(Operation.SELECT.getOperation());
						tableOpsMap.put(table, Ops);
			        }
				}
				else {
					logger.error(EELFLoggerDelegate.errorLogger, "Not recognized sql type");
					tbl = "";
				}
			}
		} catch (JSQLParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tableOpsMap;
	}
	
	
	

	public static void main(String[] args) {
		String sqlQuery = "SELECT name, age FROM table1 t1, table2 t2 WHERE t1.id = t2.id";
		Map<String, List<String>> tableOpsMap = extractTableFromQuery(sqlQuery);
		System.out.println(tableOpsMap);
		sqlQuery = "INSERT INTO Employees (id, name) values ('1','Vikram')";
		tableOpsMap = extractTableFromQuery(sqlQuery);
		System.out.println(tableOpsMap);
		
		sqlQuery = "UPDATE Employees SET id = 1 WHERE id = 2";
		tableOpsMap = extractTableFromQuery(sqlQuery);
		System.out.println(tableOpsMap);
		
		sqlQuery = "UPDATE Employees SET id = 1";
		tableOpsMap = extractTableFromQuery(sqlQuery);
		System.out.println(tableOpsMap);
		
		sqlQuery = "UPDATE table1 SET id = 1";
		tableOpsMap = extractTableFromQuery(sqlQuery);
		
		System.out.println(tableOpsMap);
	}
	

}

