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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.Util;
import org.onap.music.logging.EELFLoggerDelegate;

public class QueryProcessor {

    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(QueryProcessor.class);

    public QueryProcessor() {

    }

    protected static SqlParserImplFactory parserImplFactory() {
        return SqlParserImpl.FACTORY;
    }

    protected static SqlParser getSqlParser(String sql) {
        Quoting quoting = Quoting.DOUBLE_QUOTE;
        Casing unquotedCasing = Casing.TO_UPPER;
        Casing quotedCasing = Casing.UNCHANGED;
        SqlConformance conformance = SqlConformanceEnum.DEFAULT;

        return SqlParser.create(sql, SqlParser.configBuilder().setParserFactory(parserImplFactory()).setQuoting(quoting)
                .setUnquotedCasing(unquotedCasing).setQuotedCasing(quotedCasing).setConformance(conformance).build());
    }

    /**
     * 
     * @param query
     * @param tables set of tables found in sql database. This is only used as a cross reference, 
     *         the parser will try to find tables in the query first, regardless of whether they
     *         are in this set
     * @return map of table name to {@link org.onap.music.mdbc.query.SQLOperation}
     * @throws SqlParseException
     */
    public static Map<String, List<SQLOperation>> parseSqlQuery(String query, Set<String> tables) throws SQLException {
        logger.info(EELFLoggerDelegate.applicationLogger, "Parsing query: "+query);
        query = query.trim();
        if (query.endsWith(";")) {
            query = query.substring(0, query.length() - 1);
        }
        Map<String, List<SQLOperation>> tableOpsMap = new HashMap<>();
        //for Create no need to check locks.
        if(query.toUpperCase().startsWith("CREATE"))  {
            logger.error(EELFLoggerDelegate.errorLogger, "CREATE TABLE DDL not currently supported currently.");
            return tableOpsMap;
        }

        SqlNode sqlNode;
        try {
            sqlNode = getSqlParser(query).parseStmt();
        } catch (SqlParseException e) {
            logger.warn(EELFLoggerDelegate.errorLogger, "Unable to parse query: " + query + "; Falling back to Secondary basic parser",e);
            return basicStringParser(query, tables);
        }

        SqlBasicVisitor<Void> visitor = new SqlBasicVisitor<Void>() {

            public Void visit(SqlCall call) {
                if (call.getOperator() instanceof SqlInOperator) {
                    throw new Util.FoundOne(call);
                }
                return super.visit(call);
            }

        };

        sqlNode.accept(visitor);
        switch (sqlNode.getKind()) {
            case INSERT:
                parseInsert((SqlInsert) sqlNode, tableOpsMap);
                break;
            case UPDATE:
                parseUpdate((SqlUpdate) sqlNode, tableOpsMap);
                break;
            case SELECT:
                parseSelect((SqlSelect) sqlNode, tableOpsMap);
                break;
            default:
                logger.error("Unhandled sql query type " + sqlNode.getKind() +" for query " + query);
        }
        return tableOpsMap;
    }

    private static void parseInsert(SqlInsert sqlInsert, Map<String, List<SQLOperation>> tableOpsMap) {
        String tableName = sqlInsert.getTargetTable().toString();
        //handle insert into select query
        if (sqlInsert.getSource().getKind()==SqlKind.SELECT) {
            parseSelect((SqlSelect) sqlInsert.getSource(), tableOpsMap);
        }
        List<SQLOperation> Ops = tableOpsMap.get(tableName);
        if (Ops == null)
            Ops = new ArrayList<>();
        Ops.add(SQLOperation.INSERT);
        tableOpsMap.put(tableName, Ops);
    }
    
    private static void parseUpdate(SqlUpdate sqlUpdate, Map<String, List<SQLOperation>> tableOpsMap) {
        SqlNode targetTable = sqlUpdate.getTargetTable();
        switch (targetTable.getKind()) {
            case IDENTIFIER:
                addIdentifierToMap(tableOpsMap, (SqlIdentifier) targetTable, SQLOperation.UPDATE);
                break;
            default:
                logger.error("Unable to process: " + targetTable.getKind() + " query");
        }
    }
    
    private static void parseSelect(SqlSelect sqlSelect, Map<String, List<SQLOperation>> tableOpsMap ) {
        SqlNode from = sqlSelect.getFrom();
        switch (from.getKind()) {
            case IDENTIFIER:
                addIdentifierToMap(tableOpsMap, (SqlIdentifier) from, SQLOperation.SELECT);
                break;
            case AS:
                SqlBasicCall as = (SqlBasicCall) from;
                SqlNode node = as.getOperandList().get(0);
                addIdentifierToMap(tableOpsMap, (SqlIdentifier) node, SQLOperation.SELECT);
                break;
            case JOIN:
                parseJoin((SqlJoin) from, tableOpsMap);
                break;
            default:
                logger.error("Unable to process: " + from.getKind() + " query");
        }
    }

    private static void parseJoin(SqlJoin join, Map<String, List<SQLOperation>> tableOpsMap) {
        switch (join.getLeft().getKind()) {
            case IDENTIFIER:
                addIdentifierToMap(tableOpsMap, (SqlIdentifier) join.getLeft(), SQLOperation.SELECT);
                break;
            case AS:
                SqlBasicCall as = (SqlBasicCall) join.getLeft();
                SqlNode node = as.getOperandList().get(0);
                addIdentifierToMap(tableOpsMap, (SqlIdentifier) node, SQLOperation.SELECT);
                break;
            default:
                logger.error("Error parsing join. Can't process left type: " + join.getLeft().getKind());        
        }
        
        switch (join.getRight().getKind()) {
            case IDENTIFIER:
                addIdentifierToMap(tableOpsMap, (SqlIdentifier) join.getRight(), SQLOperation.SELECT);
                break;
            case AS:
                SqlBasicCall as = (SqlBasicCall) join.getRight();
                SqlNode node = as.getOperandList().get(0);
                addIdentifierToMap(tableOpsMap, (SqlIdentifier) node, SQLOperation.SELECT);
                break;
            default:
                logger.error("Error parsing join. Can't process left type: " + join.getRight().getKind());        
        }
    }

    /**
     * Add the identifier, the range in the query, to the map
     * @param tableOpsMap
     * @param identifier
     * @param op
     */
    private static void addIdentifierToMap(Map<String, List<SQLOperation>> tableOpsMap, SqlIdentifier identifier,
            SQLOperation op) {
        List<SQLOperation> opList = tableOpsMap.get(identifier.toString());
        if (opList == null) opList = new ArrayList<>();
        opList.add(op);
        tableOpsMap.put(identifier.toString(), opList);
    }
    
    /**
     * Parse the string using basic string methods if parsing library fails
     * @param query
     * @return
     * @throws SQLException 
     */
    private static Map<String, List<SQLOperation>> basicStringParser(String query, Set<String> tables) throws SQLException {
        if (tables==null) {
            throw new SQLException("Unable to parse sql query: No tables to look for.");
        }
        Map<String, List<SQLOperation>> tableOpsMap = new HashMap<>();
        SQLOperation op;
        if (query.toUpperCase().startsWith("INSERT")) {
            op = SQLOperation.INSERT;
        } else if (query.toUpperCase().startsWith("UPDATE")) {
            op = SQLOperation.UPDATE;
        } else if (query.toUpperCase().startsWith("DELETE")) {
            op = SQLOperation.DELETE;
        } else if (query.toUpperCase().startsWith("SELECT")) {
            op = SQLOperation.SELECT;
        } else {
            throw new SQLException("Unable to parse sql query: " + query);
        }
        for (String table: tables) {
            if (query.toLowerCase().contains(table.toLowerCase())) {
                List<SQLOperation> opList = tableOpsMap.get(table);
                if (opList == null) opList = new ArrayList<>();
                opList.add(op);
                tableOpsMap.put(table.toString(), opList);
            }
        }
        return tableOpsMap;
    }
    

}
