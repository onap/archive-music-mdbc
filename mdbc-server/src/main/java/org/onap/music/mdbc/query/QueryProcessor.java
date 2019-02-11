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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
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

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class QueryProcessor {

    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(QueryProcessor.class);

    public List<String> tables = null;

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
     * @return map of table name to {@link org.onap.music.mdbc.query.SQLOperation}
     * @throws SQLException 
     * @throws SqlParseException
     */
    public static Map<String, List<SQLOperation>> parseSqlQuery(String query) throws SQLException {
        logger.info(EELFLoggerDelegate.applicationLogger, "Parsing query: "+query);
        Map<String, List<SQLOperation>> tableOpsMap = new HashMap<>();
        //for Create no need to check locks.
        if(query.toUpperCase().startsWith("CREATE"))  {
            logger.error(EELFLoggerDelegate.errorLogger, "CREATE TABLE DDL not currently supported currently.");
            return tableOpsMap;
        }

        /*SqlParser parser = SqlParser.create(query);
		SqlNode sqlNode = parser.parseQuery();*/
        SqlNode sqlNode;
        try {
            sqlNode = getSqlParser(query).parseStmt();
        } catch (SqlParseException e) {
            logger.error(EELFLoggerDelegate.errorLogger, "Unable to parse query: " + query +". " + e.getMessage());
            throw new SQLException("Unable to parse query: " + query);
        }

        SqlBasicVisitor<Void> visitor = new SqlBasicVisitor<Void>() {

            public Void visit(SqlCall call) {
                if (call.getOperator() instanceof SqlInOperator) {
                    throw new Util.FoundOne(call);
                }
                return super.visit(call);
            }

        };

        // sqlNode.accept(new SqlAnalyzer());
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

    private static void parseInsert(SqlInsert sqlNode, Map<String, List<SQLOperation>> tableOpsMap) {
        SqlInsert sqlInsert = (SqlInsert) sqlNode;
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
    
    private static void parseUpdate(SqlUpdate sqlNode, Map<String, List<SQLOperation>> tableOpsMap) {
        SqlUpdate sqlUpdate = (SqlUpdate) sqlNode;
        String tableName = sqlUpdate.getTargetTable().toString();
        List<SQLOperation> Ops = tableOpsMap.get(tableName);
        if (Ops == null)
            Ops = new ArrayList<>();
        Ops.add(SQLOperation.UPDATE);
        tableOpsMap.put(tableName, Ops);
    }
    
    private static void parseSelect(SqlSelect sqlNode, Map<String, List<SQLOperation>> tableOpsMap ) {
        SqlSelect sqlSelect = (SqlSelect) sqlNode;
        SqlNodeList selectList = sqlSelect.getSelectList();
        String tables = sqlSelect.getFrom().toString();
        String[] tablesArr = tables.split(",");

        for (String table : tablesArr) {

            String tableName = null;
            if(table.contains("`")) {
                String[] split = table.split("`");
                tableName = split[1];
            } else {
                tableName = table;
            }
            List<SQLOperation> Ops = tableOpsMap.get(tableName);
            if (Ops == null) Ops = new ArrayList<>();
            Ops.add(SQLOperation.SELECT);
            tableOpsMap.put(tableName, Ops);
        }
    }

    /**
     * Get the maximum operation level in a list of operations.
     * If multiple write operations, this list will return write operation.
     * Multiple write and read operations returns write
     * Multiple read operations return read
     * @param list
     * @return
     */
    public static SQLOperationType getOperationType(List<SQLOperation> list) { 
        for (SQLOperation op: list) {
            if (op.getOperationType()!= SQLOperationType.READ) {
                return op.getOperationType();
            }
        }
        return SQLOperationType.READ;
    }

}
