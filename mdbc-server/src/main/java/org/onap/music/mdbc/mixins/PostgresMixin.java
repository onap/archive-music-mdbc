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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.*;
import java.util.Map.Entry;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.KeeperException.UnimplementedException;
import org.json.JSONObject;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.Configuration;
import org.onap.music.mdbc.MDBCUtils;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.TableInfo;
import org.onap.music.mdbc.mixins.MySQLMixin.StagingTableUpdateRunnable;
import org.onap.music.mdbc.tables.Operation;
import org.onap.music.mdbc.query.SQLOperation;
import org.onap.music.mdbc.tables.StagingTable;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * This class provides the methods that MDBC needs in order to mirror data to/from a
 * <a href="https://dev.mysql.com/">MySQL</a> or <a href="http://mariadb.org/">MariaDB</a> database instance. This class
 * uses the <code>JSON_OBJECT()</code> database function, which means it requires the following minimum versions of
 * either database:
 * <table summary="">
 * <tr>
 * <th>DATABASE</th>
 * <th>VERSION</th>
 * </tr>
 * <tr>
 * <td>MySQL</td>
 * <td>5.7.8</td>
 * </tr>
 * <tr>
 * <td>MariaDB</td>
 * <td>10.2.3 (Note: 10.2.3 is currently (July 2017) a <i>beta</i> release)</td>
 * </tr>
 * </table>
 *
 * @author Robert P. Eby
 */
public class PostgresMixin implements DBInterface {

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(PostgresMixin.class);

    public static final String MIXIN_NAME = "postgres";
    public static final String TRANS_TBL_SCHEMA = "audit";
    public static final String TRANS_TBL = "mdbc_translog";

    private final MusicInterface mi;
    private final String connId;
    private final String dbName;
    private final String schema;
    private final Connection jdbcConn;
    private final Map<String, TableInfo> tables;
    private PreparedStatement deleteStagingStatement;
    private boolean useAsyncStagingUpdate = false;
    private Object stagingHandlerLock = new Object();
    private AsyncUpdateHandler stagingHandler = null;
    private StagingTable currentStaging = null;

    public PostgresMixin() {
        this.mi = null;
        this.connId = "";
        this.dbName = null;
        this.schema = null;
        this.jdbcConn = null;
        this.tables = null;
        this.deleteStagingStatement = null;
    }

    private void initializeDeleteStatement() throws SQLException {
        deleteStagingStatement = jdbcConn.prepareStatement("DELETE FROM " + TRANS_TBL_SCHEMA + "." + TRANS_TBL
                + " WHERE (ix BETWEEN ? AND ? ) AND " + "connection_id = ?;");
    }

    public PostgresMixin(MusicInterface mi, String url, Connection conn, Properties info) throws SQLException {
        this.mi = mi;
        this.connId = generateConnID(conn);
        this.dbName = getDBName(conn);
        this.schema = getSchema(conn);
        this.jdbcConn = conn;
        this.tables = new HashMap<>();
        useAsyncStagingUpdate = Boolean.parseBoolean(info.getProperty(Configuration.KEY_ASYNC_STAGING_TABLE_UPDATE,
                Configuration.ASYNC_STAGING_TABLE_UPDATE));
        initializePostgresTriggersStructures();
        initializeDeleteStatement();
    }

    class StagingTableUpdateRunnable implements Runnable {

        private PostgresMixin mixin;
        private StagingTable staging;

        StagingTableUpdateRunnable(PostgresMixin mixin, StagingTable staging) {
            this.mixin = mixin;
            this.staging = staging;
        }

        @Override
        public void run() {
            try {
                this.mixin.updateStagingTable(staging);
            } catch (NoSuchFieldException | MDBCServiceException e) {
                this.mixin.logger.error("Error when updating the staging table");
            }
        }
    }


    private void createTriggerTable() throws SQLException {
        final String createSchemaSQL = "CREATE SCHEMA IF NOT EXISTS " + TRANS_TBL_SCHEMA + ";";
        final String revokeCreatePrivilegesSQL = "REVOKE CREATE ON schema " + TRANS_TBL_SCHEMA + " FROM public;";
        final String createTableSQL = "CREATE TABLE IF NOT EXISTS " + TRANS_TBL_SCHEMA + "." + TRANS_TBL + " ("
                + "ix serial," + "op TEXT NOT NULL CHECK (op IN ('I','D','U'))," + "schema_name text NOT NULL,"
                + "table_name text NOT NULL," + "original_data json," + "new_data json," + "connection_id text,"
                + "PRIMARY KEY (connection_id,ix)" + ") WITH (fillfactor=100);";
        final String revokeSQL = "REVOKE INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER ON " + TRANS_TBL_SCHEMA + "."
                + TRANS_TBL + " FROM public;";
        final String grantSelectSQL = "GRANT SELECT ON " + TRANS_TBL_SCHEMA + "." + TRANS_TBL + " TO public;";
        final String createIndexSQL = "CREATE INDEX IF NOT EXISTS logged_actions_connection_id_idx" + " ON "
                + TRANS_TBL_SCHEMA + "." + TRANS_TBL + " (connection_id);";
        Map<String, String> sqlStatements = new LinkedHashMap<String, String>() {
            {
                put("create_schema", createSchemaSQL);
                put("revoke_privileges", revokeCreatePrivilegesSQL);
                put("create_table", createTableSQL);
                put("revoke_sql", revokeSQL);
                put("grant_select", grantSelectSQL);
                put("create_index", createIndexSQL);
            }
        };
        for (Entry<String, String> query : sqlStatements.entrySet()) {
            int retryCount = 0;
            boolean ready = false;
            while (retryCount < 3 && !ready) {
                try {
                    Statement statement = jdbcConn.createStatement();
                    statement.executeUpdate(query.getValue());
                    if (!jdbcConn.getAutoCommit()) {
                        jdbcConn.commit();
                    }
                    statement.close();
                    ready = true;
                } catch (SQLException e) {
                    if (e.getMessage().equalsIgnoreCase("ERROR: tuple concurrently updated") || e.getMessage()
                            .toLowerCase().startsWith("error: duplicate key value violates unique constraint")) {
                        logger.warn("Error creating schema, retrying. for " + query.getKey(), e);
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e1) {
                        }
                    } else {
                        logger.error("Error executing " + query.getKey(), e);
                        throw e;
                    }
                }
                retryCount++;
            }
        }
    }

    private String updateTriggerSection() {
        return "IF (TG_OP = 'UPDATE') THEN\n" + "v_old_data := row_to_json(OLD);\n"
                + "v_new_data := row_to_json(NEW);\n" + "INSERT INTO " + TRANS_TBL_SCHEMA + "." + TRANS_TBL
                + " (op,schema_name,table_name,original_data,new_data,connection_id)\n"
                + "VALUES (substring(TG_OP,1,1),TG_TABLE_SCHEMA::TEXT,TG_TABLE_NAME::TEXT,v_old_data,v_new_data,pg_backend_pid());\n"
                + "RETURN NEW; ";
    }

    private String insertTriggerSection() {
        // \TODO add additinoal conditional on change "IF NEW IS DISTINCT FROM OLD THEN"
        return "IF (TG_OP = 'INSERT') THEN\n" + "v_new_data := row_to_json(NEW);\n" + "INSERT INTO " + TRANS_TBL_SCHEMA
                + "." + TRANS_TBL + " (op,schema_name,table_name,new_data,connection_id)\n"
                + "VALUES (substring(TG_OP,1,1),TG_TABLE_SCHEMA::TEXT,TG_TABLE_NAME::TEXT,v_new_data,pg_backend_pid());\n"
                + "RETURN NEW; ";
    }

    private String deleteTriggerSection() {
        return "IF (TG_OP = 'DELETE') THEN\n" + "v_old_data := row_to_json(OLD);\n" + "INSERT INTO " + TRANS_TBL_SCHEMA
                + "." + TRANS_TBL + " (op,schema_name,table_name,original_data,connection_id)\n"
                + "VALUES (substring(TG_OP,1,1),TG_TABLE_SCHEMA::TEXT,TG_TABLE_NAME::TEXT,v_old_data,pg_backend_pid());\n"
                + "RETURN OLD; ";
    }

    private String functionName(SQLOperation type) {
        final String functionName = (type.equals(SQLOperation.UPDATE)) ? "if_updated_func"
                : (type.equals(SQLOperation.INSERT)) ? "if_inserted_func" : "if_deleted_func";
        return "audit." + functionName + "()";
    }

    private void createTriggerFunctions(SQLOperation type) throws SQLException {
        StringBuilder functionSQL =
                new StringBuilder("CREATE OR REPLACE FUNCTION " + functionName(type) + " RETURNS TRIGGER AS $body$\n"
                        + "DECLARE\n" + "v_old_data json;\n" + "v_new_data json;\n" + "BEGIN\n");
        switch (type) {
            case UPDATE:
                functionSQL.append(updateTriggerSection());
                break;
            case INSERT:
                functionSQL.append(insertTriggerSection());
                break;
            case DELETE:
                functionSQL.append(deleteTriggerSection());
                break;
            default:
                throw new IllegalArgumentException("Invalid operation type for creation of trigger functions");
        }
        functionSQL.append("ELSE\n"
                + "RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();\n"
                + "RETURN NULL;\n" + "END IF;\n" + "EXCEPTION\n" + "WHEN data_exception THEN\n"
                + "RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;\n"
                + "RETURN NULL;\n" + "WHEN unique_violation THEN\n"
                + "RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;\n"
                + "RETURN NULL;\n" + "WHEN OTHERS THEN\n"
                + "RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;\n"
                + "RETURN NULL;\n" + "END;\n" + "$body$\n" + "LANGUAGE plpgsql\n" + "SECURITY DEFINER\n"
                + "SET search_path = pg_catalog, audit;");
        int retryCount = 0;
        boolean ready = false;
        while (retryCount < 3 && !ready) {
            try {
                executeSQLWrite(functionSQL.toString());
                ready = true;
            } catch (SQLException e) {
                if (e.getMessage().equalsIgnoreCase("ERROR: tuple concurrently updated")) {
                    logger.warn("Error creating schema, retrying. ", e);
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e1) {
                    }
                } else {
                    logger.error("Error executing creation of trigger function", e);
                    throw e;
                }
            }
            retryCount++;
        }
    }

    private void initializePostgresTriggersStructures() throws SQLException {
        try {
            createTriggerTable();
        } catch (SQLException e) {
            logger.error("Error creating the trigger tables in postgres", e);
            throw e;
        }
        try {
            createTriggerFunctions(SQLOperation.INSERT);
            createTriggerFunctions(SQLOperation.UPDATE);
            createTriggerFunctions(SQLOperation.DELETE);
        } catch (SQLException e) {
            logger.error("Error creating the trigger functions in postgres", e);
            throw e;
        }
    }

    // This is used to generate a unique connId for this connection to the DB.
    private String generateConnID(Connection conn) {
        String rv = Integer.toString((int) System.currentTimeMillis()); // random-ish
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT pg_backend_pid() AS IX");
            if (rs.next()) {
                rv = rs.getString("IX");
            }
            stmt.close();
        } catch (SQLException e) {
            logger.error(EELFLoggerDelegate.errorLogger, "generateConnID: problem generating a connection ID!");
        }
        return rv;
    }

    /**
     * Get the name of this DBnterface mixin object.
     * 
     * @return the name
     */
    @Override
    public String getMixinName() {
        return MIXIN_NAME;
    }

    @Override
    public void close() {
        // nothing yet
    }

    /**
     * Determines the db name associated with the connection This is the private/internal method that actually
     * determines the name
     * 
     * @param conn
     * @return
     */
    private String getDBName(Connection conn) {
        String dbname = "mdbc"; // default name
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT current_database();");
            if (rs.next()) {
                dbname = rs.getString("current_database");
            }
            stmt.close();
        } catch (SQLException e) {
            logger.error(EELFLoggerDelegate.errorLogger, "getDBName: problem getting database name from mysql");
        }
        return dbname;
    }

    /**
     * Determines the db name associated with the connection This is the private/internal method that actually
     * determines the name
     * 
     * @param conn
     * @return
     */
    private String getSchema(Connection conn) {
        String schema = "public"; // default name
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT current_schema();");
            if (rs.next()) {
                schema = rs.getString("current_schema");
            }
            stmt.close();
        } catch (SQLException e) {
            logger.error(EELFLoggerDelegate.errorLogger, "getDBName: problem getting database name from mysql");
        }
        return schema;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getDatabaseName() {
        return this.dbName;
    }

    /**
     * Get a set of the table names in the database.
     * 
     * @return the set
     */
    @Override
    public Set<Range> getSQLRangeSet() {
        Set<String> set = new TreeSet<String>();
        String sql =
                "SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema=current_schema();";
        try {
            Statement stmt = jdbcConn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String s = rs.getString("table_name");
                set.add(s);
            }
            stmt.close();
        } catch (SQLException e) {
            logger.error(EELFLoggerDelegate.errorLogger, "getSQLTableSet: " + e);
        }
        logger.debug(EELFLoggerDelegate.applicationLogger, "getSQLTableSet returning: " + set);
        Set<Range> rangeSet = new HashSet<>();
        for (String table : set) {
            rangeSet.add(new Range(table));
        }
        return rangeSet;
    }

    /**
     * Return a TableInfo object for the specified table. This method first looks in a cache of previously constructed
     * TableInfo objects for the table. If not found, it queries the INFORMATION_SCHEMA.COLUMNS table to obtain the
     * column names, types, and indexes of the table. It creates a new TableInfo object with the results.
     * 
     * @param tableName the table to look up
     * @return a TableInfo object containing the info we need, or null if the table does not exist
     */
    @Override
    public TableInfo getTableInfo(String tableName) {
        // \TODO: invalidate "tables" when a table schema is modified (uncommon), but needs to be handled
        TableInfo ti = tables.get(tableName);
        if (ti == null) {
            try {
                String tbl, localSchema;
                final String[] split = tableName.split("\\.");
                if (split.length == 2) {
                    localSchema = split[0];
                    tbl = split[1];
                } else {
                    tbl = tableName;
                    localSchema = this.schema;
                }
                String sql;
                if (schema == null) {
                    sql = "select column_name, data_type from information_schema.columns where table_schema=current_schema() and table_name='"
                            + tbl + "';";
                } else {
                    sql = "select column_name, data_type from information_schema.columns where table_schema='"
                            + localSchema + "' and table_name='" + tbl + "';";
                }
                ResultSet rs = executeSQLRead(sql);
                if (rs != null) {
                    ti = new TableInfo();
                    while (rs.next()) {
                        String name = rs.getString("column_name");
                        String type = rs.getString("data_type");
                        ti.columns.add(name);
                        ti.coltype.add(mapDatatypeNameToType(type));
                    }
                    rs.getStatement().close();
                } else {
                    logger.error(EELFLoggerDelegate.errorLogger,
                            "Cannot retrieve table info for table " + tableName + " from POSTGRES.");
                    return null;
                }
                final String keysql =
                        "SELECT a.attname as column_name FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid"
                                + " AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = '" + tbl + "'::regclass "
                                + " AND i.indisprimary;";
                ResultSet rs2 = executeSQLRead(keysql);
                Set<String> keycols = new HashSet<>();
                if (rs2 != null) {
                    while (rs2.next()) {
                        String name = rs2.getString("column_name");
                        keycols.add(name);
                    }
                    rs2.getStatement().close();
                } else {
                    logger.error(EELFLoggerDelegate.errorLogger,
                            "Cannot retrieve table info for table " + tableName + " from MySQL.");
                }
                for (String col : ti.columns) {
                    if (keycols.contains(col)) {
                        ti.iskey.add(true);
                    } else {
                        ti.iskey.add(false);
                    }
                }
            } catch (SQLException e) {
                logger.error(EELFLoggerDelegate.errorLogger,
                        "Cannot retrieve table info for table " + tableName + " from MySQL: " + e);
                return null;
            }
            tables.put(tableName, ti);
        }
        return ti;
    }

    // Map Postgres data type names to the java.sql.Types equivalent
    private int mapDatatypeNameToType(String nm) {
        switch (nm) {
            case "character":
                return Types.CHAR;
            case "national character":
                return Types.NCHAR;
            case "character varying":
                return Types.VARCHAR;
            case "national character varying":
                return Types.NVARCHAR;
            case "text":
                return Types.VARCHAR;
            case "bytea":
                return Types.BINARY;
            case "smallint":
                return Types.SMALLINT;
            case "integer":
                return Types.INTEGER;
            case "bigint":
                return Types.BIGINT;
            case "smallserial":
                return Types.SMALLINT;
            case "serial":
                return Types.INTEGER;
            case "bigserial":
                return Types.BIGINT;
            case "real":
                return Types.REAL;
            case "double precision":
                return Types.DOUBLE;
            case "numeric":
                return Types.NUMERIC;
            case "decimal":
                return Types.DECIMAL;
            case "date":
                return Types.DATE;
            case "time with time zone":
                return Types.TIME_WITH_TIMEZONE;
            case "time without time zone":
                return Types.TIME;
            case "timestamp without time zone":
                return Types.TIMESTAMP;
            case "timestamp with time zone":
                return Types.TIMESTAMP_WITH_TIMEZONE;
            case "boolean":
                return Types.BIT;
            case "bit":
                return Types.BIT;
            case "oid":
                return Types.BIGINT;
            case "xml":
                return Types.SQLXML;
            case "array":
                return Types.ARRAY;
            case "tinyint":
                return Types.TINYINT;
            case "uuid":
            case "money":
            case "interval":
            case "bit varying":
            case "box":
            case "point":
            case "lseg":
            case "path":
            case "polygon":
            case "circle":
            case "json":
            case "inet":
            case "cidr":
            case "macaddr":
            case "tsvector":
            case "tsquery":
                return Types.OTHER;
            default:
                logger.error(EELFLoggerDelegate.errorLogger, "unrecognized and/or unsupported data type " + nm);
                return Types.VARCHAR;
        }
    }

    @Override
    public void createSQLTriggers(String tableName) {
        // Don't create triggers for the table the triggers write into!!!
        if (tableName.equals(TRANS_TBL) || tableName.equals(TRANS_TBL_SCHEMA + "." + TRANS_TBL))
            return;
        try {
            // No SELECT trigger
            executeSQLWrite(generateTrigger(tableName, SQLOperation.INSERT));
            executeSQLWrite(generateTrigger(tableName, SQLOperation.DELETE));
            executeSQLWrite(generateTrigger(tableName, SQLOperation.UPDATE));
        } catch (SQLException e) {
            if (e.getMessage().trim().endsWith("already exists")) {
                // only warn if trigger already exists
                logger.warn(EELFLoggerDelegate.applicationLogger, "createSQLTriggers" + e);
            } else {
                logger.error(EELFLoggerDelegate.errorLogger, "createSQLTriggers: " + e);
            }
        }
    }

    private String generateTrigger(String tableName, SQLOperation op) {
        StringBuilder triggerSql = new StringBuilder("CREATE TRIGGER ").append(getTriggerName(tableName, op))
                .append(" AFTER " + op + " ON ").append(tableName).append(" FOR EACH ROW EXECUTE PROCEDURE ")
                .append(functionName(op)).append(';');
        return triggerSql.toString();
    }

    private String getTriggerName(String tableName, SQLOperation op) {
        switch (op) {
            case DELETE:
                return "D_" + tableName;
            case UPDATE:
                return "U_" + tableName;
            case INSERT:
                return "I_" + tableName;
            default:
                throw new IllegalArgumentException("Invalid option in trigger operation type");
        }
    }

    private String[] getTriggerNames(String tableName) {
        return new String[] {getTriggerName(tableName, SQLOperation.INSERT),
                getTriggerName(tableName, SQLOperation.DELETE), getTriggerName(tableName, SQLOperation.UPDATE),};
    }

    @Override
    public void dropSQLTriggers(String tableName) {
        try {
            for (String name : getTriggerNames(tableName)) {
                logger.debug(EELFLoggerDelegate.applicationLogger, "REMOVE trigger " + name + " from postgres");
                executeSQLWrite("DROP TRIGGER IF EXISTS " + name + ";");
            }
        } catch (SQLException e) {
            logger.error(EELFLoggerDelegate.errorLogger, "dropSQLTriggers: " + e);
        }
    }

    @Override
    public void insertRowIntoSqlDb(String tableName, Map<String, Object> map) {
        throw new org.apache.commons.lang.NotImplementedException("Function not implemented yet in postgres");
    }

    @Override
    public void deleteRowFromSqlDb(String tableName, Map<String, Object> map) {
        throw new org.apache.commons.lang.NotImplementedException("Function not implemented yet in postgres");
    }

    /**
     * This method executes a read query in the SQL database. Methods that call this method should be sure to call
     * resultset.getStatement().close() when done in order to free up resources.
     * 
     * @param sql the query to run
     * @return a ResultSet containing the rows returned from the query
     */
    @Override
    public ResultSet executeSQLRead(String sql) {
        logger.debug(EELFLoggerDelegate.applicationLogger, "Executing sql read in postgres");
        logger.debug("Executing SQL read:" + sql);
        ResultSet rs;
        try {
            Statement stmt = jdbcConn.createStatement();
            rs = stmt.executeQuery(sql);
        } catch (SQLException e) {
            logger.error(EELFLoggerDelegate.errorLogger, "executeSQLRead" + e);
            return null;
        }
        return rs;
    }

    /**
     * This method executes a write query in the sql database.
     * 
     * @param sql the SQL to be sent to MySQL
     * @throws SQLException if an underlying JDBC method throws an exception
     */
    protected void executeSQLWrite(String sql) throws SQLException {
        logger.debug(EELFLoggerDelegate.applicationLogger, "Executing SQL write:" + sql);
        Statement stmt = jdbcConn.createStatement();
        stmt.execute(sql);
        stmt.close();
    }

    @Override
    public void preCommitHook() {
        synchronized (stagingHandlerLock) {
            // \TODO check if this can potentially block forever in certain scenarios
            if (stagingHandler != null) {
                stagingHandler.waitForAllPendingUpdates();
            }
        }
    }

    /**
     * Code to be run within the DB driver before a SQL statement is executed. This is where tables can be synchronized
     * before a SELECT, for those databases that do not support SELECT triggers.
     * 
     * @param sql the SQL statement that is about to be executed
     * @return list of keys that will be updated, if they can't be determined afterwards (i.e. sql table doesn't have
     *         primary key)
     */
    @Override
    public void preStatementHook(final String sql) {
        if (sql == null) {
            return;
        }
        // \TODO: check if anything needs to be executed here for postgres
    }

    /**
     * Code to be run within the DB driver after a SQL statement has been executed. This is where remote statement
     * actions can be copied back to Cassandra/MUSIC.
     * 
     * @param sql the SQL statement that was executed
     */
    @Override
    public void postStatementHook(final String sql, StagingTable transactionDigest) {
        if (sql != null) {
            String[] parts = sql.trim().split(" ");
            String cmd = parts[0].toLowerCase();
            if ("delete".equals(cmd) || "insert".equals(cmd) || "update".equals(cmd)) {
                if (useAsyncStagingUpdate) {
                    synchronized (stagingHandlerLock) {
                        if (stagingHandler == null || currentStaging != transactionDigest) {
                            Runnable newRunnable =
                                    new PostgresMixin.StagingTableUpdateRunnable(this, transactionDigest);
                            currentStaging = transactionDigest;
                            stagingHandler = new AsyncUpdateHandler(newRunnable);
                        }
                        // else we can keep using the current staging Handler
                    }
                    stagingHandler.processNewUpdate();
                } else {

                    try {
                        this.updateStagingTable(transactionDigest);
                    } catch (NoSuchFieldException | MDBCServiceException e) {
                        // TODO Auto-generated catch block
                        this.logger.error("Error updating the staging table");
                    }
                }
            }
        }
    }

    private SQLOperation toOpEnum(String operation) throws NoSuchFieldException {
        switch (operation.toLowerCase()) {
            case "i":
                return SQLOperation.INSERT;
            case "d":
                return SQLOperation.DELETE;
            case "u":
                return SQLOperation.UPDATE;
            case "s":
                return SQLOperation.SELECT;
            default:
                logger.error(EELFLoggerDelegate.errorLogger, "Invalid operation selected: [" + operation + "]");
                throw new NoSuchFieldException("Invalid operation enum");
        }

    }

    /**
     * Copy data that is in transaction table into music interface
     * 
     * @param transactionDigests
     * @throws NoSuchFieldException
     */
    private void updateStagingTable(StagingTable transactionDigests) throws NoSuchFieldException, MDBCServiceException {
        String selectSql = "select ix, op, schema_name, table_name, original_data,new_data FROM " + TRANS_TBL_SCHEMA
                + "." + TRANS_TBL + " where connection_id = '" + this.connId + "';";
        Integer biggestIx = Integer.MIN_VALUE;
        Integer smallestIx = Integer.MAX_VALUE;
        try {
            ResultSet rs = executeSQLRead(selectSql);
            Set<Integer> rows = new TreeSet<Integer>();
            while (rs.next()) {
                int ix = rs.getInt("ix");
                biggestIx = Integer.max(biggestIx, ix);
                smallestIx = Integer.min(smallestIx, ix);
                String op = rs.getString("op");
                SQLOperation opType = toOpEnum(op);
                String schema = rs.getString("schema_name");
                String tbl = rs.getString("table_name");
                String original = rs.getString("original_data");
                String newData = rs.getString("new_data");
                Range range = new Range(schema + "." + tbl);
                transactionDigests.addOperation(range, opType, newData, original);
                rows.add(ix);
            }
            rs.getStatement().close();
            if (rows.size() > 0) {
                logger.debug("Staging delete: Executing with vals [" + smallestIx + "," + biggestIx + "," + this.connId
                        + "]");
                this.deleteStagingStatement.setInt(1, smallestIx);
                this.deleteStagingStatement.setInt(2, biggestIx);
                this.deleteStagingStatement.setString(3, this.connId);
                this.deleteStagingStatement.execute();
            }
        } catch (SQLException e) {
            logger.warn("Exception in postStatementHook: " + e);
            e.printStackTrace();
        }
    }



    /**
     * Update music with data from MySQL table
     *
     * @param tableName - name of table to update in music
     */
    @Override
    public void synchronizeData(String tableName) {}

    /**
     * Return a list of "reserved" names, that should not be used by MySQL client/MUSIC These are reserved for mdbc
     */
    @Override
    public List<String> getReservedTblNames() {
        ArrayList<String> rsvdTables = new ArrayList<String>();
        rsvdTables.add(TRANS_TBL_SCHEMA + "." + TRANS_TBL);
        rsvdTables.add(TRANS_TBL);
        // Add others here as necessary
        return rsvdTables;
    }

    @Override
    public String getPrimaryKey(String sql, String tableName) {
        return null;
    }

    /**
     * Parse the transaction digest into individual events
     * 
     * @param transaction - base 64 encoded, serialized digest
     */
    @Override
    public void replayTransaction(StagingTable transaction, Set<Range> ranges)
            throws SQLException, MDBCServiceException {
        boolean autocommit = jdbcConn.getAutoCommit();
        jdbcConn.setAutoCommit(false);
        Statement jdbcStmt = jdbcConn.createStatement();
        final ArrayList<Operation> opList = transaction.getOperationList();

        for (Operation op : opList) {
            if (Range.overlaps(ranges, op.getTable())) {
                try {
                    replayOperationIntoDB(jdbcStmt, op);
                } catch (SQLException | MDBCServiceException e) {
                    // rollback transaction
                    logger.error("Unable to replay: " + op.getOperationType() + "->" + op.getVal() + "."
                            + "Rolling back the entire digest replay.");
                    jdbcConn.rollback();
                    throw e;
                }
            }
        }

        clearReplayedOperations(jdbcStmt);
        jdbcConn.commit();
        jdbcStmt.close();
        jdbcConn.setAutoCommit(autocommit);
    }

    @Override
    public void disableForeignKeyChecks() throws SQLException {
        Statement disable = jdbcConn.createStatement();
        disable.execute("SET session_replication_role = 'replica';");
        disable.closeOnCompletion();
    }

    @Override
    public void enableForeignKeyChecks() throws SQLException {
        Statement enable = jdbcConn.createStatement();
        enable.execute("SET session_replication_role = 'origin';");
        enable.closeOnCompletion();
    }

    @Override
    public void applyTxDigest(StagingTable txDigest, Set<Range> ranges) throws SQLException, MDBCServiceException {
        replayTransaction(txDigest, ranges);
    }

    /**
     * Replays operation into database, usually from txDigest
     * 
     * @param jdbcStmt: Connection used to perform the replay
     * @param op: operation to be replayed
     * @throws SQLException
     * @throws MDBCServiceException
     */
    private void replayOperationIntoDB(Statement jdbcStmt, Operation op) throws SQLException, MDBCServiceException {
        logger.debug("Replaying Operation: " + op.getOperationType() + "->" + op.getVal());
        JSONObject newVal = op.getVal();
        JSONObject oldVal = null;
        try {
            oldVal = op.getKey();
        } catch (MDBCServiceException e) {
            // Ignore exception, in postgres the structure of the operation is different
        }


        TableInfo ti = getTableInfo(op.getTable());
        final List<String> keyColumns = ti.getKeyColumns();

        // build and replay the queries
        String sql = constructSQL(op, keyColumns, newVal, oldVal);
        if (sql == null)
            return;

        try {
            logger.debug("Replaying operation: " + sql);
            int updated = jdbcStmt.executeUpdate(sql);

            if (updated == 0) {
                // This applies only for replaying transactions involving Eventually Consistent tables
                logger.warn(
                        "Error Replaying operation: " + sql + "; Replacing insert/replace/viceversa and replaying ");

                buildAndExecuteSQLInverse(jdbcStmt, op, keyColumns, newVal, oldVal);
            }
        } catch (SQLException sqlE) {
            // This applies for replaying transactions involving Eventually Consistent tables
            // or transactions that replay on top of existing keys
            logger.warn(
                    "Error Replaying operation: " + sql + ";" + "Replacing insert/replace/viceversa and replaying ");

            buildAndExecuteSQLInverse(jdbcStmt, op, keyColumns, newVal, oldVal);

        }
    }

    protected void buildAndExecuteSQLInverse(Statement jdbcStmt, Operation op, List<String> keyColumns,
            JSONObject newVals, JSONObject oldVals) throws SQLException, MDBCServiceException {
        String sqlInverse = constructSQLInverse(op, keyColumns, newVals, oldVals);
        if (sqlInverse == null)
            return;
        logger.debug("Replaying operation: " + sqlInverse);
        jdbcStmt.executeUpdate(sqlInverse);
    }

    protected String constructSQLInverse(Operation op, List<String> keyColumns, JSONObject newVals, JSONObject oldVals)
            throws MDBCServiceException {
        String sqlInverse = null;
        switch (op.getOperationType()) {
            case INSERT:
                sqlInverse = constructUpdate(op.getTable(), keyColumns, newVals, oldVals);
                break;
            case UPDATE:
                sqlInverse = constructInsert(op.getTable(), newVals);
                break;
            default:
                break;
        }
        return sqlInverse;
    }

    protected String constructSQL(Operation op, List<String> keyColumns, JSONObject newVals, JSONObject oldVals)
            throws MDBCServiceException {
        String sql = null;
        switch (op.getOperationType()) {
            case INSERT:
                sql = constructInsert(op.getTable(), newVals);
                break;
            case UPDATE:
                sql = constructUpdate(op.getTable(), keyColumns, newVals, oldVals);
                break;
            case DELETE:
                sql = constructDelete(op.getTable(), keyColumns, oldVals);
                break;
            case SELECT:
                // no update happened, do nothing
                break;
            default:
                logger.error(op.getOperationType() + "not implemented for replay");
        }
        return sql;
    }

    private String constructDelete(String tableName, List<String> keyColumns, JSONObject oldVals)
            throws MDBCServiceException {
        if (oldVals == null) {
            throw new MDBCServiceException("Trying to update row with an empty old val exception");
        }
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ");
        sql.append(tableName + " WHERE ");
        sql.append(getPrimaryKeyConditional(keyColumns, oldVals));
        sql.append(";");
        return sql.toString();
    }

    private String constructInsert(String tableName, JSONObject newVals) {
        StringBuilder keys = new StringBuilder();
        StringBuilder vals = new StringBuilder();
        String sep = "";
        for (String col : newVals.keySet()) {
            keys.append(sep + col);
            vals.append(sep + "'" + newVals.get(col) + "'");
            sep = ", ";
        }
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName + " (").append(keys).append(") VALUES (").append(vals).append(");");
        return sql.toString();
    }

    private String constructUpdate(String tableName, List<String> keyColumns, JSONObject newVals, JSONObject oldVals)
            throws MDBCServiceException {
        if (oldVals == null) {
            throw new MDBCServiceException("Trying to update row with an empty old val exception");
        }
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(tableName).append(" SET ");
        String sep = "";
        for (String key : newVals.keySet()) {
            sql.append(sep).append(key).append("=\"").append(newVals.get(key)).append("\"");
            sep = ", ";
        }
        sql.append(" WHERE ");
        sql.append(getPrimaryKeyConditional(keyColumns, oldVals));
        sql.append(";");
        return sql.toString();
    }

    /**
     * Create an SQL string for AND'ing all of the primary keys
     * 
     * @param keyColumns list with the name of the columns that are key
     * @param vals json with the contents of the old row
     * @return string in the form of PK1=Val1 AND PK2=Val2 AND PK3=Val3
     */
    private String getPrimaryKeyConditional(List<String> keyColumns, JSONObject vals) {
        StringBuilder keyCondStmt = new StringBuilder();
        String and = "";
        for (String key : keyColumns) {
            // We cannot use the default primary key for the sql table and operations
            if (!key.equals(mi.getMusicDefaultPrimaryKeyName())) {
                Object val = vals.get(key);
                keyCondStmt.append(and + key + "=\"" + val + "\"");
                and = " AND ";
            }
        }
        return keyCondStmt.toString();
    }

    /**
     * Cleans out the transaction table, removing the replayed operations
     * 
     * @param jdbcStmt
     * @throws SQLException
     */
    private void clearReplayedOperations(Statement jdbcStmt) throws SQLException {
        logger.info("Clearing replayed operations");
        String sql =
                "DELETE FROM " + TRANS_TBL_SCHEMA + "." + TRANS_TBL + " WHERE CONNECTION_ID = '" + this.connId + "';";
        jdbcStmt.executeUpdate(sql);
    }

    @Override
    public Connection getSQLConnection() {
        return jdbcConn;
    }

    @Override
    public Set<String> getSQLTableSet() {
        Set<String> set = new TreeSet<String>();
        String sql =
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=CURRENT_SCHEMA() AND TABLE_TYPE='BASE TABLE'";
        try {
            Statement stmt = jdbcConn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String s = rs.getString("TABLE_NAME");
                set.add(s);
            }
            stmt.close();
        } catch (SQLException e) {
            logger.error(EELFLoggerDelegate.errorLogger, "getSQLTableSet: " + e);
            System.out.println("getSQLTableSet: " + e);
            e.printStackTrace();
        }
        logger.debug(EELFLoggerDelegate.applicationLogger, "getSQLTableSet returning: " + set);
        System.out.println("getSQLTableSet returning: " + set);
        return set;
    }

    @Override
    public void updateCheckpointLocations(Range r, Pair<UUID, Integer> playbackPointer) {
        throw new org.apache.commons.lang.NotImplementedException();
    }

    @Override
    public void initTables() {
        throw new org.apache.commons.lang.NotImplementedException();
    }

}
