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

import static org.junit.Assert.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.mdbc.MdbcTestUtils;
import org.onap.music.mdbc.MdbcTestUtils.DBType;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.TableInfo;
import org.onap.music.mdbc.tables.StagingTable;

public class PostgresMixinTest {
    final private static String keyspace="metricmusictest";
    final private static String mdbcServerName = "name";

    static PostgresMixin mixin;

    private static MusicMixin mi = null;
    private static Connection conn;

    @BeforeClass
    public static void init() throws MDBCServiceException {
        MdbcTestUtils.initCassandra();
        mi=MdbcTestUtils.getMusicMixin();
        try {
            conn = MdbcTestUtils.getConnection(DBType.POSTGRES);
            Properties info = new Properties();
            mixin = new PostgresMixin(mi, null, conn, info);
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @AfterClass
    public static void close(){
        //TODO: shutdown cassandra
        mixin=null;
        MdbcTestUtils.cleanDatabase(DBType.POSTGRES);
        MdbcTestUtils.stopCassandra();
     }

    @Test
    public void getMixinName() {
        final String mixinName = mixin.getMixinName();
        assertEquals(mixinName.toLowerCase(),"postgres");
    }

    @Test
    public void getSQLTableSet() {
        createTestTable();
        final Set<String> sqlTableSet = mixin.getSQLTableSet();
        assertEquals(1,sqlTableSet.size());
        assertTrue(sqlTableSet.contains("testtable"));
    }

    @Test
    public void getTableInfo() {
        createTestTable();
        final TableInfo tableInfo = mixin.getTableInfo("testtable");
        assertNotNull(tableInfo);
        assertEquals(3,tableInfo.columns.size());
        int index=0;
        for(String col: tableInfo.columns) {
            switch(col.toLowerCase()){
                case "ix":
                    assertTrue(tableInfo.iskey.get(index));
                    assertEquals(Types.INTEGER, tableInfo.coltype.get(index).intValue());
                    break;
                case "test1":
                    assertFalse(tableInfo.iskey.get(index));
                    assertEquals(Types.CHAR, tableInfo.coltype.get(index).intValue());
                    break;
                case "test2":
                    assertFalse(tableInfo.iskey.get(index));
                    assertEquals(Types.VARCHAR, tableInfo.coltype.get(index).intValue());
                    break;
                default:
                    fail();
            }
            index++;
        }
    }

    private void createTestTable() {
        try {
            final Statement statement = conn.createStatement();
            statement.execute("CREATE TABLE IF NOT EXISTS testtable (IX SERIAL, test1 CHAR(1), test2 VARCHAR(255), PRIMARY KEY (IX));");
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    private void cleanTestTable() {
        try {
            final Statement statement = conn.createStatement();
            statement.execute("DELETE FROM testtable;");
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void postStatementHook() {
        createTestTable();
        mixin.createSQLTriggers("testtable");
        final String sqlOperation = "INSERT INTO testtable (test1,test2) VALUES ('u','test');";
        Statement stm=null;
        try {
            stm = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
        mixin.preStatementHook(sqlOperation);
        try {
            stm.execute(sqlOperation);
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
        StagingTable st=new StagingTable();
        mixin.postStatementHook(sqlOperation,st);
        mixin.preCommitHook();
        assertFalse(st.isEmpty());
    }

    void checkEmptyTestTable(){
        ResultSet resultSet = mixin.executeSQLRead("SELECT * FROM testtable;");
        try {
            assertFalse(resultSet.next());
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }


    void checkOneRowWithContents(String test1Val, String test2Val){
        ResultSet resultSet = mixin.executeSQLRead("SELECT * FROM testtable;");
        try {
            assertTrue(resultSet.next());
            assertEquals(test1Val, resultSet.getString("test1"));
            assertEquals(test2Val, resultSet.getString("test2"));
            assertFalse(resultSet.next());
        }
        catch(SQLException e){
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void applyTxDigest() {
        createTestTable();
        mixin.createSQLTriggers("testtable");
        final String sqlOperation = "INSERT INTO testtable (test1,test2) VALUES ('u','test');";
        Statement stm=null;
        try {
            stm = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
        mixin.preStatementHook(sqlOperation);
        try {
            stm.execute(sqlOperation);
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
        StagingTable st=new StagingTable();
        mixin.postStatementHook(sqlOperation,st);
        mixin.preCommitHook();
        assertFalse(st.isEmpty());
        cleanTestTable();
        checkEmptyTestTable();
        List<Range> ranges = new ArrayList<>();
        ranges.add(new Range("public.testtable"));
        try {
            mixin.applyTxDigest(st,ranges);
        } catch (SQLException|MDBCServiceException e) {
            e.printStackTrace();
            fail();
        }
        checkOneRowWithContents("u","test");
    }
}