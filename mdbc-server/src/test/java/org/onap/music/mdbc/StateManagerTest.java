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

package org.onap.music.mdbc;

import static org.junit.Assert.*;

import ch.vorburger.exec.ManagedProcessException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.mdbc.MdbcTestUtils.DBType;
import org.onap.music.mdbc.mixins.MusicMixin;

public class StateManagerTest {
    final private static String tableName = "testtable";
    final private static String eventualTableName = "eventual";
    private static MusicMixin musicMixin = null;
    private static StateManager manager;

    @BeforeClass
    public static void init() throws ClassNotFoundException, MDBCServiceException {
        MdbcTestUtils.initCassandra();
        MdbcTestUtils.initNamespaces();
        Class.forName("org.postgresql.Driver");
		//start embedded mariadb
        MdbcTestUtils.startPostgres();
    }

    @AfterClass
    public static void close() {
        //TODO: shutdown cassandra
        musicMixin=null;
        MdbcTestUtils.cleanDatabase(DBType.POSTGRES);
        MdbcTestUtils.stopCassandra();
    }

    public static boolean cleanTable(Connection testConnection, String table) {
        Boolean execute = null;
        final String cleanCmd = "DELETE FROM "+table+";";
        try {
            Statement stmt = testConnection.createStatement();
            execute = stmt.execute(cleanCmd);
            testConnection.commit();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        return execute;
    }

    private void dropTable(Connection connection, String table) throws SQLException {
        final Statement statement = connection.createStatement();
        statement.execute("DROP TABLE IF EXISTS "+table+";");
        statement.close();
        connection.commit();
    }

    private void createTable(Connection connection,String table) throws SQLException {
        final Statement statement2 = connection.createStatement();
        statement2.execute("CREATE TABLE IF NOT EXISTS "+table+" (id serial,"
            + "    description TEXT,"
            + "    PRIMARY KEY (id));");
        statement2.close();
        connection.commit();

    }


    private int insertRows(Connection connection, int numOfRows, String table) throws SQLException {
        final StringBuilder insertSQLBuilder = new StringBuilder()
            .append("INSERT INTO "+table+"(description) VALUES (");
        String begin= "";
        for (int i = 0; i < numOfRows; i++) {
            insertSQLBuilder.append(begin).append("'First-")
                .append(i)
                .append("')");
            begin=",(";
        }
        insertSQLBuilder.append(";");
        int execute=-1;
        try {
            Statement stmt = connection.createStatement();
            execute = stmt.executeUpdate(insertSQLBuilder.toString());
            connection.commit();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return execute;
    }

    @Before
    public void initTest() throws SQLException {
        MdbcTestUtils.getSession().execute("DROP KEYSPACE IF EXISTS "+MdbcTestUtils.getKeyspace());
        Properties connectionProps = new Properties();
        connectionProps.put("user", "postgres");
        connectionProps.put("password", "postgres");
        connectionProps.put(Configuration.KEY_MUSIC_MIXIN_NAME,"cassandra2");
        connectionProps.put(Configuration.KEY_DB_MIXIN_NAME,"postgres");
        connectionProps.setProperty(MusicMixin.KEY_MY_ID,MdbcTestUtils.getServerName());
        connectionProps.setProperty(MusicMixin.KEY_MUSIC_NAMESPACE,MdbcTestUtils.getKeyspace());
        connectionProps.setProperty(MusicMixin.KEY_MUSIC_RFACTOR,"1");
        connectionProps.setProperty(MusicMixin.KEY_MUSIC_ADDRESS,MdbcTestUtils.getCassandraUrl());
        Utils.registerDefaultDrivers();
        try {
            manager = new StateManager(MdbcTestUtils.getPostgresUrlWithoutDb(),connectionProps,MdbcTestUtils.getServerName(),
                "postgres");
        } catch (MDBCServiceException e) {
            e.printStackTrace();
            fail("Error initializing state manager");
        }
    }


    @Test
    public void getConnection() throws SQLException {
        final Connection connection = manager.getConnection("0");
        connection.setAutoCommit(false);
        dropTable(connection,tableName);
        createTable(connection,tableName);
        insertRows(connection,10,tableName);
        cleanTable(connection,tableName);
        insertRows(connection,30,tableName);
        Statement stmt = connection.createStatement();
        ResultSet result = stmt.executeQuery("SELECT * FROM "+tableName+";");
        int counter = 0;
        while(result.next()){
            String colVal = result.getString("description");
            assertEquals(colVal,"First-"+Integer.toString(counter));
            counter++;
        }
        connection.commit();
        stmt.close();
    }

    @Test
    public void eventual() throws SQLException {
        List<Range> eventual = new ArrayList<>();
        eventual.add(new Range("public."+eventualTableName));
        manager.setEventualRanges(eventual);
        final Connection connection = manager.getConnection("eventual");
        connection.setAutoCommit(false);
        dropTable(connection,eventualTableName);
        createTable(connection,eventualTableName);
        insertRows(connection,10,eventualTableName);
        cleanTable(connection,eventualTableName);
        insertRows(connection,30,eventualTableName);
        Statement stmt = connection.createStatement();
        ResultSet result = stmt.executeQuery("SELECT * FROM "+eventualTableName+";");
        int counter = 0;
        while(result.next()){
            String colVal = result.getString("description");
            assertEquals(colVal,"First-"+Integer.toString(counter));
            counter++;
        }
        assertEquals(30,counter);
        connection.commit();
        stmt.close();
    }
}