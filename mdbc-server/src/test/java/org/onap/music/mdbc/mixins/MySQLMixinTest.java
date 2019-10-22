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

package org.onap.music.mdbc.mixins;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.onap.music.mdbc.MDBCUtils;
import org.onap.music.mdbc.MdbcTestUtils;
import org.onap.music.mdbc.MdbcTestUtils.DBType;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.mixins.MySQLMixin;
import org.onap.music.mdbc.tables.MriReference;
import org.onap.music.mdbc.tables.MusicTxDigestId;
import ch.vorburger.mariadb4j.DB;

public class MySQLMixinTest {

	public static final String DATABASE = "mdbctest";
	public static final String TABLE= MdbcTestUtils.getMariaDBDBName();
	public static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS " + MdbcTestUtils.getMariaDBDBName()+ " (\n" +
            "    PersonID int,\n" +
            "    LastName varchar(255),\n" +
            "    FirstName varchar(255),\n" +
            "    Address varchar(255),\n" +
            "    City varchar(255),\n" +
            "    PRIMARY KEY (PersonID,LastName)" +
            ");";
	
	
	Connection conn;
	MySQLMixin mysqlMixin;
	
	
	
	@BeforeClass
	public static void init() throws Exception {
		Class.forName("org.mariadb.jdbc.Driver");
		MdbcTestUtils.startMariaDb();

	}
	
	@AfterClass
	public static void close() throws Exception {
		MdbcTestUtils.stopDatabase(DBType.MySQL);
	}
	
	@Before
	public void beforeTest() throws SQLException {
		this.conn = MdbcTestUtils.getConnection(DBType.MySQL);
		Properties info = new Properties();
		this.mysqlMixin = new MySQLMixin(null, null, conn, info);
	}
	

    @After
    public void afterEachTest() throws SQLException {
        clearTables();
    }
	
    @Test
    public void testGetDataBaseName() throws SQLException {
        assertEquals(MdbcTestUtils.getMariaDBDBName(), mysqlMixin.getDatabaseName());
    }
    
    @Test
    public void testGetTableSet() throws SQLException {
        Set<Range> rangesAdded = new HashSet<>();
        rangesAdded.add(new Range("TEST.RANGER"));
        rangesAdded.add(new Range("TEST.RANGES"));
        Statement st = conn.createStatement();
        for (Range r: rangesAdded) {
            st.execute("CREATE TABLE " + r + " (name VARCHAR(20));");
        }
        st.close();
        Set<Range> ranges = mysqlMixin.getSQLRangeSet();

        assertTrue(ranges.containsAll(rangesAdded));
        assertTrue(rangesAdded.containsAll(ranges));
    }
    
    @Test
    public void testCkpt() throws SQLException {
        createTables();

        Range r1 = new Range(MdbcTestUtils.mariaDBDatabaseName + ".RANGER");
        MriReference mri1 = new MriReference(MDBCUtils.generateUniqueKey());
        MusicTxDigestId i1 = new MusicTxDigestId(mri1.index, MDBCUtils.generateUniqueKey(), 1);
        Pair<MriReference, MusicTxDigestId> digestId = Pair.of(mri1, i1);
        mysqlMixin.updateCheckpointLocations(r1, digestId);
        
        Range r2 = new Range(MdbcTestUtils.mariaDBDatabaseName + ".RANGES");
        MriReference mri2 = new MriReference(MDBCUtils.generateUniqueKey());
        MusicTxDigestId i2 = new MusicTxDigestId(mri2.index, MDBCUtils.generateUniqueKey(), 2);
        Pair<MriReference, MusicTxDigestId> p2 = Pair.of(mri2, i2);
        mysqlMixin.updateCheckpointLocations(r2, p2);
        
        Map<Range, Pair<MriReference, MusicTxDigestId>> ckptmap = mysqlMixin.getCheckpointLocations();
        assertTrue(ckptmap.containsKey(r1));
        assertEquals(mri1.getIndex(), ckptmap.get(r1).getLeft().getIndex());
        assertEquals(i1.transactionId, ckptmap.get(r1).getRight().transactionId);
        
        assertTrue(ckptmap.containsKey(r2));
        assertEquals(mri2.getIndex(), ckptmap.get(r2).getLeft().getIndex());
        assertEquals(i2.transactionId, ckptmap.get(r2).getRight().transactionId);
    }

    private void createTables() throws SQLException {
        Statement st = conn.createStatement();
        st.execute("CREATE TABLE RANGER (name VARCHAR(20));");
        st.execute("CREATE TABLE RANGES (name VARCHAR(20));");
        st.close();
        //need to re-initiate the tables
        this.mysqlMixin.initTables();
    }

    
    private void clearTables() throws SQLException {
        Set<Range> ranges = mysqlMixin.getSQLRangeSet();
        Statement st = conn.createStatement();
        for (Range r: ranges) {
            try {
                st.execute("DROP TABLE " + r + ";");
            } catch (SQLException e) {
                System.out.println("Trouble dropping: " + r);
                e.printStackTrace();
            }
        }
        st.close();
    }

}
