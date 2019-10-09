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

package org.onap.music.mdbc.ownership;

import static org.junit.Assert.*;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.sql.*;
import java.util.*;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.CassaLockStore;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.MDBCUtils;
import org.onap.music.mdbc.MdbcTestUtils.DBType;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.StateManager;
import org.onap.music.mdbc.MdbcTestUtils;
import org.onap.music.mdbc.TestUtils;
import org.onap.music.mdbc.mixins.LockResult;
import org.onap.music.mdbc.mixins.MusicInterface;
import org.onap.music.mdbc.mixins.MusicInterface.OwnershipReturn;
import org.onap.music.mdbc.query.SQLOperationType;
import org.onap.music.mdbc.mixins.MusicMixin;
import org.onap.music.mdbc.mixins.MySQLMixin;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.StagingTable;
import org.onap.music.mdbc.tables.TxCommitProgress;

@Ignore
public class OwnershipAndCheckpointTest {
    public static final String DATABASE = MdbcTestUtils.mariaDBDatabaseName;
	public static final String TABLE= MdbcTestUtils.mariaDBDatabaseName+".PERSONS";
	public static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS " + TABLE + " (\n" +
            "    PersonID int,\n" +
            "    LastName varchar(255),\n" +
            "    FirstName varchar(255),\n" +
            "    Address varchar(255),\n" +
            "    City varchar(255),\n" +
            "    PRIMARY KEY (PersonID,LastName)" +
            ");";
	public static final String DROP_TABLE = "DROP TABLE IF EXISTS " + TABLE + ";";
    //Properties used to connect to music
    private static MusicMixin musicMixin = null;
	Connection conn;
	MySQLMixin mysqlMixin;
	OwnershipAndCheckpoint ownAndCheck;
	
	@Mock
	StateManager stateManager = Mockito.mock(StateManager.class);
	

    @BeforeClass
    public static void init() throws MusicServiceException, ClassNotFoundException, ManagedProcessException, MDBCServiceException {
        MdbcTestUtils.initCassandra();
        Class.forName("org.mariadb.jdbc.Driver");
		//start embedded mariadb
        MdbcTestUtils.startMariaDb();
    }

    @AfterClass
    public static void close() throws MusicServiceException, MusicQueryException, ManagedProcessException {
        //TODO: shutdown cassandra
        musicMixin=null;
        MdbcTestUtils.cleanDatabase(DBType.MySQL);
        MdbcTestUtils.stopCassandra();
    }

    private void dropTable() throws SQLException {
        final Statement dropStatement = this.conn.createStatement();
        dropStatement.execute(DROP_TABLE);
        dropStatement.close();
    }

    private void createTable() throws SQLException {
        final Statement createStatement = this.conn.createStatement();
        createStatement.execute(CREATE_TABLE);
        createStatement.close();
    }

    private void dropAndCreateTable() throws SQLException {
        mysqlMixin.dropSQLTriggers(TABLE);
        dropTable();
        createTable();
        mysqlMixin.createSQLTriggers(TABLE);
    }

    @Before
    public void initTest() throws SQLException {
        MdbcTestUtils.getSession().execute("DROP KEYSPACE IF EXISTS "+MdbcTestUtils.getKeyspace());
        try {
            Properties properties = new Properties();
/*
            properties.setProperty(MusicMixin.KEY_MY_ID,mdbcServerName);
            properties.setProperty(MusicMixin.KEY_MUSIC_NAMESPACE,keyspace);
            properties.setProperty(MusicMixin.KEY_MUSIC_RFACTOR,"1");
            //StateManager stateManager = new StateManager("dbUrl", properties, "serverName", "dbName");
            ownAndCheck = new OwnershipAndCheckpoint();
            musicMixin =new MusicMixin(stateManager, mdbcServerName,properties);
*/
            properties.setProperty(MusicMixin.KEY_MY_ID,MdbcTestUtils.getServerName());
            properties.setProperty(MusicMixin.KEY_MUSIC_NAMESPACE,MdbcTestUtils.getKeyspace());
            properties.setProperty(MusicMixin.KEY_MUSIC_RFACTOR,"1");
            properties.setProperty(MusicMixin.KEY_MUSIC_ADDRESS,MdbcTestUtils.getCassandraUrl());
            ownAndCheck = new OwnershipAndCheckpoint();
            musicMixin =new MusicMixin(stateManager, MdbcTestUtils.getServerName(), properties);
        } catch (MDBCServiceException e) {
            fail("error creating music musicMixin " + e.getMessage());
        }
        this.conn = MdbcTestUtils.getConnection(DBType.MySQL);
        Properties info = new Properties();
        this.mysqlMixin = new MySQLMixin(musicMixin, "localhost:"+MdbcTestUtils.getMariaDbPort()+"/"+DATABASE, conn, info);
        this.mysqlMixin.initTables();
        dropAndCreateTable();
    }

    private void initDatabase(Range range) throws MDBCServiceException, SQLException {
        final DatabasePartition partition = TestUtils.createBasicRow(range, musicMixin, MdbcTestUtils.getServerName());
        String sqlOperation = "INSERT INTO "+TABLE+" (PersonID,LastName,FirstName,Address,City) VALUES "+
            "(1,'SAUREZ','ENRIQUE','GATECH','ATLANTA');";
        StagingTable stagingTable = new StagingTable();
        final Statement executeStatement = this.conn.createStatement();
        executeStatement.execute(sqlOperation);
        this.conn.commit();
        mysqlMixin.postStatementHook(sqlOperation,stagingTable);
        mysqlMixin.preCommitHook();
        executeStatement.close();
        String id = MDBCUtils.generateUniqueKey().toString();
        TxCommitProgress progressKeeper = new TxCommitProgress();
        progressKeeper.createNewTransactionTracker(id ,this.conn);
        musicMixin.commitLog(partition, null, stagingTable, id, progressKeeper);
        try {
//            TestUtils.unlockRow(keyspace, mriTableName, partition);
            TestUtils.unlockRow(MdbcTestUtils.getKeyspace(), MdbcTestUtils.getMriTableName(), partition);
        }
        catch(Exception e){
            fail(e.getMessage());
        }
    }

    private OwnershipReturn cleanAndOwnPartition(Set<Range> ranges, UUID ownOpId) throws SQLException {
        dropAndCreateTable();
        cleanAlreadyApplied(ownAndCheck);
        DatabasePartition currentPartition = new DatabasePartition(MDBCUtils.generateTimebasedUniqueKey());

        OwnershipReturn own=null;
        try {
            own = ownAndCheck.own(musicMixin, ranges, currentPartition, ownOpId, SQLOperationType.WRITE);
        } catch (MDBCServiceException e) {
            fail("failure when running own function");
        }
        return own;
    }

    public void checkData() throws SQLException {
        Statement statement = this.conn.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM " + TABLE + ";");
        int counter = 0;
        while (rs.next()) {
            int personId = rs.getInt("PersonID");
            assertEquals(1,personId);
            String lastname = rs.getString("LastName");
            assertEquals("SAUREZ",lastname);
            String firstname = rs.getString("FirstName");
            assertEquals("ENRIQUE",firstname);
            String address = rs.getString("Address");
            assertEquals("GATECH",address);
            String city = rs.getString("City");
            assertEquals("ATLANTA",city);
            counter++;
        }
        assertEquals(1,counter);
    }

    @Test
    //@Ignore
    public void checkpoint() throws MDBCServiceException, SQLException {
        Range range =
            new Range(TABLE);
        Mockito.when(stateManager.getOwnAndCheck()).thenReturn(this.ownAndCheck);

        initDatabase(range);

        Set<Range> ranges = new HashSet<>();
        ranges.add(range);
        UUID ownOpId = MDBCUtils.generateTimebasedUniqueKey();
        OwnershipReturn own = cleanAndOwnPartition(ranges,ownOpId);

        Map<MusicRangeInformationRow, LockResult> locks = new HashMap<>();
        if(own.getDag()!=null) {
            locks.put(own.getDag().getNode(own.getRangeId()).getRow(),
                new LockResult(own.getRangeId(), own.getLockId(), true,
                    ranges));
            ownAndCheck.checkpoint(musicMixin, mysqlMixin, own.getDag(), ranges, ownOpId);
        }

        checkData();
    }

    @Test
    //@Ignore
    public void warmup() throws MDBCServiceException, SQLException {
        Range range = new Range(TABLE);
        Mockito.when(stateManager.getOwnAndCheck()).thenReturn(this.ownAndCheck);

        initDatabase(range);

        Set<Range> ranges = new HashSet<>();
        ranges.add(range);
        UUID ownOpId = MDBCUtils.generateTimebasedUniqueKey();
        OwnershipReturn own = cleanAndOwnPartition(ranges,ownOpId);


        Map<MusicRangeInformationRow, LockResult> locks = new HashMap<>();
        if(own.getDag()!=null) {
            locks.put(own.getDag().getNode(own.getRangeId()).getRow(),
                new LockResult(own.getRangeId(), own.getLockId(), true,
                    ranges));
        }
        ownAndCheck.warmup(musicMixin,mysqlMixin,ranges);

        checkData();
    }

    
    @Test
    public void readOwn() throws Exception {
        Range range = new Range("TABLE1");
        MusicInterface mi = MdbcTestUtils.getMusicMixin();
        Set<Range> ranges = new HashSet<>();
        ranges.add(range);
        final DatabasePartition partition = TestUtils.createBasicRow(range, mi, MdbcTestUtils.getServerName());
        TestUtils.unlockRow(MdbcTestUtils.getKeyspace(), MdbcTestUtils.getMriTableName(), partition);

        DatabasePartition currentPartition = new DatabasePartition(MDBCUtils.generateTimebasedUniqueKey());
        MusicInterface.OwnershipReturn own1, own2;
        try {
            own1 = ownAndCheck.own(mi, ranges, currentPartition,
                    MDBCUtils.generateTimebasedUniqueKey(), SQLOperationType.READ);
            // acquire the table again, should be allowed since they're both reads
            own2 = ownAndCheck.own(mi, ranges, currentPartition,
                    MDBCUtils.generateTimebasedUniqueKey(), SQLOperationType.READ);
        } catch (MDBCServiceException e) {
            fail("failure when running own function");
            return;
        }

        assertEquals(own1.getRangeId(), own2.getRangeId());
    }
    
    private void cleanAlreadyApplied(OwnershipAndCheckpoint ownAndCheck) {
        ownAndCheck.getAlreadyApplied().clear();
    }
}
