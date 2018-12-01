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
import org.junit.Test;
import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.CassaLockStore;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.MDBCUtils;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.TestUtils;
import org.onap.music.mdbc.mixins.LockResult;
import org.onap.music.mdbc.mixins.MusicInterface.OwnershipReturn;
import org.onap.music.mdbc.mixins.MusicMixin;
import org.onap.music.mdbc.mixins.MySQLMixin;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.StagingTable;
import org.onap.music.mdbc.tables.TxCommitProgress;

public class OwnershipAndCheckpointTest {
    final private static String keyspace="metricmusictest";
    final private static String mriTableName = "musicrangeinformation";
    final private static String mtdTableName = "musictxdigest";
    final private static String mdbcServerName = "name";
    public static final String DATABASE = "mdbcTest";
	public static final String TABLE= "Persons";
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
    private static Cluster cluster;
    private static Session session;
    private static String cassaHost = "localhost";
    private static MusicMixin musicMixin = null;
    private static DB db;
	Connection conn;
	MySQLMixin mysqlMixin;

    @BeforeClass
    public static void init() throws MusicServiceException, ClassNotFoundException, ManagedProcessException {
        try {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        } catch (Exception e) {
            fail(e.getMessage());
        }
        cluster = new Cluster.Builder().addContactPoint(cassaHost).withPort(9142).build();
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(20000);
        assertNotNull("Invalid configuration for cassandra", cluster);
        session = cluster.connect();
        assertNotNull("Invalid configuration for cassandra", session);
        Class.forName("org.mariadb.jdbc.Driver");
        MusicDataStoreHandle.mDstoreHandle = new MusicDataStore(cluster, session);
        CassaLockStore store = new CassaLockStore(MusicDataStoreHandle.mDstoreHandle);
        assertNotNull("Invalid configuration for music", store);
		//start embedded mariadb
		db = DB.newEmbeddedDB(13306);
		db.start();
		db.createDB(DATABASE);
    }

    @AfterClass
    public static void close() throws MusicServiceException, MusicQueryException, ManagedProcessException {
        //TODO: shutdown cassandra
        session.close();
        cluster.close();
        db.stop();
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
        session.execute("DROP KEYSPACE IF EXISTS "+keyspace);
        try {
            Properties properties = new Properties();
            properties.setProperty(MusicMixin.KEY_MY_ID,mdbcServerName);
            properties.setProperty(MusicMixin.KEY_MUSIC_NAMESPACE,keyspace);
            musicMixin =new MusicMixin(mdbcServerName,properties);
        } catch (MDBCServiceException e) {
            fail("error creating music musicMixin");
        }
        this.conn = DriverManager.getConnection("jdbc:mariadb://localhost:13306/"+DATABASE, "root", "");
        this.mysqlMixin = new MySQLMixin(musicMixin, "localhost:13306/"+DATABASE, conn, null);
        dropAndCreateTable();
    }

    private void initDatabase(Range range) throws MDBCServiceException, SQLException {
        final DatabasePartition partition = TestUtils.createBasicRow(range, musicMixin, mdbcServerName);
        String sqlOperation = "INSERT INTO "+TABLE+" (PersonID,LastName,FirstName,Address,City) VALUES "+
            "(1,'SAUREZ','ENRIQUE','GATECH','ATLANTA');";
        HashMap<Range, StagingTable> stagingTable = new HashMap<>();
        final Statement executeStatement = this.conn.createStatement();
        executeStatement.execute(sqlOperation);
        this.conn.commit();
        mysqlMixin.postStatementHook(sqlOperation,stagingTable);
        executeStatement.close();
        String id = MDBCUtils.generateUniqueKey().toString();
        TxCommitProgress progressKeeper = new TxCommitProgress();
        progressKeeper.createNewTransactionTracker(id ,this.conn);
        musicMixin.commitLog(partition, stagingTable, id, progressKeeper);
        TestUtils.unlockRow(keyspace,mriTableName,partition);
    }

    private OwnershipReturn cleanAndOwnPartition(List<Range> ranges, UUID ownOpId) throws SQLException {
        dropAndCreateTable();
        DatabasePartition currentPartition = new DatabasePartition(MDBCUtils.generateTimebasedUniqueKey());

        OwnershipReturn own=null;
        try {
            own = musicMixin.own(ranges, currentPartition, ownOpId);
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
    public void checkpoint() throws MDBCServiceException, SQLException {
        Range range = new Range(TABLE);
        OwnershipAndCheckpoint ownAndCheck = musicMixin.getOwnAndCheck();
        initDatabase(range);

        List<Range> ranges = new ArrayList<>();
        ranges.add(range);
        UUID ownOpId = MDBCUtils.generateTimebasedUniqueKey();
        OwnershipReturn own = cleanAndOwnPartition(ranges,ownOpId);

        Map<MusicRangeInformationRow, LockResult> locks = new HashMap<>();
        locks.put(own.getDag().getNode(own.getRangeId()).getRow(),new LockResult(own.getRangeId(),own.getOwnerId(),true,
            ranges));
        ownAndCheck.checkpoint(musicMixin,mysqlMixin,own.getDag(),ranges,locks, ownOpId);

        checkData();
    }

    @Test
    public void warmup() throws MDBCServiceException, SQLException {
        Range range = new Range(TABLE);
        OwnershipAndCheckpoint ownAndCheck = musicMixin.getOwnAndCheck();
        initDatabase(range);

        List<Range> ranges = new ArrayList<>();
        ranges.add(range);
        UUID ownOpId = MDBCUtils.generateTimebasedUniqueKey();
        OwnershipReturn own = cleanAndOwnPartition(ranges,ownOpId);

        Map<MusicRangeInformationRow, LockResult> locks = new HashMap<>();
        locks.put(own.getDag().getNode(own.getRangeId()).getRow(),new LockResult(own.getRangeId(),own.getOwnerId(),true,
            ranges));
        ownAndCheck.warmup(musicMixin,mysqlMixin,ranges);

        checkData();
    }
}