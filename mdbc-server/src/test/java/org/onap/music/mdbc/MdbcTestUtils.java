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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.lockingservice.cassandra.CassaLockStore;
import org.onap.music.mdbc.mixins.MusicMixin;
import org.onap.music.mdbc.mixins.PostgresMixin;

public class MdbcTestUtils {

    // Postgres variables
    static EmbeddedPostgres pg=null;
    static DataSource postgresDatabase=null;
    final private static int postgresPort = 13307;
    @Rule
    public static TemporaryFolder tf = new TemporaryFolder();

    // Cassandra variables
    //Properties used to connect to music
    private static Cluster cluster;
    private static Session session;

    //Mdbc variables
    final private static String keyspace="metricmusictest";
    final private static String mdbcServerName = "name";
    final private static String mtdTableName = "musictxdigest";
    final private static String eventualMtxdTableName = "musicevetxdigest";
    final private static String mriTableName = "musicrangeinformation";
    final private static String rangeDependencyTableName = "musicrangedependency";
    final private static String nodeInfoTableName = "nodeinfo";
    //Mariadb variables
    static DB db=null;
    final public static String mariaDBDatabaseName="test";
    final static Integer mariaDbPort=13306;



    public enum DBType {POSTGRES, MySQL}

    public static String getCassandraUrl(){
        return cluster.getMetadata().getAllHosts().iterator().next().getAddress().toString();

    }

    public static String getKeyspace(){
        return keyspace;
    }

    public static String getServerName(){
        return mdbcServerName;
    }

    public static String getMriTableName(){
        return mriTableName;
    }

    public static String getMariaDbPort() {
        return mariaDbPort.toString();
    }

    public static String getMariaDBDBName(){
        return mariaDBDatabaseName;
    }

    static Connection getPostgreConnection() {
        startPostgres();
        Connection conn=null;
        try
        {
            conn = postgresDatabase.getConnection();
        } catch(SQLException e){
            e.printStackTrace();
            fail();
        }
        return conn;
    }

    static synchronized public void startPostgres(){
        if(pg==null) {
            try {
                tf.create();
                pg = EmbeddedPostgres.builder().setPort(postgresPort).setDataDirectory(tf.newFolder("tmp")+"/data-dir").start();
            } catch (IOException e) {
                e.printStackTrace();
                fail();
            }
        }
        if(postgresDatabase==null) {
            postgresDatabase = pg.getPostgresDatabase();
        }
    }

    static public String getPostgresUrl(){
        return getPostgresUrlWithoutDb()+"/postgres";
    }

    static public String getPostgresUrlWithoutDb(){
        return "jdbc:postgresql://localhost:"+Integer.toString(postgresPort);
    }

    synchronized static Connection getMariadbConnection(){
        startMariaDb();
        Connection conn = null;
        try {
            conn = DriverManager
                .getConnection(getMariadbUrlWithoutDatabase()+"/"+mariaDBDatabaseName, "root", "");
        } catch (SQLException e) {
            e.printStackTrace();
            fail("Error creating mdbc connection");
        }
        return conn;
    }

    public synchronized static void startMariaDb(){
        if (db == null) {
            try {
                db=DB.newEmbeddedDB(mariaDbPort);
                db.start();
                db.createDB(mariaDBDatabaseName);
            } catch (ManagedProcessException e) {
                e.printStackTrace();
                fail("error initializing embedded mariadb");
            }
        }
    }

    static String getMariadbUrlWithoutDatabase(){
        return  "jdbc:mariadb://localhost:"+Integer.toString(mariaDbPort);
    }

    public static Connection getConnection(DBType type){
        switch(type){
            case MySQL:
                return getMariadbConnection();
            case POSTGRES:
                return getPostgreConnection();
            default:
                fail("Wrong type for creating connection");
        }
        return null;
    }

    synchronized static void stopPostgres(){
        postgresDatabase=null;
        if(pg!=null) {
            try {
                pg.close();
                pg=null;
            } catch (IOException e) {
                e.printStackTrace();
                fail("Error closing postgres database");
            }
        }
        if(tf!=null){
            tf.delete();
        }
    }

    private static void stopMySql(){
        try {
            db.stop();
            db = null;
        } catch (ManagedProcessException e) {
            e.printStackTrace();
            fail("Error closing mysql");
        }
    }

    public static void stopDatabase(DBType type){
        switch(type) {
            case MySQL:
                stopMySql();
                break;
            case POSTGRES:
                stopPostgres();
                break;
            default:
                fail("Wrong type for creating connection");
        }
    }

    public static void initCassandra(){
        try {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);
        } catch (Exception e) {
            System.out.println(e);
            fail("Error starting embedded cassandra");
        }
        cluster=EmbeddedCassandraServerHelper.getCluster();
        //cluster = new Cluster.Builder().addContactPoint(cassaHost).withPort(9142).build();
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(20000);
        assertNotNull("Invalid configuration for cassandra", cluster);
        session = EmbeddedCassandraServerHelper.getSession();
        assertNotNull("Invalid configuration for cassandra", session);

        MusicDataStoreHandle.mDstoreHandle = new MusicDataStore(cluster, session);
        CassaLockStore store = new CassaLockStore(MusicDataStoreHandle.mDstoreHandle);
        assertNotNull("Invalid configuration for music", store);
    }

    public static void stopCassandra(){
        try {
            EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        }
        catch(NullPointerException e){
        }
    }

    public static Session getSession(){
       return session;
    }

    public static MusicMixin getMusicMixin() throws MDBCServiceException {
        initNamespaces();
        initTables();
        MusicMixin mixin=null;
        try {
            Properties properties = new Properties();
            properties.setProperty(MusicMixin.KEY_MY_ID,MdbcTestUtils.getServerName());
            properties.setProperty(MusicMixin.KEY_MUSIC_NAMESPACE,MdbcTestUtils.getKeyspace());
            properties.setProperty(MusicMixin.KEY_MUSIC_RFACTOR,"1");
            properties.setProperty(MusicMixin.KEY_MUSIC_ADDRESS,MdbcTestUtils.getCassandraUrl());
            mixin =new MusicMixin(null, MdbcTestUtils.getServerName(),properties);
        } catch (MDBCServiceException e) {
            fail("error creating music mixin");
        }
        return mixin;
    }

    public static void initNamespaces() throws MDBCServiceException{
        MusicMixin.createKeyspace("music_internal",1);
        MusicMixin.createKeyspace(keyspace,1);
    }

    public static void initTables() throws MDBCServiceException{
        MusicMixin.createMusicRangeInformationTable(keyspace, mriTableName);
        MusicMixin.createMusicTxDigest(mtdTableName,keyspace, -1);
        MusicMixin.createMusicEventualTxDigest(eventualMtxdTableName,keyspace, -1);
        MusicMixin.createMusicNodeInfoTable(nodeInfoTableName,keyspace,-1);
        MusicMixin.createMusicRangeDependencyTable(keyspace,rangeDependencyTableName);
    }

}
