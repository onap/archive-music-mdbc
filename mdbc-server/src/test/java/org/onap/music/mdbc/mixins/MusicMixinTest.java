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

import static org.junit.Assert.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.CassaLockStore;
import org.onap.music.lockingservice.cassandra.MusicLockState;
import org.onap.music.main.MusicCore;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.MusicTxDigestId;
import org.onap.music.service.impl.MusicCassaCore;

public class MusicMixinTest {
	
    final private static String keyspace="metricmusictest";
    final private static String mriTableName = "musicrangeinformation";
    final private static String mtdTableName = "musictxdigest";
    final private static String mdbcServerName = "name";

    //Properties used to connect to music
    private static Cluster cluster;
    private static Session session;
    private static String cassaHost = "localhost";
    private static MusicMixin mixin = null;

    @BeforeClass
    public static void init() throws MusicServiceException {
        try {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        } catch (Exception e) {
            System.out.println(e);
        }

        cluster = new Cluster.Builder().addContactPoint(cassaHost).withPort(9142).build();
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(20000);
        assertNotNull("Invalid configuration for cassandra", cluster);
        session = cluster.connect();
        assertNotNull("Invalid configuration for cassandra", session);

        MusicDataStoreHandle.mDstoreHandle = new MusicDataStore(cluster, session);
        CassaLockStore store = new CassaLockStore(MusicDataStoreHandle.mDstoreHandle);
        assertNotNull("Invalid configuration for music", store);
        try {
            Properties properties = new Properties();
            properties.setProperty(MusicMixin.KEY_MUSIC_NAMESPACE,keyspace);
            properties.setProperty(MusicMixin.KEY_MY_ID,mdbcServerName);
            mixin=new MusicMixin(mdbcServerName,properties);
        } catch (MDBCServiceException e) {
            fail("error creating music mixin");
        }
    }

    @AfterClass
    public static void close() throws MusicServiceException, MusicQueryException {
        //TODO: shutdown cassandra
        session.close();
        cluster.close();
    }

    @Test(timeout=1000)
    public void own() {
        final UUID uuid = mixin.generateUniqueKey();
        List<Range> ranges = new ArrayList<>();
        ranges.add(new Range("table1"));
        DatabasePartition dbPartition = new DatabasePartition(ranges,uuid,null);
        MusicRangeInformationRow newRow = new MusicRangeInformationRow(dbPartition, new ArrayList<>(), "", mdbcServerName);
        DatabasePartition partition=null;
        try {
            partition = mixin.createMusicRangeInformation(newRow);
        } catch (MDBCServiceException e) {
            fail("failure when creating new row");
        }
        String fullyQualifiedMriKey = keyspace+"."+ mriTableName+"."+partition.getMRIIndex().toString();
        try {
            MusicLockState musicLockState = MusicCore.voluntaryReleaseLock(fullyQualifiedMriKey, partition.getLockId());
        } catch (MusicLockingException e) {
            fail("failure when releasing lock");
        }
        DatabasePartition newPartition = new DatabasePartition(mixin.generateUniqueKey());
        try {
            mixin.own(ranges,newPartition);
        } catch (MDBCServiceException e) {
            fail("failure when running own function");
        }
    }

    @Test(timeout=1000)
    @Ignore //TODO: Fix this. it is breaking because of previous test^
    public void own2() {
        final UUID uuid = mixin.generateUniqueKey();
        final UUID uuid2 = mixin.generateUniqueKey();
        List<Range> ranges = new ArrayList<>();
        List<Range> ranges2 = new ArrayList<>();
        ranges.add(new Range("table2"));
        ranges2.add(new Range("table3"));
        DatabasePartition dbPartition = new DatabasePartition(ranges,uuid,null);
        DatabasePartition dbPartition2 = new DatabasePartition(ranges2,uuid2,null);
        MusicRangeInformationRow newRow = new MusicRangeInformationRow(dbPartition, new ArrayList<>(), "", mdbcServerName);
        MusicRangeInformationRow newRow2 = new MusicRangeInformationRow(dbPartition2, new ArrayList<>(), "", mdbcServerName);
        DatabasePartition partition=null;
        DatabasePartition partition2=null;
        try {
            partition = mixin.createMusicRangeInformation(newRow);
            partition2 = mixin.createMusicRangeInformation(newRow2);
        } catch (MDBCServiceException e) {
            fail("failure when creating new row");
        }
        String fullyQualifiedMriKey = keyspace+"."+ mriTableName+"."+partition.getMRIIndex().toString();
        String fullyQualifiedMriKey2 = keyspace+"."+ mriTableName+"."+partition2.getMRIIndex().toString();
        try {
            MusicLockState musicLockState = MusicCore.voluntaryReleaseLock(fullyQualifiedMriKey, partition.getLockId());
            MusicLockState musicLockState2 = MusicCore.voluntaryReleaseLock(fullyQualifiedMriKey2, partition2.getLockId());
        } catch (MusicLockingException e) {
            fail("failure when releasing lock");
        }
        DatabasePartition blankPartition = new DatabasePartition(mixin.generateUniqueKey());
        DatabasePartition newPartition=null;
        try {
            List<Range> ownRanges = new ArrayList<>();
            ownRanges.add(new Range("table2"));
            ownRanges.add(new Range("table3"));
            newPartition = mixin.own(ownRanges, blankPartition);
        } catch (MDBCServiceException e) {
            fail("failure when running own function");
        }
        assertEquals(2,newPartition.getOldMRIIds().size());
        assertEquals(newPartition.getLockId(),blankPartition.getLockId());
        assertTrue(newPartition.getOldMRIIds().get(0).equals(partition.getMRIIndex())||
            newPartition.getOldMRIIds().get(1).equals(partition.getMRIIndex()));
        assertTrue(newPartition.getOldMRIIds().get(0).equals(partition2.getMRIIndex())||
            newPartition.getOldMRIIds().get(1).equals(partition2.getMRIIndex()));
        String finalfullyQualifiedMriKey = keyspace+"."+ mriTableName+"."+blankPartition.getMRIIndex().toString();
        try {
            List<String> lockQueue = MusicCassaCore.getLockingServiceHandle().getLockQueue(keyspace, mriTableName,
                blankPartition.getMRIIndex().toString());
            assertEquals(1,lockQueue.size());
            assertEquals(lockQueue.get(0),blankPartition.getLockId());
        } catch (MusicServiceException|MusicQueryException|MusicLockingException e) {
            fail("failure on getting queue");
        }
        MusicRangeInformationRow musicRangeInformation=null;
        try {
             musicRangeInformation= mixin.getMusicRangeInformation(blankPartition.getMRIIndex());
        } catch (MDBCServiceException e) {
            fail("fail to retrieve row");
        }
        assertEquals(2,musicRangeInformation.getDBPartition().getSnapshot().size());
        assertEquals(0,musicRangeInformation.getRedoLog().size());
        assertEquals(blankPartition.getLockId(),musicRangeInformation.getOwnerId());
        assertEquals(mdbcServerName,musicRangeInformation.getMetricProcessId());
        List<Range> snapshot = musicRangeInformation.getDBPartition().getSnapshot();
        boolean containsTable1=false;
        Range table1Range = new Range("table2");
        for(Range r:snapshot){
            if(r.overlaps(table1Range)){
                containsTable1=true;
                break;
            }
        }
        assertTrue(containsTable1);
        boolean containsTable2=false;
        Range table2Range = new Range("table3");
        for(Range r:snapshot){
            if(r.overlaps(table2Range)){
                containsTable2=true;
                break;
            }
        }
        assertTrue(containsTable2);
    }

    @Test
    public void relinquish() {
    }

    @Test
    public void relinquishIfRequired() {
    }
}
