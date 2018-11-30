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
import org.junit.Test;
import org.onap.music.datastore.CassaDataStore;
import org.onap.music.datastore.MusicLockState;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.MusicTxDigestId;

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
        CassaDataStore store = new CassaDataStore(cluster, session);
        assertNotNull("Invalid configuration for music", store);
        MusicCore.mDstoreHandle = store;
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

    @Test
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
        String fullyQualifiedMriKey = keyspace+"."+ mriTableName+"."+partition.getMusicRangeInformationIndex().toString();
        try {
            MusicLockState musicLockState = MusicCore.voluntaryReleaseLock(fullyQualifiedMriKey, partition.getLockId());
        } catch (MusicLockingException e) {
            fail("failure when releasing lock");
        }
        DatabasePartition newPartition = new DatabasePartition();
        try {
            mixin.own(ranges,newPartition);
        } catch (MDBCServiceException e) {
            fail("failure when running own function");
        }
    }

    @Test
    public void own2() {
        final UUID uuid = mixin.generateUniqueKey();
        final UUID uuid2 = mixin.generateUniqueKey();
        List<Range> ranges = new ArrayList<>();
        List<Range> ranges2 = new ArrayList<>();
        ranges.add(new Range("table1"));
        ranges2.add(new Range("table2"));
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
        String fullyQualifiedMriKey = keyspace+"."+ mriTableName+"."+partition.getMusicRangeInformationIndex().toString();
        String fullyQualifiedMriKey2 = keyspace+"."+ mriTableName+"."+partition2.getMusicRangeInformationIndex().toString();
        try {
            MusicLockState musicLockState = MusicCore.voluntaryReleaseLock(fullyQualifiedMriKey, partition.getLockId());
            MusicLockState musicLockState2 = MusicCore.voluntaryReleaseLock(fullyQualifiedMriKey2, partition2.getLockId());
        } catch (MusicLockingException e) {
            fail("failure when releasing lock");
        }
        DatabasePartition newPartition = new DatabasePartition();
        MusicInterface.OwnershipReturn ownershipReturn=null;
        try {
            List<Range> ownRanges = new ArrayList<>();
            ownRanges.add(new Range("table1"));
            ownRanges.add(new Range("table2"));
            ownershipReturn  = mixin.own(ownRanges, newPartition);
        } catch (MDBCServiceException e) {
            fail("failure when running own function");
        }
        assertEquals(2,ownershipReturn.getOldIRangeds().size());
        assertEquals(ownershipReturn.getOwnerId(),newPartition.getLockId());
        assertTrue(ownershipReturn.getOldIRangeds().get(0).equals(partition.getMusicRangeInformationIndex())||
            ownershipReturn.getOldIRangeds().get(1).equals(partition.getMusicRangeInformationIndex()));
        assertTrue(ownershipReturn.getOldIRangeds().get(0).equals(partition2.getMusicRangeInformationIndex())||
            ownershipReturn.getOldIRangeds().get(1).equals(partition2.getMusicRangeInformationIndex()));
        String finalfullyQualifiedMriKey = keyspace+"."+ mriTableName+"."+newPartition.getMusicRangeInformationIndex().toString();
        try {
            List<String> lockQueue = MusicCore.getLockingServiceHandle().getLockQueue(keyspace, mriTableName,
                newPartition.getMusicRangeInformationIndex().toString());
            assertEquals(1,lockQueue.size());
            assertEquals(lockQueue.get(0),newPartition.getLockId());
        } catch (MusicServiceException|MusicQueryException|MusicLockingException e) {
            fail("failure on getting queue");
        }
        MusicRangeInformationRow musicRangeInformation=null;
        try {
             musicRangeInformation= mixin.getMusicRangeInformation(newPartition.getMusicRangeInformationIndex());
        } catch (MDBCServiceException e) {
            fail("fail to retrieve row");
        }
        assertEquals(2,musicRangeInformation.getDBPartition().getSnapshot().size());
        assertEquals(0,musicRangeInformation.getRedoLog().size());
        assertEquals(newPartition.getLockId(),musicRangeInformation.getOwnerId());
        assertEquals(mdbcServerName,musicRangeInformation.getMetricProcessId());
        List<Range> snapshot = musicRangeInformation.getDBPartition().getSnapshot();
        boolean containsTable1=false;
        Range table1Range = new Range("table1");
        for(Range r:snapshot){
            if(r.overlaps(table1Range)){
                containsTable1=true;
                break;
            }
        }
        assertTrue(containsTable1);
        boolean containsTable2=false;
        Range table2Range = new Range("table2");
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
