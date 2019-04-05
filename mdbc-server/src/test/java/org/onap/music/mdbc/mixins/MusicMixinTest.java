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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
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
import org.onap.music.mdbc.MDBCUtils;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.StateManager;
import org.onap.music.mdbc.proto.ProtoDigest.Digest.CompleteDigest;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.MusicTxDigestId;
import org.onap.music.mdbc.tables.StagingTable;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.protobuf.InvalidProtocolBufferException;

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
    private StateManager stateManager;

    @BeforeClass
    public static void init() throws MusicServiceException {
        try {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        } catch (Exception e) {
            System.out.println(e);
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

    @AfterClass
    public static void close() throws MusicServiceException, MusicQueryException {
        //TODO: shutdown cassandra
        mixin=null;
        try {
            EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        }
        catch(NullPointerException e){
        }
    }

    @Before
    public void initTest(){
        session.execute("DROP KEYSPACE IF EXISTS "+keyspace);
        try {
            Properties properties = new Properties();
            properties.setProperty(MusicMixin.KEY_MUSIC_NAMESPACE,keyspace);
            properties.setProperty(MusicMixin.KEY_MY_ID,mdbcServerName);
            properties.setProperty(MusicMixin.KEY_COMPRESSION, Boolean.toString(true));
            mixin=new MusicMixin(stateManager, mdbcServerName,properties);
        } catch (MDBCServiceException e) {
            fail("error creating music mixin");
        }

    }

    //Own has been removed from musicMixin
//    @Test(timeout=10000)
//    public void own() {
//        Range range = new Range("TABLE1");
//        List<Range> ranges = new ArrayList<>();
//        ranges.add(range);
//    DatabasePartition partition=null;
//    try {
//      partition = TestUtils.createBasicRow(range, mixin, mdbcServerName);
//    }
//    catch(Exception e){
//      fail(e.getMessage());
//    }
//    try {
//      TestUtils.unlockRow(keyspace,mriTableName,partition);
//    } catch (MusicLockingException e) {
//      fail(e.getMessage());
//    }
//
//        DatabasePartition currentPartition = new DatabasePartition(MDBCUtils.generateTimebasedUniqueKey());
//        try {
//            mixin.own(ranges,currentPartition, MDBCUtils.generateTimebasedUniqueKey());
//        } catch (MDBCServiceException e) {
//            fail("failure when running own function");
//        }
//    }

    private DatabasePartition addRow(List<Range> ranges,boolean isLatest){
        final UUID uuid = MDBCUtils.generateTimebasedUniqueKey();
        DatabasePartition dbPartition = new DatabasePartition(ranges,uuid,null);
        MusicRangeInformationRow newRow = new MusicRangeInformationRow(uuid,dbPartition, new ArrayList<>(), "",
            mdbcServerName, isLatest);
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
        return partition;
    }

    //Own has been removed from musicMixin
//    @Test(timeout=10000)
//    public void own2() throws InterruptedException, MDBCServiceException {
//        List<Range> range12 = new ArrayList<>( Arrays.asList(
//            new Range("RANGE1"),
//            new Range("RANGE2")
//        ));
//        List<Range> range34 = new ArrayList<>( Arrays.asList(
//            new Range("RANGE3"),
//            new Range("RANGE4")
//        ));
//        List<Range> range24 = new ArrayList<>( Arrays.asList(
//            new Range("RANGE2"),
//            new Range("RANGE4")
//        ));
//        List<Range> range123 = new ArrayList<>( Arrays.asList(
//            new Range("RANGE1"),
//            new Range("RANGE2"),
//            new Range("RANGE3")
//        ));
//        DatabasePartition db1 = addRow(range12, false);
//        DatabasePartition db2 = addRow(range34, false);
//        MILLISECONDS.sleep(10);
//        DatabasePartition db3 = addRow(range12, true);
//        DatabasePartition db4 = addRow(range34, true);
//        MILLISECONDS.sleep(10);
//        DatabasePartition db5 = addRow(range24, true);
//        DatabasePartition currentPartition = new DatabasePartition(MDBCUtils.generateTimebasedUniqueKey());
//        MusicInterface.OwnershipReturn own = null;
//        try {
//            own = mixin.own(range123, currentPartition, MDBCUtils.generateTimebasedUniqueKey());
//        } catch (MDBCServiceException e) {
//            fail("failure when running own function");
//        }
//        Dag dag = own.getDag();
//
//        DagNode node4 = dag.getNode(db4.getMRIIndex());
//        assertFalse(node4.hasNotIncomingEdges());
//        List<DagNode> outgoingEdges = new ArrayList<>(node4.getOutgoingEdges());
//        assertEquals(1,outgoingEdges.size());
//
//        DagNode missing = outgoingEdges.get(0);
//        Set<Range> missingRanges = missing.getRangeSet();
//        assertEquals(2,missingRanges.size());
//        assertTrue(missingRanges.contains(new Range("RANGE1")));
//        assertTrue(missingRanges.contains(new Range("RANGE3")));
//        List<DagNode> outgoingEdges1 = missing.getOutgoingEdges();
//        assertEquals(1,outgoingEdges1.size());
//
//        DagNode finalNode = outgoingEdges1.get(0);
//        assertFalse(finalNode.hasNotIncomingEdges());
//        Set<Range> finalSet = finalNode.getRangeSet();
//        assertEquals(3,finalSet.size());
//        assertTrue(finalSet.contains(new Range("RANGE1")));
//        assertTrue(finalSet.contains(new Range("RANGE2")));
//        assertTrue(finalSet.contains(new Range("RANGE3")));
//
//        DagNode node5 = dag.getNode(db5.getMRIIndex());
//        List<DagNode> toRemoveOutEdges = node5.getOutgoingEdges();
//        assertEquals(1,toRemoveOutEdges.size());
//        toRemoveOutEdges.remove(finalNode);
//        assertEquals(0,toRemoveOutEdges.size());
//
//        MusicRangeInformationRow row = mixin.getMusicRangeInformation(own.getRangeId());
//        assertTrue(row.getIsLatest());
//        DatabasePartition dbPartition = row.getDBPartition();
//        List<Range> snapshot = dbPartition.getSnapshot();
//        assertEquals(3,snapshot.size());
//        MusicRangeInformationRow node5row = mixin.getMusicRangeInformation(node5.getId());
//        assertFalse(node5row.getIsLatest());
//        MusicRangeInformationRow node4Row = mixin.getMusicRangeInformation(db4.getMRIIndex());
//        assertFalse(node4Row.getIsLatest());
//        MusicRangeInformationRow node3Row = mixin.getMusicRangeInformation(db3.getMRIIndex());
//        assertFalse(node3Row.getIsLatest());
//    }

    @Test
    public void relinquish() {
    }

    @Test
    public void relinquishIfRequired() {
    }
    
    @Test
    public void getEveTxDigest() throws Exception {
        
        mixin.createMusicEventualTxDigest();
        ByteBuffer compressed = mockCompressedProtoByteBuff();
        MusicTxDigestId digestId = new MusicTxDigestId(UUID.randomUUID(), 1);
        mixin.addEventualTxDigest(digestId, compressed);
        
        LinkedHashMap<UUID, StagingTable> digest =  mixin.getEveTxDigest("n1");
        
        Consumer<Map.Entry<UUID,StagingTable>> consumer = new Consumer<Map.Entry<UUID,StagingTable>>() {

            @Override
            public void accept(Entry<UUID, StagingTable> mapEntry) {
                assertNotNull(mapEntry.getValue());
            }
            
        };
       
        digest.entrySet().forEach(consumer);
        
        
        
        
    }

    protected ByteBuffer mockCompressedProtoByteBuff() throws MDBCServiceException, InvalidProtocolBufferException {
        CompleteDigest instance = CompleteDigest.getDefaultInstance();
        // CompleteDigest instance  = CompleteDigest.parseFrom(ByteBuffer.wrap("Test".getBytes()));
        byte[] bytes = instance.toByteArray();
        ByteBuffer serialized = ByteBuffer.wrap(bytes);
        ByteBuffer compressed = StagingTable.Compress(serialized);
        return compressed;
    }
}
