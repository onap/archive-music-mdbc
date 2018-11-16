///*
// * ============LICENSE_START====================================================
// * org.onap.music.mdbc
// * =============================================================================
// * Copyright (C) 2018 AT&T Intellectual Property. All rights reserved.
// * =============================================================================
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// * 
// *      http://www.apache.org/licenses/LICENSE-2.0
// * 
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// * ============LICENSE_END======================================================
// */
//package org.onap.music.mdbc;
//
//import com.datastax.driver.core.*;
//import com.datastax.driver.core.exceptions.QueryExecutionException;
//import com.datastax.driver.core.exceptions.SyntaxError;
//import org.apache.commons.lang3.tuple.Pair;
//import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
//import org.json.JSONObject;
//import org.junit.After;
//import org.junit.AfterClass;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import org.junit.Ignore;
//import org.onap.music.datastore.CassaDataStore;
//import org.onap.music.datastore.PreparedQueryObject;
//import org.onap.music.exceptions.MDBCServiceException;
//import org.onap.music.exceptions.MusicLockingException;
//import org.onap.music.exceptions.MusicQueryException;
//import org.onap.music.exceptions.MusicServiceException;
//import org.onap.music.main.MusicCore;
//import org.onap.music.main.MusicUtil;
//import org.onap.music.main.ResultType;
//import org.onap.music.main.ReturnType;
//import org.onap.music.mdbc.mixins.CassandraMixin;
//import org.onap.music.mdbc.tables.*;
//
//
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.*;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.locks.Condition;
//import java.util.concurrent.locks.Lock;
//import java.util.concurrent.locks.ReentrantLock;
//
//import static org.junit.Assert.*;
//
//@Ignore
//public class DatabaseOperationsTest {
//
//    final private String keyspace="metricmusictest";
//    final private String mriTableName = "musicrangeinformation";
//    final private String mtdTableName = "musictxdigest";
//
//    //Properties used to connect to music
//    private static Cluster cluster;
//    private static Session session;
//    private static String cassaHost = "localhost";
//    
//    @BeforeClass
//    public static void init() throws MusicServiceException {
//    	try {
//    		EmbeddedCassandraServerHelper.startEmbeddedCassandra();
//    	} catch (Exception e) {
//    		System.out.println(e);
//    	}
//        
//    	cluster = new Cluster.Builder().addContactPoint(cassaHost).withPort(9142).build();
//        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(20000);
//        session = cluster.connect();
//        
//        assertNotNull("Invalid configuration for cassandra", cluster);
//        session = cluster.connect();
//        assertNotNull("Invalid configuration for cassandra", session);
////        TestUtils.populateMusicUtilsWithProperties(prop);
//        CassaDataStore store = new CassaDataStore(cluster, session);
//        assertNotNull("Invalid configuration for music", store);
//        MusicCore.mDstoreHandle = store;
//
//    }
//    
//    @AfterClass
//    public static void close() throws MusicServiceException, MusicQueryException {
// 
//        //TODO: shutdown cassandra
//
//    }
//    
//    @Before
//    public void setUp() throws Exception {
//        //		System.out.println("TEST 1: Getting ready for testing connection to Cassandra");
//        //Create keyspace
//
//    	
//        createKeyspace();
//        useKeyspace();
//    }
//
//    @After
//    public void tearDown() {
//        deleteKeyspace();
//    }
//    
//    private void createKeyspace() {
//        String queryOp = "CREATE KEYSPACE " +
//                keyspace +
//                " WITH REPLICATION " +
//                "= {'class':'SimpleStrategy', 'replication_factor':1}; ";
//        ResultSet res=null;
//        try {
//            res = session.execute(queryOp);
//        }
//        catch(QueryExecutionException e){
//            fail("Failure executing creation of keyspace with error: " + e.getMessage());
//        } catch(SyntaxError e){
//            fail("Failure executing creation of keyspace with syntax error: " + e.getMessage());
//        }
//        assertTrue("Keyspace "+keyspace+" is already being used, please change it to avoid loosing data",res.wasApplied());
//    }
//
//    private void useKeyspace(){
//        String queryBuilder = "USE " +
//                keyspace +
//                "; ";
//        ResultSet res = session.execute(queryBuilder);
//        assertTrue("Keyspace "+keyspace+" is already being used, please change it to avoid loosing data",res.wasApplied());
//    }
//
//    private void deleteKeyspace(){
//        String queryBuilder = "DROP KEYSPACE " +
//                keyspace +
//                ";";
//        ResultSet res = session.execute(queryBuilder);
//        assertTrue("Keyspace "+keyspace+" doesn't exist and it should",res.wasApplied());
//    }
//
//    private void CreateMTD(){
//        try {
//            MusicTxDigest.createMusicTxDigest(keyspace, mtdTableName);
//        } catch (MDBCServiceException e) {
//            fail("Execution of creating music tx digest failed");
//        }
//    }
//
//    @Test
//    public void createMusicTxDigest() {
//        HashSet<String> expectedColumns = new HashSet<>(
//                Arrays.asList("txid","transactiondigest")
//        );
//        HashMap<String,DataType> expectedTypes = new HashMap<>();
//        expectedTypes.put("txid",DataType.uuid());
//        expectedTypes.put("transactiondigest",DataType.text());
//        CreateMTD();
//        //check structure of table
//        CassaDataStore ds=null;
//        try {
//            ds = MusicCore.getDSHandle();
//        } catch (MusicServiceException e) {
//            fail("Getting DS handle fail with error " + e.getErrorMessage());
//        }
//        TableMetadata table = ds.returnColumnMetadata(keyspace,mtdTableName);
//        assertNotNull("Error obtaining metadata of table, there may be an error with its creation", table);
//        List<ColumnMetadata> columnsMeta = table.getColumns();
//        checkDataTypeForTable(columnsMeta,expectedColumns,expectedTypes);
//    }
//
//    @Test
//    public void createMusicRangeInformationTable() {
//        HashSet<String> expectedColumns = new HashSet<>(
//                Arrays.asList("rangeid","keys","txredolog","ownerid","metricprocessid")
//        );
//        HashMap<String,DataType> expectedTypes = new HashMap<>();
//        expectedTypes.put("rangeid",DataType.uuid());
//        expectedTypes.put("keys",DataType.set(DataType.text()));
//        ProtocolVersion currentVer =  cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
//        assertNotNull("Protocol version for cluster is invalid", currentVer);
//        CodecRegistry registry = cluster.getConfiguration().getCodecRegistry();
//        assertNotNull("Codec registry for cluster is invalid", registry);
//        expectedTypes.put("txredolog",DataType.list(TupleType.of(currentVer,registry,DataType.text(),DataType.uuid())));
//        expectedTypes.put("ownerid",DataType.text());
//        expectedTypes.put("metricprocessid",DataType.text());
//        try {
//            DatabaseOperations.createMusicRangeInformationTable(keyspace,mriTableName);
//        } catch (MDBCServiceException e) {
//            fail("Execution of creating music tx digest failed");
//        }
//        //check structure of table
//        CassaDataStore ds=null;
//        try {
//            ds = MusicCore.getDSHandle();
//        } catch (MusicServiceException e) {
//            fail("Getting DS handle fail with error " + e.getErrorMessage());
//        }
//        TableMetadata table = ds.returnColumnMetadata(keyspace,mriTableName);
//        assertNotNull("Error obtaining metadata of table, there may be an error with its creation", table);
//        List<ColumnMetadata> columnsMeta = table.getColumns();
//        checkDataTypeForTable(columnsMeta,expectedColumns,expectedTypes);
//    }
//
//    private void checkDataTypeForTable(List<ColumnMetadata> columnsMeta, HashSet<String> expectedColumns,
//                               HashMap<String,DataType> expectedTypes){
//        for(ColumnMetadata cMeta : columnsMeta){
//            String columnName = cMeta.getName();
//            DataType type = cMeta.getType();
//            assertTrue("Invalid column name: "+columnName,expectedColumns.contains(columnName));
//            assertTrue("Fix the contents of expectedtypes for column: "+columnName,
//                    expectedTypes.containsKey(columnName));
//            assertEquals("Invalid type for column: "+columnName,
//                    expectedTypes.get(columnName),type);
//        }
//    }
//
//    private void createMRI(){
//        try {
//            DatabaseOperations.createMusicRangeInformationTable(keyspace,mriTableName);
//        } catch (MDBCServiceException e) {
//            fail("Execution of creating music tx digest failed");
//        }
//    }
//
//    @Test
//    public void createEmptyMriRow() {
//        //Assume mri creation is working
//        createMRI();
//        List<Range> ranges = new ArrayList<>();
//        ranges.add(new Range("table1"));
//        ranges.add(new Range("table2"));
//        final String lockId = null;
//        String processId = "tcp://test:1234";
//        UUID newRowId=null;
//        try {
//            newRowId = DatabaseOperations.createEmptyMriRow(keyspace,mriTableName,processId,
//                    lockId, ranges);
//        } catch (MDBCServiceException e) {
//            fail("Adding a new empty mri row failed");
//        }
//        getRowFromMriAndCompare(newRowId,ranges,lockId,processId);
//    }
//
//    private String getLock(String table, MriReference mriIndex){
//        String fullyQualifiedMriKey = keyspace+"."+ mriIndex.table+"."+mriIndex.index.toString();
//        String lockId;
//        lockId = MusicCore.createLockReference(fullyQualifiedMriKey);
//        //\TODO Handle better failures to acquire locks
//        ReturnType lockReturn=null;
//        try {
//            lockReturn = MusicCore.acquireLock(fullyQualifiedMriKey,lockId);
//        } catch (MusicLockingException | MusicServiceException | MusicQueryException e) {
//            fail(e.getMessage());
//        }
//        assertEquals(lockReturn.getResult(),ResultType.SUCCESS);
//        return lockId;
//    }
//
//    private void releaseLock(MriReference mriIndex, String lock){
//        String fullyQualifiedMriKey = keyspace+"."+ mriIndex.table+"."+mriIndex.index.toString();
//        try {
//            MusicCore.voluntaryReleaseLock(fullyQualifiedMriKey,lock);
//        } catch (MusicLockingException e) {
//            fail(e.getMessage());
//        }
//    }
//
//    private List<Range> getTestRanges(){
//        List<Range> ranges = new ArrayList<>();
//        ranges.add(new Range("table1"));
//        ranges.add(new Range("table2"));
//        return ranges;
//    }
//
//    private String getTestProcessId(){
//        return "tcp://test:1234";
//    }
//
//    private UUID CreateRowWithLockAndCheck(UUID newId, String lockId){
//
//        List<Range> ranges = getTestRanges();
//        String processId = getTestProcessId();
//        UUID newRowId=null;
//        try {
//            newRowId = DatabaseOperations.createEmptyMriRow(keyspace,mriTableName,newId, processId, lockId, ranges);
//        } catch (MDBCServiceException e) {
//            fail("Adding a new empty mri row failed");
//        }
//        getRowFromMriAndCompare(newRowId,ranges,lockId,processId);
//        return newRowId;
//    }
//
//    @Test
//    public void createEmptyMriRowWithLock() {
//        createMRI();
//        //Assume mri creation is working
//        UUID newId = MDBCUtils.generateUniqueKey();
//        MriReference mriIndex = new MriReference(mriTableName,newId);
//        String lockId = getLock(mriTableName,mriIndex);
//        assertTrue("Error obtaining lock",!lockId.isEmpty());
//        UUID newRowId = CreateRowWithLockAndCheck(newId,lockId);
//        assertEquals(newRowId,newId);
//        releaseLock(mriIndex,lockId);
//    }
//
//    private void getRowFromMriAndCompare(UUID newRowId, List<Range> ranges, String lockId, String processId){
//        lockId=(lockId==null)?"":lockId;
//        ResultSet res=null;
//        String queryOp = "SELECT * FROM " +
//                keyspace + "." + mriTableName +
//                " WHERE rangeid = " +
//                newRowId +
//                ";";
//        try {
//            res = session.execute(queryOp);
//        }
//        catch(QueryExecutionException e){
//            fail("Failure executing retrieval of row in MRU error: " + e.getMessage());
//        } catch(SyntaxError e){
//            fail("Failure executing retrieval of row with syntax error: " + e.getMessage());
//        }
//        assertFalse(res.isExhausted());
//        Row response = res.one();
//        UUID id = response.get("rangeid",UUID.class);
//        assertEquals(id,newRowId);
//        Set<String> keys = response.getSet("keys",String.class);
//        for(Range r : ranges){
//            assertTrue("Table was not found in retrieved keys",keys.contains(r.table));
//        }
//        List<TupleValue> redo = response.getList("txredolog",TupleValue.class);
//        assertTrue(redo.isEmpty());
//        String ownerId = response.getString("ownerid");
//        assertEquals(ownerId,lockId);
//        String mpid= response.getString("metricprocessid");
//        assertEquals(mpid,processId);
//    }
//
//    @Test
//    public void getMriRow() {
//        createMRI();
//        //Assume mri creation is working
//        UUID newId = MDBCUtils.generateUniqueKey();
//        MriReference mriIndex = new MriReference(mriTableName,newId);
//        String lockId = getLock(mriTableName,mriIndex);
//        assertTrue("Error obtaining lock",!lockId.isEmpty());
//        UUID newRowId = CreateRowWithLockAndCheck(newId,lockId);
//        MusicRangeInformationRow mriRow=null;
//        try {
//            mriRow = DatabaseOperations.getMriRow(keyspace, mriTableName, newRowId, lockId);
//        } catch (MDBCServiceException e) {
//            fail(e.getErrorMessage());
//        }
//        final List<Range> ranges = getTestRanges();
//        String processId = getTestProcessId();
//        assertEquals("invalid process id", mriRow.metricProcessId,processId);
//        assertEquals("invalid index", mriRow.index,newRowId);
//        assertEquals("invalid lock id",mriRow.ownerId,lockId);
//        assertTrue("redo log is not empty", mriRow.redoLog.isEmpty());
//        List<Range> readRange = mriRow.partition.ranges;
//        List<Range> range = ranges;
//        for(Range r: range){
//            boolean found = false;
//            for(Range rr : readRange) {
//                if(r.equals(rr)) {
//                    found = true;
//                }
//
//            }
//            assertTrue("ranges are incorrect", found);
//        }
//    }
//
//    @Test
//    public void getTransactionDigest() {
//        CreateMTD();
//        Range inputRange = new Range("table1");
//        StagingTable inputStaging = new StagingTable();
//        JSONObject key = new JSONObject();
//        inputStaging.addOperation(OperationType.INSERT,"1", key.put("key", "key_value").toString());
//        HashMap<Range, StagingTable> input= new HashMap<>();
//        input.put(inputRange, inputStaging);
//        MusicTxDigestId newId = new MusicTxDigestId(MDBCUtils.generateUniqueKey());
//        try {
//            MusicTxDigest.createTxDigestRow(keyspace,mtdTableName,newId,MDBCUtils.toString(input));
//        } catch (MDBCServiceException e) {
//            fail("Adding a new mtd row failed");
//        } catch (IOException e) {
//            fail("Fail compressing input staging tables");
//        }
//        HashMap<Range, StagingTable> results=null;
//        try {
//            results = MusicTxDigest.getTransactionDigest(keyspace,mtdTableName,newId);
//        } catch (MDBCServiceException e) {
//            fail("Adding a new mtd row failed with error: "+e.getErrorMessage());
//        }
//        assertTrue(results.containsKey(inputRange));
//        StagingTable newStaging = results.get(inputRange);
//        ArrayList<Operation> opers=null;
//        ArrayList<Operation> initialOpers=null;
//        opers=newStaging.getOperationList();
//        initialOpers=inputStaging.getOperationList();
//
//        assertEquals("Operations are not equal",opers.size(),initialOpers.size());
//        while(!opers.isEmpty()){
//            Operation recvOper = opers.get(0);
//            Operation originalOper = initialOpers.get(0);
//            assertEquals(recvOper,originalOper);
//
//            opers.remove(0);
//            initialOpers.remove(0);
//        }
//    }
//
//    @Test
//    public void createNamespace() {
//        deleteKeyspace();
//        try {
//            CassandraMixin.createNamespace(keyspace,1);
//        } catch (MDBCServiceException e) {
//            fail(e.getErrorMessage());
//        }
//        String describeOp = "USE "+keyspace+";";
//        ResultSet res=null;
//        try {
//            res = session.execute(describeOp);
//        }
//        catch(QueryExecutionException e){
//            fail("Failure executing retrieval of row in MRU error: " + e.getMessage());
//        } catch(SyntaxError e){
//            fail("Failure executing retrieval of row with syntax error: " + e.getMessage());
//        }
//        assertTrue("Error with keyspace: "+keyspace, res.wasApplied());
//    }
//
//    private void getRowFromMtdAndCompare(MusicTxDigestId newId, String transactionDigest){
//        ResultSet res=null;
//        String queryOp = "SELECT * FROM " +
//                keyspace + "." + mtdTableName+
//                " WHERE txid = " +
//                newId.tablePrimaryKey +
//                ";";
//        try {
//            res = session.execute(queryOp);
//        }
//        catch(QueryExecutionException e){
//            fail("Failure executing retrieval of row in MTD error: " + e.getMessage());
//        } catch(SyntaxError e){
//            fail("Failure executing retrieval of row in MTD with syntax error: " + e.getMessage());
//        }
//        assertFalse(res.isExhausted());
//        Row response = res.one();
//        UUID id = response.getUUID("txId");
//        assertEquals(id,newId.tablePrimaryKey);
//        String digest = response.getString("transactiondigest");
//        assertEquals(digest,transactionDigest);
//    }
//
//    @Test
//    public void createTxDigestRow(){
//        CreateMTD();
//        MusicTxDigestId newId = new MusicTxDigestId(MDBCUtils.generateUniqueKey());
//        String transactionDigest = "newdigest";
//        try {
//            MusicTxDigest.createTxDigestRow(keyspace,mtdTableName,newId,transactionDigest);
//        } catch (MDBCServiceException e) {
//            fail("Adding a new empty mtd row failed");
//        }
//        getRowFromMtdAndCompare(newId,transactionDigest);
//
//    }
//
//}
