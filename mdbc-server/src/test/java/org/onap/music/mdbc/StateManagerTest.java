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
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.mdbc.mixins.DBInterface;
import org.onap.music.mdbc.tables.TxCommitProgress;

public class StateManagerTest {

    StateManager stateManager;
    
    @BeforeClass
    public static void beforeClass() {
        System.out.println("StateManagerTest");
    }
    
    @Before
    public void before() throws MDBCServiceException {
        //shouldn't use separate constructor, but this will do for now
        stateManager = new StateManager();
    }

    @Test
    public void testGetEventualRanges() throws NoSuchFieldException, SecurityException {
        List<Range> evList = new ArrayList<>();
        evList.add(new Range("eventualRange"));
        FieldSetter.setField(stateManager, stateManager.getClass().getDeclaredField("eventualRanges"), evList);
        assertEquals(evList, stateManager.getEventualRanges());
    }
    
    @Test
    public void testSetEventualRanges() {
        List<Range> evList = new ArrayList<>();
        evList.add(new Range("eventualRange"));
        stateManager.setEventualRanges(evList);
        assertEquals(evList, stateManager.getEventualRanges());
    }

    @Test
    public void testSetMdbcServerName() {
        String serverName = "serverName";
        stateManager.setMdbcServerName(serverName);
        assertEquals(serverName, stateManager.getMdbcServerName());
    }

    @Test
    public void testGetConnection() throws Exception {
        System.out.println("Testing getting a connection");

        Connection connMock = Mockito.mock(Connection.class);
        String connName = "connectionName";
        Map<String, Connection> connMap = new HashMap<>();
        connMap.put(connName, connMock);
        FieldSetter.setField(stateManager, stateManager.getClass().getDeclaredField("mdbcConnections"),
                connMap);
        
        TxCommitProgress txInfoMock = Mockito.mock(TxCommitProgress.class);
        FieldSetter.setField(stateManager, stateManager.getClass().getDeclaredField("transactionInfo"), 
                txInfoMock);
        
        
        assertEquals(connMock, stateManager.getConnection(connName));
    }

    @Test
    public void testGetRangesToWarmup() throws Exception {
        System.out.println("Testing warmup ranges where no ranges are defined");
        
        //getConnection
        MdbcConnection connMock = Mockito.mock(MdbcConnection.class);
        String connName = "daemon";
        Map<String, Connection> connMap = new HashMap<>();
        connMap.put(connName, connMock);
        FieldSetter.setField(stateManager, stateManager.getClass().getDeclaredField("mdbcConnections"),
                connMap);
        TxCommitProgress txInfoMock = Mockito.mock(TxCommitProgress.class);
        FieldSetter.setField(stateManager, stateManager.getClass().getDeclaredField("transactionInfo"), 
                txInfoMock);
        
        DBInterface dbiMock = Mockito.mock(DBInterface.class);
        Mockito.when(connMock.getDBInterface()).thenReturn(dbiMock);
        Set<Range> allRanges = new HashSet<>();
        allRanges.add(new Range("rangeToWarmup"));
        allRanges.add(new Range("rangeToWarmup2"));
        allRanges.add(new Range("eventualRange"));
        Mockito.when(dbiMock.getSQLRangeSet()).thenReturn(allRanges);
        
        List<Range> eventualRanges = new ArrayList<Range>();
        eventualRanges.add(new Range("eventualRange"));
        stateManager.setEventualRanges(eventualRanges);
        
        assertEquals(2, stateManager.getRangesToWarmup().size());
        assertTrue(stateManager.getRangesToWarmup().contains(new Range("rangeToWarmup")));
        assertTrue(stateManager.getRangesToWarmup().contains(new Range("rangeToWarmup2")));
    }
    
    @Test
    public void testSetWarmupRanges() {
        Set<Range> warmupRanges = new HashSet<>();
        warmupRanges.add(new Range("rangeToWarmup"));
        warmupRanges.add(new Range("rangeToWarmup2"));
        stateManager.setWarmupRanges(warmupRanges);
        assertEquals(warmupRanges, stateManager.getRangesToWarmup());
    }

}
