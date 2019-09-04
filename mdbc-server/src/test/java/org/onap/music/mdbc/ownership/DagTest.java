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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.MDBCUtils;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.tables.MriReference;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.MusicTxDigestId;

import static java.util.concurrent.TimeUnit.*;
import static org.junit.Assert.*;

public class DagTest {

    private MusicRangeInformationRow createNewRow(Set<Range> ranges, String lockid, boolean isLatest){
        List<MusicTxDigestId> redoLog = new ArrayList<>();
        return createNewRow(ranges,lockid,isLatest,redoLog);
    }

    private MusicRangeInformationRow createNewRow(Set<Range> ranges, String lockid, boolean isLatest,
                                                  List<MusicTxDigestId> redoLog) {
        UUID id = MDBCUtils.generateTimebasedUniqueKey();
        DatabasePartition dbPartition = new DatabasePartition(ranges, id, lockid);
        return new MusicRangeInformationRow(dbPartition, redoLog, isLatest);
    }

    @Test
    public void getDag() throws InterruptedException, MDBCServiceException {
        List<MusicRangeInformationRow> rows = new ArrayList<>();
        Set<Range> ranges = new HashSet<>( Arrays.asList(
           new Range("schema.range1")
        ));
        rows.add(createNewRow(new HashSet<>(ranges),"",false));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(ranges),"",false));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(ranges),"",true));
        Dag dag = Dag.getDag(rows, ranges);
        DagNode node1 = dag.getNode(rows.get(0).getPartitionIndex());
        DagNode node2 = dag.getNode(rows.get(1).getPartitionIndex());
        DagNode node3 = dag.getNode(rows.get(2).getPartitionIndex());
        List<DagNode> outgoingEdges1 = node1.getOutgoingEdges();
        assertTrue(node1.hasNotIncomingEdges());
        assertEquals(outgoingEdges1.size(),1);
        assertEquals(outgoingEdges1.get(0),node2);
        List<DagNode> outgoingEdges2 = node2.getOutgoingEdges();
        assertEquals(outgoingEdges2.size(),1);
        assertEquals(outgoingEdges2.get(0),node3);
        assertFalse(node2.hasNotIncomingEdges());
        List<DagNode> outgoingEdges3 = node3.getOutgoingEdges();
        assertEquals(outgoingEdges3.size(),0);
        assertFalse(node3.hasNotIncomingEdges());
    }

    @Test
    public void getDag2() throws InterruptedException, MDBCServiceException {
        List<MusicRangeInformationRow> rows = new ArrayList<>();
        List<Range> range1 = new ArrayList<>( Arrays.asList(
           new Range("schema.range1")
        ));
        List<Range> range2 = new ArrayList<>( Arrays.asList(
           new Range("schema.range2")
        ));
        Set<Range> ranges = new HashSet<>( Arrays.asList(
            new Range("schema.range2"),
            new Range("schema.range1")
        ));
        rows.add(createNewRow(new HashSet<>(range1),"",false));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(range2),"",false));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(ranges),"",true));
        Dag dag = Dag.getDag(rows, ranges);
        DagNode node1 = dag.getNode(rows.get(0).getPartitionIndex());
        DagNode node2 = dag.getNode(rows.get(1).getPartitionIndex());
        DagNode node3 = dag.getNode(rows.get(2).getPartitionIndex());
        List<DagNode> outgoingEdges1 = node1.getOutgoingEdges();
        assertTrue(node1.hasNotIncomingEdges());
        assertEquals(outgoingEdges1.size(),1);
        assertEquals(outgoingEdges1.get(0),node3);
        List<DagNode> outgoingEdges2 = node2.getOutgoingEdges();
        assertEquals(outgoingEdges2.size(),1);
        assertEquals(outgoingEdges2.get(0),node3);
        assertTrue(node2.hasNotIncomingEdges());
        List<DagNode> outgoingEdges3 = node3.getOutgoingEdges();
        assertEquals(outgoingEdges3.size(),0);
        assertFalse(node3.hasNotIncomingEdges());
    }


    @Test
    public void nextToOwn() throws InterruptedException, MDBCServiceException {
        List<MusicRangeInformationRow> rows = new ArrayList<>();
        Set<Range> ranges = new HashSet<>( Arrays.asList(
            new Range("schema.range1")
        ));
        rows.add(createNewRow(new HashSet<>(ranges),"",false));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(ranges),"",false));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(ranges),"",true));
        Dag dag = Dag.getDag(rows, ranges);
        int counter = 0;
        while(dag.hasNextToOwn()){
            DagNode node = dag.nextToOwn();
            MusicRangeInformationRow row = node.getRow();
            UUID uuid = row.getPartitionIndex();
            assertEquals(rows.get(counter).getPartitionIndex(),uuid);
            dag.setOwn(node);
            counter++;
            assertNotEquals(4,counter);
        }
        assertEquals(3,counter);
        assertTrue(dag.isOwned());
    }

    @Test
    public void nextToApply() throws InterruptedException {
        List<MusicRangeInformationRow> rows = new ArrayList<>();
        Set<Range> ranges = new HashSet<>( Arrays.asList(
            new Range("schema.range1")
        ));
        List<MusicTxDigestId> redo1 = new ArrayList<>(Arrays.asList(
            new MusicTxDigestId(null,MDBCUtils.generateUniqueKey(),0)
        ));
        rows.add(createNewRow(new HashSet<>(ranges),"",false,redo1));
        MILLISECONDS.sleep(10);
        List<MusicTxDigestId> redo2 = new ArrayList<>(Arrays.asList(
            new MusicTxDigestId(null,MDBCUtils.generateUniqueKey(),0)
        ));
        rows.add(createNewRow(new HashSet<>(ranges),"",false,redo2));
        MILLISECONDS.sleep(10);
        List<MusicTxDigestId> redo3 = new ArrayList<>(Arrays.asList(
            new MusicTxDigestId(null,MDBCUtils.generateUniqueKey(),0)
        ));
        rows.add(createNewRow(new HashSet<>(ranges),"",true,redo3));
        Dag dag = Dag.getDag(rows, ranges);
        int nodeCounter = 0;
        HashSet<Range> rangesSet = new HashSet<>(ranges);
        while(!dag.applied()){
            DagNode node = dag.nextToApply(ranges);
            Pair<MusicTxDigestId, Set<Range>> pair = node.nextNotAppliedTransaction(rangesSet);
            int transactionCounter = 0;
            while(pair!=null) {
                assertNotEquals(1,transactionCounter);
                MusicRangeInformationRow row = rows.get(nodeCounter);
                MusicTxDigestId id = row.getRedoLog().get(transactionCounter);
                assertEquals(id,pair.getKey());
                assertEquals(0,pair.getKey().index);
                Set<Range> value = pair.getValue();
                assertEquals(1,value.size());
                assertTrue(value.contains(new Range("schema.range1")));
                //assertEquals(new Range("schema.range1"),value.get(0));
                pair = node.nextNotAppliedTransaction(rangesSet);
                transactionCounter++;
            }
            assertEquals(1,transactionCounter);
            nodeCounter++;
        }
        assertEquals(3,nodeCounter);
    }

    @Test
    public void nextToApply2() throws InterruptedException, MDBCServiceException {
        Map<Range, Pair<MriReference, MusicTxDigestId>> alreadyApplied = new HashMap<>();
        List<MusicRangeInformationRow> rows = new ArrayList<>();
        Set<Range> ranges = new HashSet<>( Arrays.asList(
            new Range("schema.range1")
        ));
        List<MusicTxDigestId> redo1 = new ArrayList<>(Arrays.asList(
            new MusicTxDigestId(null,MDBCUtils.generateUniqueKey(),0)
        ));
        rows.add(createNewRow(new HashSet<>(ranges),"",false,redo1));
        MILLISECONDS.sleep(10);
        List<MusicTxDigestId> redo2 = new ArrayList<>(Arrays.asList(
            new MusicTxDigestId(null,MDBCUtils.generateUniqueKey(),0),
            new MusicTxDigestId(null,MDBCUtils.generateUniqueKey(),1)
        ));
        MusicRangeInformationRow newRow = createNewRow(new HashSet<>(ranges), "", false, redo2);
        alreadyApplied.put(new Range("schema.range1"),Pair.of(new MriReference(newRow.getPartitionIndex()), redo1.get(0)));
        rows.add(newRow);
        MILLISECONDS.sleep(10);
        List<MusicTxDigestId> redo3 = new ArrayList<>(Arrays.asList(
            new MusicTxDigestId(null,MDBCUtils.generateUniqueKey(),0)
        ));
        rows.add(createNewRow(new HashSet<>(ranges),"",true,redo3));
        Dag dag = Dag.getDag(rows, ranges);
        HashSet<Range> rangesSet = new HashSet<>(ranges);
        dag.setAlreadyApplied(alreadyApplied, rangesSet);
        int nodeCounter = 1;
        while(!dag.applied()){
            DagNode node = dag.nextToApply(ranges);
            Pair<MusicTxDigestId, Set<Range>> pair = node.nextNotAppliedTransaction(rangesSet);
            int transactionCounter = 0;
            while(pair!=null) {
                assertNotEquals(1,transactionCounter);
                MusicRangeInformationRow row = rows.get(nodeCounter);
                MusicTxDigestId id = row.getRedoLog().get(2-nodeCounter);
                assertEquals(id,pair.getKey());
                assertEquals(2-nodeCounter,pair.getKey().index);
                Set<Range> value = pair.getValue();
                assertEquals(1,value.size());
                assertTrue(value.contains(new Range("schema.range1")));
                //assertEquals(new Range("schema.range1"),value.get(0));
                pair = node.nextNotAppliedTransaction(rangesSet);
                transactionCounter++;
            }
            assertEquals(1,transactionCounter);
            nodeCounter++;
        }
        assertEquals(3,nodeCounter);
    }

    @Test
    public void isDifferent() throws InterruptedException {
        List<MusicRangeInformationRow> rows = new ArrayList<>();
        List<Range> range1 = new ArrayList<>( Arrays.asList(
            new Range("schema.range1")
        ));
        List<Range> range2 = new ArrayList<>( Arrays.asList(
            new Range("schema.range2")
        ));
        Set<Range> ranges = new HashSet<>( Arrays.asList(
            new Range("schema.range2"),
            new Range("schema.range1")
        ));
        rows.add(createNewRow(new HashSet<>(range1),"",false));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(range2),"",false));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(ranges),"",true));
        List<MusicRangeInformationRow> rows2 = new ArrayList<>(rows);
        List<MusicRangeInformationRow> rows3 = new ArrayList<>(rows);
        MILLISECONDS.sleep(10);
        rows3.add(createNewRow(new HashSet<>(ranges),"",true));
        Dag dag = Dag.getDag(rows, ranges);
        Dag dag2 = Dag.getDag(rows2, new HashSet<>(ranges));
        Dag dag3 = Dag.getDag(rows3, new HashSet<>(ranges));
        assertFalse(dag.isDifferent(dag2));
        assertFalse(dag2.isDifferent(dag));
        assertTrue(dag.isDifferent(dag3));
        assertTrue(dag3.isDifferent(dag));
        assertTrue(dag2.isDifferent(dag3));
        assertTrue(dag3.isDifferent(dag2));
    }

    @Test
    public void getOldestDoubles() throws InterruptedException, MDBCServiceException {
        List<MusicRangeInformationRow> rows = new ArrayList<>();
        List<Range> range1 = new ArrayList<>( Arrays.asList(
            new Range("schema.range1")
        ));
        List<Range> range2 = new ArrayList<>( Arrays.asList(
            new Range("schema.range2")
        ));
        Set<Range> ranges = new HashSet<>( Arrays.asList(
            new Range("schema.range2"),
            new Range("schema.range1")
        ));
        rows.add(createNewRow(new HashSet<>(range1),"",false));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(range2),"",false));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(range1),"",true));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(range2),"",true));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(ranges),"",true));
        Dag dag = Dag.getDag(rows, ranges);
        List<DagNode> oldestDoubles = dag.getOldestDoubles();
        assertTrue(oldestDoubles.contains(dag.getNode(rows.get(2).getPartitionIndex())));
        assertTrue(oldestDoubles.contains(dag.getNode(rows.get(3).getPartitionIndex())));
        assertEquals(2,oldestDoubles.size());
    }

    @Test
    public void getIncompleteRangesAndDependents() throws InterruptedException, MDBCServiceException {
                List<MusicRangeInformationRow> rows = new ArrayList<>();
        List<Range> range1 = new ArrayList<>( Arrays.asList(
            new Range("schema.range1")
        ));
        List<Range> range2 = new ArrayList<>( Arrays.asList(
            new Range("schema.range2"),
            new Range("schema.range3")
        ));
        Set<Range> ranges = new HashSet<>( Arrays.asList(
            new Range("schema.range2"),
            new Range("schema.range1")
        ));
        rows.add(createNewRow(new HashSet<>(range1),"",false));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(range2),"",false));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(range1),"",true));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(range2),"",true));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(ranges),"",true));
        Dag dag = Dag.getDag(rows, ranges);
        Pair<Set<Range>, Set<DagNode>> incompleteRangesAndDependents = dag.getIncompleteRangesAndDependents();
        Set<Range> incomplete = incompleteRangesAndDependents.getKey();
        Set<DagNode> dependents = incompleteRangesAndDependents.getValue();
        assertEquals(1,incomplete.size());
        assertTrue(incomplete.contains(new Range("schema.range3")));
        assertEquals(1,dependents.size());
        assertTrue(dependents.contains(dag.getNode(rows.get(3).getPartitionIndex())));
    }

    @Test
    public void getIncompleteRangesAndDependents2() throws InterruptedException, MDBCServiceException {
        List<MusicRangeInformationRow> rows = new ArrayList<>();
        List<Range> range1 = new ArrayList<>( Arrays.asList(
            new Range("schema.range1"),
            new Range("schema.range4")
        ));
        List<Range> range2 = new ArrayList<>( Arrays.asList(
            new Range("schema.range2"),
            new Range("schema.range3")
        ));
        Set<Range> ranges = new HashSet<>( Arrays.asList(
            new Range("schema.range2"),
            new Range("schema.range1")
        ));
        rows.add(createNewRow(new HashSet<>(range1),"",false));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(range2),"",false));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(range1),"",true));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(range2),"",true));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(ranges),"",true));
        Dag dag = Dag.getDag(rows, ranges);
        Pair<Set<Range>, Set<DagNode>> incompleteRangesAndDependents = dag.getIncompleteRangesAndDependents();
        Set<Range> incomplete = incompleteRangesAndDependents.getKey();
        Set<DagNode> dependents = incompleteRangesAndDependents.getValue();
        assertEquals(2,incomplete.size());
        assertTrue(incomplete.contains(new Range("schema.range3")));
        assertTrue(incomplete.contains(new Range("schema.range4")));
        assertEquals(2,dependents.size());
        assertTrue(dependents.contains(dag.getNode(rows.get(3).getPartitionIndex())));
        assertTrue(dependents.contains(dag.getNode(rows.get(2).getPartitionIndex())));
    }

    @Test
    public void addNewNodeWithSearch() throws InterruptedException, MDBCServiceException {
        List<MusicRangeInformationRow> rows = new ArrayList<>();
        List<Range> range1 = new ArrayList<>( Arrays.asList(
            new Range("schema.range1")
        ));
        List<Range> range2 = new ArrayList<>( Arrays.asList(
            new Range("schema.range2"),
            new Range("schema.range3")
        ));
        Set<Range> ranges = new HashSet<>( Arrays.asList(
            new Range("schema.range2"),
            new Range("schema.range1")
        ));
        Set<Range> allRanges = new HashSet<>( Arrays.asList(
            new Range("schema.range2"),
            new Range("schema.range3"),
            new Range("schema.range1")
        ));
        rows.add(createNewRow(new HashSet<>(range1),"",false));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(range2),"",false));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(range1),"",true));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(range2),"",true));
        MILLISECONDS.sleep(10);
        rows.add(createNewRow(new HashSet<>(ranges),"",true));
        Dag dag = Dag.getDag(rows, ranges);
        MusicRangeInformationRow newRow = createNewRow(new HashSet<>(allRanges), "", true);
        dag.addNewNodeWithSearch(newRow,allRanges);
        DagNode newNode = dag.getNode(newRow.getPartitionIndex());
        DagNode node = dag.getNode(rows.get(4).getPartitionIndex());
        List<DagNode> outgoingEdges = node.getOutgoingEdges();
        assertEquals(1,outgoingEdges.size());
        assertEquals(newNode,outgoingEdges.get(0));
        DagNode oNode = dag.getNode(rows.get(3).getPartitionIndex());
        outgoingEdges = node.getOutgoingEdges();
        assertEquals(1,outgoingEdges.size());
        assertEquals(newNode,outgoingEdges.get(0));
        DagNode node0 = dag.getNode(rows.get(0).getPartitionIndex());
        outgoingEdges = node0.getOutgoingEdges();
        assertEquals(1,outgoingEdges.size());
        DagNode node1 = dag.getNode(rows.get(1).getPartitionIndex());
        outgoingEdges = node1.getOutgoingEdges();
        assertEquals(1,outgoingEdges.size());
        DagNode node2 = dag.getNode(rows.get(2).getPartitionIndex());
        outgoingEdges = node1.getOutgoingEdges();
        assertEquals(1,outgoingEdges.size());
    }
}