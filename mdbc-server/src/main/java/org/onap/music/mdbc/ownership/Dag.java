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

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.tables.MriReference;
import org.onap.music.mdbc.tables.MriRowComparator;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;

public class Dag {

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(Dag.class);

    private boolean valid;
    private boolean ownInit;
    private boolean readyInit;
    private Map<UUID,DagNode> nodes;
    private Queue<DagNode> readyNodes;
    private Queue<DagNode> toApplyNodes;
    private Map<Range,Set<DagNode>> rowsPerLatestRange;
    private List<Range> ranges;

    public Dag(){
        this(false);
    }


    public Dag(boolean isValid){
        valid=isValid;
        ranges=null;
        readyNodes = new LinkedList<>();
        toApplyNodes = new LinkedList<>();
        nodes = new HashMap<>();
        ownInit = false;
        readyInit = false;
        rowsPerLatestRange = null;
    }

    private void createDag(List<MusicRangeInformationRow> rows, List<Range> ranges){
        this.ranges = new ArrayList<>(ranges);
        Map<Range,DagNode> latestRow = new HashMap<>();
        //sort to make sure rows are in chronological order
        Collections.sort(rows, new MriRowComparator());
        for(MusicRangeInformationRow row : rows){
            if(!nodes.containsKey(row.getPartitionIndex())){
                DagNode node = new DagNode(row);
                nodes.put(row.getPartitionIndex(),node);
                for(Range range : ranges){
                    List<Range> nodeRanges = row.getDBPartition().getSnapshot();
                    for(Range nRange : nodeRanges){
                        if(nRange.overlaps(range)){
                            if(latestRow.containsKey(range)){
                                final DagNode dagNode = latestRow.get(range);
                                dagNode.addOutgoingEdge(node);
                                node.addIncomingEdge(dagNode);
                            }
                            latestRow.put(range,node);
                        }
                    }
                }
            }
        }
    }

    public static Dag getDag(List<MusicRangeInformationRow> rows, List<Range> ranges){
        Dag newDag = new Dag(true);
        newDag.createDag(rows,ranges);
        return newDag;
    }

    public void setRowsPerLatestRange(Map<Range, Set<DagNode>> rowsPerLatestRange) {
        this.rowsPerLatestRange = rowsPerLatestRange;
    }

    private void initApplyDatastructures(){
        readyInit=true;
        nodes.forEach((id, node) -> {
            if(node.hasNotIncomingEdges()) {
                toApplyNodes.add(node);
            }
        });
    }

    private void initOwnDatastructures(){
        ownInit = true;
        nodes.forEach((id, node) -> {
            if(node.hasNotIncomingEdges()) {
                readyNodes.add(node);
            }
        });
    }

    public DagNode getNode(UUID rowId) throws MDBCServiceException {
        if(!nodes.containsKey(rowId)){
            return null;
        }
        return nodes.get(rowId);
    }

    public synchronized boolean hasNextToOwn(){
        if(!ownInit){
            initOwnDatastructures();
        }
        return !readyNodes.isEmpty();
    }

    public synchronized DagNode nextToOwn() throws MDBCServiceException {
        if(!ownInit){
            initOwnDatastructures();
        }
        DagNode nextNode = readyNodes.poll();
        if(nextNode == null){
            throw new MDBCServiceException("Next To Own was call without checking has next to own");
        }
        return nextNode;
    }

    public synchronized DagNode nextToApply(List<Range> ranges){
        if(!readyInit){
            initApplyDatastructures();
        }
        Set<Range> rangesSet = new HashSet<>(ranges);
        while(!toApplyNodes.isEmpty()){
            DagNode nextNode = toApplyNodes.poll();
            List<DagNode> outgoing = nextNode.getOutgoingEdges();
            for(DagNode out : outgoing){
                out.setApplyDependencyReady(nextNode);
                if(out.areApplyDependenciesReady()){
                    toApplyNodes.add(out);
                }
            }
            if(!nextNode.wasApplied(rangesSet)){
                return nextNode;
            }
        }
        return null;
    }

    public synchronized boolean isDifferent(Dag other){
        Set<DagNode> thisSet = new HashSet<>(nodes.values());
        Set<DagNode> otherSet = new HashSet<>(other.nodes.values());
        return !(thisSet.size()==otherSet.size() &&
            thisSet.containsAll(otherSet));
    }

    public synchronized boolean isOwned(){
        if(!valid){
            return false;
        }
        else if(nodes.isEmpty()){
            return true;
        }
        for(Map.Entry<UUID,DagNode> pair : nodes.entrySet()){
            if(!pair.getValue().isOwned()){
                return false;
            }
        }
        return true;
    }

    public void setOwn(DagNode node) throws MDBCServiceException {
        if(node == null){
            throw new MDBCServiceException("Set Own was call with a null node");
        }
        final DagNode dagNode = nodes.get(node.getId());
        if(dagNode == null){
            throw new MDBCServiceException("Set Own was call with a node that is not in the DAG");
        }
        dagNode.setOwned();
        for(DagNode next: dagNode.getOutgoingEdges()){
            next.setOwnDependencyReady(dagNode);
            if (next.areOwnDependenciesReady()) {
               readyNodes.add(next);
            }
        }
    }

    public void setReady(DagNode node, Range range) throws MDBCServiceException {
        if(node == null){
            throw new MDBCServiceException("Set Ready was call with a null node");
        }
        final DagNode dagNode = nodes.get(node.getId());
        if(dagNode == null){
            throw new MDBCServiceException("Set Ready was call with a node that is not in the DAG");
        }
        dagNode.addReady(range);
    }

    public void setPartiallyReady(DagNode node, Range range, int index) throws MDBCServiceException {
        if(node == null){
            throw new MDBCServiceException("Set Ready was call with a null node");
        }
        final DagNode dagNode = nodes.get(node.getId());
        if(dagNode == null){
            throw new MDBCServiceException("Set Ready was call with a node that is not in the DAG");
        }
        dagNode.addPartiallyReady(range,index);
    }

    public synchronized boolean applied(){
        if(!valid) {
            return false;
        }
        if(!readyInit){
            initApplyDatastructures();
        }
        return toApplyNodes.isEmpty();
    }

    public void setAlreadyApplied(Map<Range, Pair<MriReference,Integer>> alreadyApplied, Set<Range> ranges)
        throws MDBCServiceException {
        for(Map.Entry<UUID,DagNode> node : nodes.entrySet()){
            Set<Range> intersection = new HashSet<>(ranges);
            intersection.retainAll(node.getValue().getRangeSet());
            for(Range r : intersection){
                if(alreadyApplied.containsKey(r)){
                    final Pair<MriReference, Integer> appliedPair = alreadyApplied.get(r);
                    final MriReference appliedRow = appliedPair.getKey();
                    final int index = appliedPair.getValue();
                    final long appliedTimestamp = appliedRow.getTimestamp();
                    final long nodeTimestamp = node.getValue().getTimestamp();
                    if(appliedTimestamp > nodeTimestamp){
                        setReady(node.getValue(),r);
                    }
                    else if(appliedTimestamp == nodeTimestamp){
                        setPartiallyReady(node.getValue(),r,index);
                    }
                }
            }
        }
    }

    public void addNewNode(MusicRangeInformationRow row, List<DagNode> dependencies) throws MDBCServiceException {
        boolean found=false;
        if (ranges != null) {
            DatabasePartition dbPartition = row.getDBPartition();
            for(Range range : dbPartition.getSnapshot()){
                for(Range dagRange : ranges){
                    if(dagRange.overlaps(range)){
                        found = true;
                        break;
                    }
                }
                if(found) break;
            }
            if(!found) {
                return;
            }
        }

        DagNode newNode = new DagNode(row);
        nodes.put(row.getPartitionIndex(),newNode);
        for(DagNode dependency : dependencies) {
            newNode.addIncomingEdge(dependency);
            DagNode localNode = getNode(dependency.getId());
            localNode.addOutgoingEdge(newNode);
        }
    }

    public void addNewNodeWithSearch(MusicRangeInformationRow row, List<Range> ranges) throws MDBCServiceException {
        Map<Range,DagNode> newestNode = new HashMap<>();
        for(DagNode node : nodes.values()){
            for(Range range : ranges) {
                if (node.getRangeSet().contains(range)){
                   if(!newestNode.containsKey(range)){
                        newestNode.put(range,node);
                   }
                   else{
                       DagNode current = newestNode.get(range);
                       if(node.getTimestamp() > current.getTimestamp()){
                           newestNode.put(range,node);
                       }
                   }
                }
            }
        }
        List<DagNode> dependencies = newestNode.values().stream().distinct().collect(Collectors.toList());
        addNewNode(row,dependencies);
    }

    /**
     * 
     * @return All ranges in every node of the DAG
     */
    public Set<Range> getAllRanges(){
        Set<Range> ranges = new HashSet<>();
        for(DagNode node : nodes.values()){
            ranges.addAll(node.getRangeSet());
        }
        return ranges;
    }

    public void setIsLatest(UUID id, boolean isLatest){
        DagNode dagNode = nodes.get(id);
        dagNode.setIsLatest(isLatest);
        if(isLatest) {
            MusicRangeInformationRow row = dagNode.getRow();
            DatabasePartition dbPartition = row.getDBPartition();
            for (Range range : dbPartition.getSnapshot()) {
                if (!rowsPerLatestRange.containsKey(range)) {
                    rowsPerLatestRange.put(range, new HashSet<>());
                }
                rowsPerLatestRange.get(range).add(dagNode);
            }
        }
        else{
            MusicRangeInformationRow row = dagNode.getRow();
            DatabasePartition dbPartition = row.getDBPartition();
            for (Range range : dbPartition.getSnapshot()) {
                if (rowsPerLatestRange.containsKey(range)) {
                    rowsPerLatestRange.get(range).remove(dagNode);
                }
            }
        }
    }

    private Map<Range,Set<DagNode>> getIsLatestPerRange(){
        if(rowsPerLatestRange == null){
            rowsPerLatestRange = new HashMap<>();
        }
        for(DagNode node : nodes.values()){
            MusicRangeInformationRow row = node.getRow();
            DatabasePartition dbPartition = row.getDBPartition();
            if (row.getIsLatest()) {
                for(Range range : dbPartition.getSnapshot()){
                    if(!rowsPerLatestRange.containsKey(range)){
                        rowsPerLatestRange.put(range,new HashSet<>());
                    }
                    rowsPerLatestRange.get(range).add(node);
                }
            }
        }
        return new HashMap<>(rowsPerLatestRange);
    }

    private List<DagNode> getOldestDoubleRows(Map<Range,Set<DagNode>> rowPerLatestRange) throws MDBCServiceException {
        Set<DagNode> oldest = new HashSet<>();
        for(Map.Entry<Range,Set<DagNode>> rangeAndNodes : rowPerLatestRange.entrySet()){
            Range range = rangeAndNodes.getKey();
            Set<DagNode> nodes = rangeAndNodes.getValue();
            if(nodes.size() > 2){
                logger.error("Range "+range.getTable()+"has more than 2 active rows");
                throw new MDBCServiceException("Range has more than 2 active rows");
            }
            else if(nodes.size()==2){
                DagNode older = null;
                long olderTimestamp = Long.MAX_VALUE;
                for(DagNode node : nodes){
                    if(olderTimestamp > node.getTimestamp()){
                        older  = node;
                        olderTimestamp=node.getTimestamp();
                    }
                }
                oldest.add(older);
            }
        }
        return new ArrayList<>(oldest);
    }

    public List<DagNode> getOldestDoubles() throws MDBCServiceException{
        Map<Range,Set<DagNode>> rowsPerLatestRange = getIsLatestPerRange();
        List<DagNode> toDisable = getOldestDoubleRows(rowsPerLatestRange);
        return toDisable;
    }

    public Pair<List<Range>,Set<DagNode>> getIncompleteRangesAndDependents() throws MDBCServiceException {
        List<Range> incomplete = new ArrayList<>();
        Set<DagNode> dependents = new HashSet<>();
        Map<Range,Set<DagNode>> rowsPerLatestRange = getIsLatestPerRange();
        List<DagNode> toDisable = getOldestDoubleRows(rowsPerLatestRange);
        for(DagNode node : toDisable) {
            for (Range range : node.getRangeSet()) {
                rowsPerLatestRange.get(range).remove(node);
                if (rowsPerLatestRange.get(range).size() == 0) {
                    incomplete.add(range);
                    dependents.add(node);
                }
            }
        }
        return Pair.of(incomplete,dependents);
    }
}
