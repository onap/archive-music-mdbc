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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.tables.MriReference;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.MusicTxDigestId;

public class DagNode {

    private boolean owned;
    private boolean applyInit;
    private final MusicRangeInformationRow row;
    private int currentIndex;
    private Set<DagNode> dependencies;
    private Set<DagNode> outgoingEdges;
    private Set<DagNode> readyOwnDependencies;
    private Set<DagNode> readyAppliedDependencies;
    private List<Range> alreadyApplied;
    private Map<Range,Integer> partiallyApplied;
    private Map<Range,Integer> startIndex;

    public DagNode(MusicRangeInformationRow row){
        this.row = row;
        owned = false;
        applyInit = false;
        currentIndex = 0;
        dependencies = new HashSet<>();
        outgoingEdges = new HashSet<>();
        readyOwnDependencies = new HashSet<>();
        readyAppliedDependencies = new HashSet<>();
        alreadyApplied = new ArrayList<>();
        partiallyApplied = new HashMap<>();
        startIndex = new HashMap<>();
    }

    public MusicRangeInformationRow getRow() {
        return row;
    }

    public synchronized void setOwned(){
        owned = true;
    }

    public synchronized boolean isOwned(){
        return owned;
    }

    /**
     * 
     * @return the row's MRI Index represented by this dagnode
     */
    public UUID getId(){
        return row.getPartitionIndex();
    }

    public synchronized void addIncomingEdge(DagNode sourceNode){
        dependencies.add(sourceNode);
    }

    public synchronized void addOutgoingEdge(DagNode destinationNode){
        outgoingEdges.add(destinationNode);
    }

    public synchronized  boolean hasNotIncomingEdges(){
        return dependencies.isEmpty();
    }

    public synchronized List<DagNode> getOutgoingEdges(){
       return new ArrayList<>(outgoingEdges);
    }

    public synchronized  void addReady(Range r) throws MDBCServiceException {
        if(!row.getDBPartition().isContained(r)){
            throw new MDBCServiceException("Range was set ready to a node that doesn't own it");
        }
        alreadyApplied.add(r);
    }

    public synchronized void addPartiallyReady(Range r, int index){
        partiallyApplied.put(r,index);
    }

    public synchronized void setOwnDependencyReady(DagNode other){
        readyOwnDependencies.add(other);
    }

    public synchronized boolean areOwnDependenciesReady(){
        final int dSize = dependencies.size();
        final int oSize = readyOwnDependencies.size();
        return (dSize == oSize) && dependencies.containsAll(readyOwnDependencies);
    }

    public synchronized void setApplyDependencyReady(DagNode other){
        readyAppliedDependencies.add(other);
    }

    public synchronized boolean areApplyDependenciesReady(){
        final int dSize = dependencies.size();
        final int oSize = readyAppliedDependencies.size();
        return (dSize == oSize) && dependencies.containsAll(readyAppliedDependencies);
    }

    private void initializeApply(Set<Range> ranges){
        applyInit = true;
        int redoSize = row.getRedoLog().size();
        // No need to apply
        for(Range r: alreadyApplied){
            startIndex.put(r,redoSize);
        }
        // Only apply the required subsection
        partiallyApplied.forEach((r, index) -> {
            startIndex.put(r,index);
        });
        // All other ranges need to be applied completely
        Set<Range> alreadySet = new HashSet<>(alreadyApplied);
        Set<Range> partialSet = partiallyApplied.keySet();
        Set<Range> pending = new HashSet<>(ranges);
        pending.removeAll(alreadySet);
        pending.removeAll(partialSet);
        for(Range r: pending){
            startIndex.put(r,-1);
        }
        //Get the index of the redo log to begin with
        currentIndex = startIndex.values().stream().mapToInt(v->v).min().orElse(0);
        currentIndex = currentIndex+1;
    }

    /**
     * 
     * @param ranges
     * @return the index of the next transaction to replay and the ranges needed for this transaction
     */
    public synchronized Pair<MusicTxDigestId, Set<Range>> nextNotAppliedTransaction(Set<Range> ranges){
        if(row.getRedoLog().isEmpty()) return null;
        if(!applyInit){
            initializeApply(ranges);
        }
        final List<MusicTxDigestId> redoLog = row.getRedoLog();
        if(currentIndex  < redoLog.size()){
            Set<Range> responseRanges= new HashSet<>();
            startIndex.forEach((r, index) -> {
                if(index < currentIndex){
                   responseRanges.add(r);
                }
            });
            return Pair.of(row.getRedoLog().get(currentIndex++),responseRanges);
        }
        return null;
    }

    public void setIsLatest(boolean isLatest){
        row.setIsLatest(isLatest);
    }

    public synchronized Set<Range> getRangeSet(){
       return new HashSet<>(row.getDBPartition().getSnapshot());
    }

    public synchronized boolean wasApplied(Set<Range> ranges){
        if(row.getRedoLog().isEmpty()) return true;
        if(!applyInit){
            initializeApply(ranges);
        }        
        return currentIndex >= row.getRedoLog().size();
    }

    public long getTimestamp(){
        return row.getTimestamp();
    }


    @Override
    public boolean equals(Object o){
        if (this == o) return true;
        if(o == null) return false;
        if(!(o instanceof DagNode)) return false;
        DagNode other = (DagNode) o;
        return other.row.equals(this.row);
    }

    @Override
    public int hashCode(){
        return row.getPartitionIndex().hashCode();
    }


}
