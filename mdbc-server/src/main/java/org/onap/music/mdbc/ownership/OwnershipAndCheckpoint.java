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

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.mixins.DBInterface;
import org.onap.music.mdbc.mixins.LockRequest;
import org.onap.music.mdbc.mixins.LockResult;
import org.onap.music.mdbc.mixins.MusicInterface;
import org.onap.music.mdbc.mixins.MusicInterface.OwnershipReturn;
import org.onap.music.mdbc.mixins.MusicMixin;
import org.onap.music.mdbc.query.SQLOperationType;
import org.onap.music.mdbc.tables.MriReference;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.MusicTxDigestId;
import org.onap.music.mdbc.tables.StagingTable;

public class OwnershipAndCheckpoint{

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(OwnershipAndCheckpoint.class);
    private Lock checkpointLock;
    private AtomicBoolean change;
    private Map<Range, Pair<MriReference, Integer>> alreadyApplied;
    private Map<UUID,Long> ownershipBeginTime;
    private long timeoutInMs;

    public OwnershipAndCheckpoint(){
      this(new HashMap<>(),Long.MAX_VALUE);
    }

    public OwnershipAndCheckpoint(Map<Range, Pair<MriReference, Integer>> alreadyApplied, long timeoutInMs){
        change = new AtomicBoolean(true);
        checkpointLock = new ReentrantLock();
        this.alreadyApplied = alreadyApplied;
        ownershipBeginTime = new HashMap<>();
        this.timeoutInMs =  timeoutInMs;
    }

    public void startOwnershipTimeoutClock(UUID id){
        ownershipBeginTime.put(id,System.currentTimeMillis());
    }

    public void stopOwnershipTimeoutClock(UUID id){
        if(ownershipBeginTime.containsKey(id)) {
            ownershipBeginTime.remove(id);
        }
        else{
            logger.warn("clock was deleted with an invalid/stale id "+id);
        }
    }

    public boolean timeout(UUID id) throws MDBCServiceException {
        long current = System.currentTimeMillis();
        if(!ownershipBeginTime.containsKey(id)){
            throw new MDBCServiceException("timeout was call with an invalid id");
        }
        Long beginTime = ownershipBeginTime.get(id);
        if(current-beginTime > timeoutInMs){
            return true;
        }
        return false;
    }

    
    private List<MusicRangeInformationRow> extractRowsForRange(List<MusicRangeInformationRow> allMriRows, List<Range> ranges,
                                                  boolean onlyIsLatest){
        List<MusicRangeInformationRow> rows = new ArrayList<>();
        for(MusicRangeInformationRow row : allMriRows){
            if(onlyIsLatest && !row.getIsLatest()){
                continue;
            }
            final List<Range> rowRanges = row.getDBPartition().getSnapshot();
            boolean found = false;
            for(Range sRange : ranges){
                for(Range rRange: rowRanges) {
                    if(sRange.overlaps(rRange)){
                        rows.add(row);
                        found=true;
                        break;
                    }
                }
                if(found) break;
            }
        }
        return rows;
    }

    /**
     * Extracts all the rows that match any of the ranges.
     * @param allMriRows
     * @param ranges - ranges interested in
     * @param onlyIsLatest - only return the "latest" rows
     * @return
     */
    private List<MusicRangeInformationRow> extractRowsForRange(MusicInterface music, List<Range> ranges, boolean onlyIsLatest)
        throws MDBCServiceException {
        final List<MusicRangeInformationRow> allMriRows = music.getAllMriRows();
        return extractRowsForRange(allMriRows,ranges,onlyIsLatest);
    }

    /**
     * make sure data is up to date for list of ranges
     * @param mi
     * @param di
     * @param extendedDag
     * @param ranges
     * @param locks
     * @param ownOpId
     * @throws MDBCServiceException
     */
    public void checkpoint(MusicInterface mi, DBInterface di, Dag extendedDag, List<Range> ranges,
        Map<MusicRangeInformationRow, LockResult> locks, UUID ownOpId) throws MDBCServiceException {
        if(ranges.isEmpty()){
            return;
        }
        try {
            checkpointLock.lock();
            change.set(true);
            Set<Range> rangesSet = new HashSet<>(ranges);
            extendedDag.setAlreadyApplied(alreadyApplied, rangesSet);
            applyRequiredChanges(mi, di, extendedDag, ranges, ownOpId);
        }
        catch(MDBCServiceException e){
            stopOwnershipTimeoutClock(ownOpId);
            throw e;
        }
        finally {
            checkpointLock.unlock();
        }
    }

    private void enableForeignKeys(DBInterface di) throws MDBCServiceException {
        try {
            di.enableForeignKeyChecks();
        } catch (SQLException e) {
            throw new MDBCServiceException("Error enabling foreign keys checks",e);
        }
    }

    private void disableForeignKeys(DBInterface di) throws MDBCServiceException {
        try {
            di.disableForeignKeyChecks();
        } catch (SQLException e) {
            throw new MDBCServiceException("Error disable foreign keys checks",e);
        }
    }

    private void applyTxDigest(List<Range> ranges, DBInterface di, StagingTable txDigest)
        throws MDBCServiceException {
        try {
            di.applyTxDigest(txDigest,ranges);
        } catch (SQLException e) {
            throw new MDBCServiceException("Error applying tx digest in local SQL",e);
        }
    }
    
    /**
     * Replay the updates for the partitions containing ranges to the local database
     * @param mi
     * @param di
     * @param rangesToWarmup
     * @throws MDBCServiceException
     */
    public void warmup(MusicInterface mi, DBInterface di, List<Range> rangesToWarmup) throws MDBCServiceException {
        if(rangesToWarmup.isEmpty()){
            return;
        }
        boolean ready = false;
        change.set(true);
        Set<Range> rangeSet = new HashSet<Range>(rangesToWarmup);
        Dag dag = new Dag(false);
        while(!ready){
            if(change.get()){
                change.set(false);
                final List<MusicRangeInformationRow> rows = extractRowsForRange(mi, rangesToWarmup,false);
                dag = Dag.getDag(rows,rangesToWarmup);
            }
            else if(!dag.applied()){
                DagNode node = dag.nextToApply(rangesToWarmup);
                if(node!=null) {
                    Pair<MusicTxDigestId, List<Range>> pair = node.nextNotAppliedTransaction(rangeSet);
                    while (pair != null) {
                        disableForeignKeys(di);
                        checkpointLock.lock();
                        if (change.get()) {
                            enableForeignKeys(di);
                            checkpointLock.unlock();
                            break;
                        } else {
                            applyDigestAndUpdateDataStructures(mi, di, rangesToWarmup, node, pair);
                        }
                        pair = node.nextNotAppliedTransaction(rangeSet);
                        enableForeignKeys(di);
                        checkpointLock.unlock();
                    }
                }
            }
            else{
                ready = true;
            }
        }
    }

    private void applyDigestAndUpdateDataStructures(MusicInterface mi, DBInterface di, List<Range> ranges, DagNode node,
                                                    Pair<MusicTxDigestId, List<Range>> pair) throws MDBCServiceException {
        final StagingTable txDigest;
        try {
            txDigest = mi.getTxDigest(pair.getKey());
        } catch (MDBCServiceException e) {
            logger.warn("Transaction digest was not found, this could be caused by a failure of the previous owner"
                +"And would normally only happen as the last ID of the corresponding redo log. Please check that this is the"
                +" case for txID "+pair.getKey().transactionId.toString());
            return;
        }
        applyTxDigest(ranges,di, txDigest);
        for (Range r : pair.getValue()) {
            MusicRangeInformationRow row = node.getRow();
            alreadyApplied.put(r, Pair.of(new MriReference(row.getPartitionIndex()), pair.getKey().index));
        }
    }

    private void applyRequiredChanges(MusicInterface mi, DBInterface db, Dag extendedDag, List<Range> ranges, UUID ownOpId)
        throws MDBCServiceException {
        Set<Range> rangeSet = new HashSet<Range>(ranges);
        disableForeignKeys(db);
        while(!extendedDag.applied()){
            DagNode node = extendedDag.nextToApply(ranges);
            if(node!=null) {
                Pair<MusicTxDigestId, List<Range>> pair = node.nextNotAppliedTransaction(rangeSet);
                while (pair != null) {
                    applyDigestAndUpdateDataStructures(mi, db, ranges, node, pair);
                    pair = node.nextNotAppliedTransaction(rangeSet);
                    if (timeout(ownOpId)) {
                        enableForeignKeys(db);
                        throw new MDBCServiceException("Timeout apply changes to local dbi");
                    }
                }
            }
        }
        enableForeignKeys(db);

    }

    /**
     * Use this functions to verify ownership, and taking locking ownership of new ranges
     * @param ranges the ranges that should be own after calling this function
     * @param currPartition current information of the ownership in the system
     * @param ownOpId is the id used to describe this ownership operation (it is not used to create the new row, if any is
     *                required
     * @return an object indicating the status of the own function result
     * @throws MDBCServiceException
     */
    public OwnershipReturn own(MusicInterface mi, List<Range> ranges,
            DatabasePartition currPartition, UUID opId, SQLOperationType lockType) throws MDBCServiceException {
        
        if (ranges == null || ranges.isEmpty()) {
              return null;
        }

        //Init timeout clock
        startOwnershipTimeoutClock(opId);
        if (currPartition.isLocked()&&currPartition.getSnapshot().containsAll(ranges)) {
            return new OwnershipReturn(opId,currPartition.getLockId(),currPartition.getMRIIndex(),
                    currPartition.getSnapshot(),null);
        }
        //Find
        Map<UUID,LockResult> newLocks = new HashMap<>();
        List<Range> rangesToOwn = mi.getRangeDependencies(ranges);
        List<MusicRangeInformationRow> rangesToOwnRows = extractRowsForRange(mi,rangesToOwn, false);
        Dag toOwn =  Dag.getDag(rangesToOwnRows,rangesToOwn);
        Dag currentlyOwn = new Dag();
        while ( (toOwn.isDifferent(currentlyOwn) || !currentlyOwn.isOwned() ) &&
                !timeout(opId)
            ) {
            takeOwnershipOfDag(mi, currPartition, opId, newLocks, toOwn, lockType);
            currentlyOwn=toOwn;
            //TODO instead of comparing dags, compare rows
            rangesToOwnRows = extractRowsForRange(mi, rangesToOwn, false);
            toOwn =  Dag.getDag(rangesToOwnRows,rangesToOwn);
        }
        if (!currentlyOwn.isOwned() || toOwn.isDifferent(currentlyOwn)) {
            //hold on to previous partition
            newLocks.remove(currPartition.getMRIIndex());
            mi.releaseLocks(newLocks);
            stopOwnershipTimeoutClock(opId);
            logger.error("Error when owning a range: Timeout");
            throw new MDBCServiceException("Ownership timeout");
        }
        Set<Range> allRanges = currentlyOwn.getAllRanges();
        List<MusicRangeInformationRow> latestRows = extractRowsForRange(mi, new ArrayList<>(allRanges), true);
        currentlyOwn.setRowsPerLatestRange(getIsLatestPerRange(toOwn,latestRows));
        return mi.mergeLatestRowsIfNecessary(currentlyOwn,latestRows,ranges,newLocks,opId);
    }
    
    /**
     * Step through dag and take lock ownership of each range
     * @param partition current partition owned by system
     * @param opId
     * @param newLocks
     * @param toOwn
     * @param lockType 
     * @throws MDBCServiceException
     */
    private void takeOwnershipOfDag(MusicInterface mi, DatabasePartition partition, UUID opId,
            Map<UUID, LockResult> newLocks, Dag toOwn, SQLOperationType lockType) throws MDBCServiceException {
        
        while(toOwn.hasNextToOwn()){
            DagNode node = toOwn.nextToOwn();
            MusicRangeInformationRow row = node.getRow();
            UUID uuidToOwn = row.getPartitionIndex();
            if (partition.isLocked() && partition.getMRIIndex().equals(uuidToOwn) ) {
                toOwn.setOwn(node);
                newLocks.put(uuidToOwn, new LockResult(true, uuidToOwn, partition.getLockId(),
                        false, partition.getSnapshot()));
            } else if ( newLocks.containsKey(uuidToOwn) || !row.getIsLatest() ) {
                toOwn.setOwn(node);
            } else {
                LockRequest request = new LockRequest(uuidToOwn,
                        new ArrayList<>(node.getRangeSet()), lockType);
                String lockId = mi.createLock(request);
                LockResult result = null;
                boolean owned = false;
                while(!owned && !timeout(opId)){
                    try {
                        result = mi.acquireLock(request, lockId);
                        if (result.wasSuccessful()) {
                            owned = true;
                            continue;
                        }
                        //backOff
                        try {
                            Thread.sleep(result.getBackOffPeriod());
                        } catch (InterruptedException e) {
                            continue;
                        }
                        request.incrementAttempts();
                    }
                    catch (MDBCServiceException e){
                        logger.warn("Locking failed, retrying",e);
                    }
                }
                // TODO look into updating the partition object with the latest lockId; 
                if(owned){
                    toOwn.setOwn(node);
                    newLocks.put(uuidToOwn,result);
                }
                else{
                    mi.relinquish(lockId,uuidToOwn.toString());
                    break;
                }
            }
        }
    }
    
    
    public void reloadAlreadyApplied(DatabasePartition partition) throws MDBCServiceException {
        List<Range> snapshot = partition.getSnapshot();
        UUID row = partition.getMRIIndex();
        for(Range r : snapshot){
            alreadyApplied.put(r,Pair.of(new MriReference(row),-1));
        }
    }
    
    // \TODO merge with dag code
    private Map<Range,Set<DagNode>> getIsLatestPerRange(Dag dag, List<MusicRangeInformationRow> rows) throws MDBCServiceException {
        Map<Range,Set<DagNode>> rowsPerLatestRange = new HashMap<>();
        for(MusicRangeInformationRow row : rows){
            DatabasePartition dbPartition = row.getDBPartition();
            if (row.getIsLatest()) {
                for(Range range : dbPartition.getSnapshot()){
                    if(!rowsPerLatestRange.containsKey(range)){
                        rowsPerLatestRange.put(range,new HashSet<>());
                    }
                    DagNode node = dag.getNode(row.getPartitionIndex());
                    if(node!=null) {
                        rowsPerLatestRange.get(range).add(node);
                    }
                    else{
                        rowsPerLatestRange.get(range).add(new DagNode(row));
                    }
                }
            }
        }
        return rowsPerLatestRange;
    }

            
    public Map<Range, Pair<MriReference, Integer>> getAlreadyApplied() {
        return this.alreadyApplied;
    } 
    
}
