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
import org.apache.commons.lang3.tuple.Pair;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.exceptions.MusicDeadlockException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.Utils;
import org.onap.music.mdbc.mixins.DBInterface;
import org.onap.music.mdbc.mixins.LockRequest;
import org.onap.music.mdbc.mixins.LockResult;
import org.onap.music.mdbc.mixins.MusicInterface;
import org.onap.music.mdbc.mixins.MusicInterface.OwnershipReturn;
import org.onap.music.mdbc.query.SQLOperationType;
import org.onap.music.mdbc.tables.MriReference;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.MusicTxDigestId;
import org.onap.music.mdbc.tables.StagingTable;

public class OwnershipAndCheckpoint{

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(OwnershipAndCheckpoint.class);
    private Lock checkpointLock;
    private Map<Range, Pair<MriReference, MusicTxDigestId>> alreadyApplied;
    private Map<UUID,Long> ownershipBeginTime;
    private long timeoutInMs;

    public OwnershipAndCheckpoint(){
      this(new HashMap<>(),Long.MAX_VALUE);
    }

    public OwnershipAndCheckpoint(Map<Range, Pair<MriReference, MusicTxDigestId>> alreadyApplied, long timeoutInMs){
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

    
    private List<MusicRangeInformationRow> extractRowsForRange(List<MusicRangeInformationRow> allMriRows, Set<Range> ranges,
                                                  boolean onlyIsLatest){
        List<MusicRangeInformationRow> rows = new ArrayList<>();
        for(MusicRangeInformationRow row : allMriRows){
            if(onlyIsLatest && !row.getIsLatest()){
                continue;
            }
            final Set<Range> rowRanges = row.getDBPartition().getSnapshot();
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
    public List<MusicRangeInformationRow> extractRowsForRange(MusicInterface music, Set<Range> ranges, boolean onlyIsLatest)
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
     * @param ownOpId
     * @throws MDBCServiceException
     */
    public void checkpoint(MusicInterface mi, DBInterface di, Dag extendedDag, Set<Range> ranges, UUID ownOpId)
            throws MDBCServiceException {
        if(ranges.isEmpty()){
            return;
        }
        try {
            checkpointLock.lock();
            extendedDag.setAlreadyApplied(alreadyApplied, ranges);
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

    private void disableForeignKeys(DBInterface dbi) throws MDBCServiceException {
        try {
            dbi.disableForeignKeyChecks();
        } catch (SQLException e) {
            throw new MDBCServiceException("Error disable foreign keys checks",e);
        }
    }

    private void applyTxDigest(DBInterface dbi, StagingTable txDigest)
        throws MDBCServiceException {
        try {
            dbi.applyTxDigest(txDigest);
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
    public void warmup(MusicInterface mi, DBInterface di, Set<Range> rangesToWarmup) throws MDBCServiceException {
        if(rangesToWarmup.isEmpty()){
            return;
        }
        Dag dag = new Dag(false);
        final List<MusicRangeInformationRow> rows = extractRowsForRange(mi, rangesToWarmup,false);
        dag = Dag.getDag(rows,rangesToWarmup);
        dag.setAlreadyApplied(alreadyApplied, rangesToWarmup);
        while(!dag.applied()){
            DagNode node = dag.nextToApply(rangesToWarmup);
            if(node!=null) {
                Pair<MusicTxDigestId, Set<Range>> pair = node.nextNotAppliedTransaction(rangesToWarmup);
                while (pair != null) {
                    checkpointLock.lock();
                    try {
                        disableForeignKeys(di);
                        applyDigestAndUpdateDataStructures(mi, di, node, pair.getLeft(), pair.getRight());
                        pair = node.nextNotAppliedTransaction(rangesToWarmup);
                        enableForeignKeys(di);
                    } catch (MDBCServiceException e) {
                        checkpointLock.unlock();
                        throw e;
                    }
                    checkpointLock.unlock();
                }
            }
        }
    }

    /**
     * Apply tx digest for dagnode update checkpoint location (alreadyApplied)
     * @param mi
     * @param di
     * @param node
     * @param pair
     * @throws MDBCServiceException
     */
    private void applyDigestAndUpdateDataStructures(MusicInterface mi, DBInterface dbi, DagNode node,
            MusicTxDigestId digestId, Set<Range> ranges) throws MDBCServiceException {
        if (alreadyReplayed(node, digestId)) {
            return;
        }

        final StagingTable txDigest;
        try {
            txDigest = mi.getTxDigest(digestId);
        } catch (MDBCServiceException e) {
            logger.warn("Transaction digest was not found, this could be caused by a failure of the previous owner"
                +"And would normally only happen as the last ID of the corresponding redo log. Please check that this is the"
                +" case for txID "+digestId.transactionId.toString());
            return;
        }
        applyTxDigest(dbi, txDigest);
        MusicRangeInformationRow row = node.getRow();
        updateAlreadyApplied(mi, dbi, ranges, row.getPartitionIndex(), digestId);
    }
    
    /**
     * Determine if this musictxdigest id has already been replayed
     * @param node
     * @param redoLogIndex
     * @return true if alreadyApplied is past this node/redolog, false if it hasn't been replayed
     */
    public boolean alreadyReplayed(DagNode node, MusicTxDigestId txdigest) {
        int index = node.getRow().getRedoLog().indexOf(txdigest);
        for (Range range: node.getRangeSet()) {
            Pair<MriReference, MusicTxDigestId> applied = alreadyApplied.get(range);
            if (applied==null) {
                return false;
            }
            MriReference appliedMriRef = applied.getLeft();
            MusicTxDigestId appliedDigest = applied.getRight();
            appliedDigest.index = node.getRow().getRedoLog().indexOf(appliedDigest);
            if (appliedMriRef==null || appliedMriRef.getTimestamp() < node.getTimestamp()
                    || (appliedMriRef.getTimestamp() == node.getTimestamp()
                            && appliedDigest.index < index)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Update external checkpoint markers in sql db and music
     * @param mi
     * @param di
     * @param r
     * @param mriRef
     * @param index
     * @throws MDBCServiceException 
     */
    private void updateCheckpointLocations(MusicInterface mi, DBInterface dbi, Range r, MriReference mriRef, MusicTxDigestId txdigest) {
        dbi.updateCheckpointLocations(r, Pair.of(mriRef, txdigest));
        mi.updateCheckpointLocations(r, Pair.of(mriRef, txdigest));
    }

    /**
     * Forceably apply changes in tx digest for ranges
     * @param mi
     * @param db
     * @param extendedDag
     * @param ranges
     * @param ownOpId
     * @throws MDBCServiceException
     */
    private void applyRequiredChanges(MusicInterface mi, DBInterface db, Dag extendedDag, Set<Range> ranges, UUID ownOpId)
        throws MDBCServiceException {
        disableForeignKeys(db);
        while(!extendedDag.applied()){
            DagNode node = extendedDag.nextToApply(ranges);
            if(node!=null) {
                Pair<MusicTxDigestId, Set<Range>> pair = node.nextNotAppliedTransaction(ranges);
                while (pair != null) {
                    applyDigestAndUpdateDataStructures(mi, db, node, pair.getLeft(), pair.getRight());
                    pair = node.nextNotAppliedTransaction(ranges);
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
    public OwnershipReturn own(MusicInterface mi, Set<Range> ranges,
            DatabasePartition currPartition, UUID opId, SQLOperationType lockType) throws MDBCServiceException {
        return own(mi, ranges, currPartition, opId, lockType, null);
    }

    public OwnershipReturn own(MusicInterface mi, Set<Range> ranges,
            DatabasePartition currPartition, UUID opId, SQLOperationType lockType, String ownerId) throws MDBCServiceException {
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
        Map<UUID,LockResult> locksForOwnership = new HashMap<>();
        Set<Range> rangesToOwn = mi.getRangeDependencies(ranges);
        List<MusicRangeInformationRow> rangesToOwnRows = extractRowsForRange(mi,rangesToOwn, false);
        Dag toOwn =  Dag.getDag(rangesToOwnRows,rangesToOwn);
        Dag currentlyOwn = new Dag();
        while ( (toOwn.isDifferent(currentlyOwn) || !currentlyOwn.isOwned() ) &&
                !timeout(opId)
            ) {
            try {
                takeOwnershipOfDag(mi, currPartition, opId, locksForOwnership, toOwn, lockType, ownerId);
            } catch (MDBCServiceException e) {
                MusicDeadlockException de = Utils.getDeadlockException(e);
                if (de!=null) {
                    locksForOwnership.remove(currPartition.getMRIIndex());
                    mi.releaseLocks(locksForOwnership);
                    stopOwnershipTimeoutClock(opId);
                    logger.error("Error when owning a range: Deadlock detected");
                }
                throw e;
            }
            currentlyOwn=toOwn;
            //TODO instead of comparing dags, compare rows
            rangesToOwnRows = extractRowsForRange(mi, rangesToOwn, false);
            toOwn =  Dag.getDag(rangesToOwnRows,rangesToOwn);
        }
        if (!currentlyOwn.isOwned() || toOwn.isDifferent(currentlyOwn)) {
            //hold on to previous partition
            locksForOwnership.remove(currPartition.getMRIIndex());
            mi.releaseLocks(locksForOwnership);
            stopOwnershipTimeoutClock(opId);
            logger.error("Error when owning a range: Timeout");
            throw new MDBCServiceException("Ownership timeout");
        }
        Set<Range> allRanges = currentlyOwn.getAllRanges();
        //TODO: we shouldn't need to go back to music at this point
        List<MusicRangeInformationRow> latestRows = extractRowsForRange(mi, allRanges, true);
        currentlyOwn.setRowsPerLatestRange(getIsLatestPerRange(toOwn,latestRows));
        return mi.mergeLatestRowsIfNecessary(currentlyOwn,locksForOwnership,opId, ownerId);
    }
   
    /**
     * Step through dag and take lock ownership of each range
     * @param partition current partition owned by system
     * @param opId
     * @param ownershipLocks
     * @param toOwn
     * @param lockType 
     * @throws MDBCServiceException
     */
    private void takeOwnershipOfDag(MusicInterface mi, DatabasePartition partition, UUID opId,
            Map<UUID, LockResult> ownershipLocks, Dag toOwn, SQLOperationType lockType, String ownerId)
            throws MDBCServiceException {
        
        while(toOwn.hasNextToOwn()){
            DagNode node = toOwn.nextToOwn();
            MusicRangeInformationRow row = node.getRow();
            UUID uuidToOwn = row.getPartitionIndex();
            if (partition.isLocked() && partition.getMRIIndex().equals(uuidToOwn) ) {
                toOwn.setOwn(node);
                ownershipLocks.put(uuidToOwn, new LockResult(true, uuidToOwn, partition.getLockId(),
                        false, partition.getSnapshot()));
            } else if ( ownershipLocks.containsKey(uuidToOwn) || !row.getIsLatest() ) {
                toOwn.setOwn(node);
                if (ownershipLocks.containsKey(uuidToOwn) && !row.getIsLatest()) {
                    //previously owned partition that is no longer latest, don't need anymore
                    LockResult result = ownershipLocks.get(uuidToOwn);
                    ownershipLocks.remove(uuidToOwn);
                    mi.relinquish(result.getLockId(), uuidToOwn.toString());
                }
            } else {
                LockRequest request = new LockRequest(uuidToOwn,
                        new ArrayList<>(node.getRangeSet()), lockType);
                String lockId = mi.createLock(request, ownerId);
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
                    ownershipLocks.put(uuidToOwn,result);
                }
                else{
                    mi.relinquish(lockId,uuidToOwn.toString());
                    break;
                }
            }
        }
    }
    
    public String getDebugInfo(MusicInterface mi, String rangesStr) {
        
        Set<Range> ranges = new HashSet<>();
        Arrays.stream(rangesStr.split(",")).forEach(a -> ranges.add(new Range(a)));
        
        StringBuffer buffer = new StringBuffer();
        Set<Range> rangesToOwn;
        try {
            rangesToOwn = mi.getRangeDependencies(ranges);
            List<MusicRangeInformationRow> rangesToOwnRows = extractRowsForRange(mi,rangesToOwn, false);
            Dag toOwn =  Dag.getDag(rangesToOwnRows,rangesToOwn);
            while(toOwn.hasNextToOwn()){
                DagNode node = null;
                try {
                    node = toOwn.nextToOwn();
                    MusicRangeInformationRow row = node.getRow();
          
                    buffer.append("\n-------------\n");
                    buffer.append(row.getDBPartition()).append(",");
                    buffer.append(row.getPrevRowIndexes()).append(",");
                    buffer.append(row.getIsLatest()).append("");
                    
                    
                } catch (Exception e) {
                    buffer.append("\n------missing MRI------\n");
                } finally {
                    
                    if(node != null) {
                        toOwn.setOwn(node);
                    }
                    
                }
                 
            }
            

        } catch (MDBCServiceException e) {
            buffer.setLength(0);
            buffer.append(" Debugging info could not be determined");
        }
        
        return buffer.toString();
        
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

            
    public Map<Range, Pair<MriReference, MusicTxDigestId>> getAlreadyApplied() {
        return this.alreadyApplied;
    }
    
    public void updateAlreadyApplied(MusicInterface mi, DBInterface dbi, Set<Range> ranges, UUID mriIndex, MusicTxDigestId digestId) {
        for (Range r: ranges) {
            updateAlreadyApplied(mi, dbi, r, mriIndex, digestId);
        }
    }
    
    public void updateAlreadyApplied(MusicInterface mi, DBInterface dbi, Range r, UUID mriIndex, MusicTxDigestId digestId) {
        MriReference mriRef = new MriReference(mriIndex);
        alreadyApplied.put(r, Pair.of(mriRef, digestId));
        updateCheckpointLocations(mi, dbi, r, mriRef, digestId);
    }

}
