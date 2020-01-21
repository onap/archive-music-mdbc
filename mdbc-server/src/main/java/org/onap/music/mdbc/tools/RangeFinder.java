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

package org.onap.music.mdbc.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.StateManager;
import org.onap.music.mdbc.mixins.MusicMixin;
import org.onap.music.mdbc.ownership.Dag;
import org.onap.music.mdbc.ownership.DagNode;
import org.onap.music.mdbc.ownership.OwnershipAndCheckpoint;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.MusicTxDigestId;
import org.onap.music.mdbc.tables.Operation;
import org.onap.music.mdbc.tables.StagingTable;

/**
 * This function traces the history of a particular range
 *
 */

public class RangeFinder {
    public static final EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(RangeFinder.class);
    MusicMixin mi;
    
    public RangeFinder() {
        Properties prop = new Properties();
        try {
            prop.load(this.getClass().getClassLoader().getResourceAsStream("music.properties"));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        try {
            mi = new MusicMixin(null, "mdbcservername", prop);
        } catch (MDBCServiceException e) {
            e.printStackTrace();
            return;
        }
    }

    public void decodeRangeHistory(String rangeStr) throws MDBCServiceException {
        System.out.println("Finding history for " + rangeStr);
        Range range = new Range(rangeStr);
        Set<Range> rangeSet = new HashSet<>();
        rangeSet.add(range);
        OwnershipAndCheckpoint oac = new OwnershipAndCheckpoint();
        List<MusicRangeInformationRow> rowsForRange = oac.extractRowsForRange(this.mi, rangeSet, false);
        Dag dag = Dag.getDag(rowsForRange,rangeSet);
        int count = 0;
        while (dag.hasNextToOwn()) {
            DagNode dagnode = dag.nextToOwn();
            MusicRangeInformationRow row = dagnode.getRow();
            System.out.println(" " + row.getPartitionIndex());
            count = printRedoLog(row.getRedoLog(), rangeStr, count);
            dag.setOwn(dagnode);
        }
    }
    
    
    private int printRedoLog(List<MusicTxDigestId> redoLog, String rangeToPrint, int count) throws MDBCServiceException {
        for (MusicTxDigestId digestId: redoLog) {
            StagingTable st = mi.getTxDigest(digestId);
            StringBuilder sb = new StringBuilder("  " + count + " [");
            String sep = "";
            boolean toPrint = false;
            for (Operation op: st.getOperationList()) {
                if (op.getTable().equalsIgnoreCase(rangeToPrint)) {
                    sb.append(sep + op.getOperationType() + "-" + op.getTable() + "->" + op.getVal());
                    sep =", ";
                    toPrint = true;
                }
            }
            sb.append("] - " + digestId.transactionId);
            
            if (toPrint) {
                System.out.println(sb.toString());
                count++;
            }
        }
        return count;
    }

    public static void main(String[] args) throws MDBCServiceException {
        List<String> ranges = new ArrayList<>();
        for (String arg: args) {
            ranges.add(arg);
        }
        
        RangeFinder rangeFinder = new RangeFinder();
        for (String range: ranges) {
            rangeFinder.decodeRangeHistory(range);
        }
        
        System.exit(1);
    }

}
