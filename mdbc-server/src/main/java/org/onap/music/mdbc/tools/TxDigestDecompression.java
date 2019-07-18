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
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.mixins.MusicInterface;
import org.onap.music.mdbc.mixins.MusicMixin;
import org.onap.music.mdbc.mixins.MySQLMixin;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.onap.music.mdbc.tables.MusicTxDigestId;
import org.onap.music.mdbc.tables.Operation;
import org.onap.music.mdbc.tables.StagingTable;

/**
 * This function outputs the tx digest, decompressing the information and making it human readable.
 * It is intended to help debug and allow users to see what is happening inside the tx digest.
 *
 */

public class TxDigestDecompression {
    public static final EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(TxDigestDecompression.class);
    MusicMixin mi;
    MySQLMixin ms;
    
    public TxDigestDecompression(MusicInterface _mi) {
        mi = (MusicMixin) _mi;
        ms = new MySQLMixin();
    }
    
    public TxDigestDecompression() {
        Properties prop = new Properties();
        try {
            prop.load(this.getClass().getClassLoader().getResourceAsStream("music.properties"));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        try {
            mi = new MusicMixin(null, "mdbcservername", prop);
            ms = new MySQLMixin();
        } catch (MDBCServiceException e) {
            e.printStackTrace();
            return;
        }
    }
    
    public void decodeTxDigest() {
        // Print out the tx digest
        try {
            List<MusicRangeInformationRow> rows = mi.getAllMriRows();
            for (MusicRangeInformationRow row: rows) {
                UUID mriId = row.getPartitionIndex();
                extractedRedoLog(row);
            }
        } catch (MDBCServiceException e) {
            e.printStackTrace();
            return;
        }
        System.exit(0);
    }

    public void extractedRedoLog(MusicRangeInformationRow row) throws MDBCServiceException {
        for (MusicTxDigestId id: row.getRedoLog()) {
            StagingTable st = mi.getTxDigest(id);
            System.out.print(id.transactionId + ": [");
            String sep = ", ";
            for (Operation op: st.getOperationList()) {
                
                ArrayList<String> cols = new ArrayList<String>();
                ArrayList<Object> vals = new ArrayList<Object>();
                ms.constructColValues(op.getVal(), cols, vals);
                StringBuilder sql = ms.constructSQL(op, cols, vals);
                
                System.out.print(sql + sep);
                
            }
            System.out.println("]");
        }
    }
    
    public static void main(String[] args) {
        TxDigestDecompression txDecompress = new TxDigestDecompression();
        txDecompress.decodeTxDigest();
    }
}
