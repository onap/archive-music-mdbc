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
package org.onap.music.mdbc;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;

import org.json.JSONObject;
import org.junit.Test;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.mdbc.query.SQLOperation;
import org.onap.music.mdbc.tables.StagingTable;

public class MDBCUtilsTest {

    @Test
    public void toStringTest1() {
        StagingTable table = new StagingTable();
        try {
            table.addOperation(new Range("TABLE1"),SQLOperation.INSERT,(new JSONObject(new String[]{"test3", "Test4"})).toString(),null);
        } catch (MDBCServiceException e) {
            fail();
        }
        ByteBuffer output=null;
        try {
            output = table.getSerializedStagingAndClean();
        } catch (MDBCServiceException e) {
            e.printStackTrace();
            fail();
        }
        assertTrue(output!=null);
        assertTrue(output.toString().length() > 0);
    }
}
