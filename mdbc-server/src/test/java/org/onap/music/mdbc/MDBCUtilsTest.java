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

import org.onap.music.mdbc.tables.OperationType;
import org.onap.music.mdbc.tables.StagingTable;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.*;

public class MDBCUtilsTest {

        @Test
    public void toStringTest1() {
        StagingTable table = new StagingTable();
        table.addOperation("test",OperationType.INSERT,(new JSONObject(new String[]{"test3", "Test4"})).toString());
        String output=null;
        try {
            output = MDBCUtils.toString(table);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        assertTrue(output!=null);
        assertTrue(!output.isEmpty());
    }

    @Test
    public void toStringTest2() {
        HashMap<String,StagingTable> mapToSerialize = new HashMap<>();
        StagingTable table = new StagingTable();
        table.addOperation("test",OperationType.INSERT,(new JSONObject(new String[]{"test3", "Test4"})).toString());
        mapToSerialize.put("table",table);
        String output=null;
        try {
            output = MDBCUtils.toString(mapToSerialize);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        assertTrue(output!=null);
        assertTrue(!output.isEmpty());
    }

    @Test
    public void toStringTest3() {
        String testStr = "test";
        OperationType typeTest = OperationType.INSERT;
        String output=null;
        try {
            output = MDBCUtils.toString(testStr);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        assertTrue(output!=null);
        assertTrue(!output.isEmpty());
        output=null;
        try {
            output = MDBCUtils.toString(typeTest);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
        assertTrue(output!=null);
        assertTrue(!output.isEmpty());
    }
}
