package com.att.research.mdbc;

import com.att.research.mdbc.mixins.OperationType;
import com.att.research.mdbc.mixins.StagingTable;
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
        table.addOperation("test",OperationType.INSERT,(new JSONObject(new String[]{"test1", "test2"})).toString(),(new JSONObject(new String[]{"test3", "Test4"})).toString());
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
        table.addOperation("test",OperationType.INSERT,(new JSONObject(new String[]{"test1", "test2"})).toString(),(new JSONObject(new String[]{"test3", "Test4"})).toString());
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