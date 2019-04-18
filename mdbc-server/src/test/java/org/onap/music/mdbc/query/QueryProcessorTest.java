/*
 * ============LICENSE_START==================================================== org.onap.music.mdbc
 * ============================================================================= Copyright (C) 2018 AT&T Intellectual
 * Property. All rights reserved. ============================================================================= Licensed
 * under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 * ============LICENSE_END======================================================
 */

package org.onap.music.mdbc.query;

import static org.junit.Assert.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Test;


public class QueryProcessorTest {

    @Test
    public void tableQuery() throws SQLException {
        String sqlQuery = "CREATE TABLE pet (name VARCHAR(20), owner VARCHAR(20))";
        HashMap<String, List<SQLOperation>> expectedOut = new HashMap<>();
        List<SQLOperation> op = new ArrayList<>();
        // no table ops for now
        // op.add(Operation.TABLE);
        // expectedOut.put("pet", op);
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery));
    }

    @Test
    public void selectQuery() throws SQLException {
        String sqlQuery = "SELECT name, age FROM DB.table1 t1;";
        HashMap<String, List<SQLOperation>> expectedOut = new HashMap<>();
        List<SQLOperation> t1op = new ArrayList<>();
        t1op.add(SQLOperation.SELECT);
        expectedOut.put("DB.TABLE1", t1op);
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery));
    }
    
    @Test
    public void selectQuery1() throws SQLException {
        String sqlQuery = "SELECT name, age FROM DB.table1;";
        HashMap<String, List<SQLOperation>> expectedOut = new HashMap<>();
        List<SQLOperation> t1op = new ArrayList<>();
        t1op.add(SQLOperation.SELECT);
        expectedOut.put("DB.TABLE1", t1op);
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery));
    }

    @Test
    public void selectQuery2Table() throws SQLException {
        String sqlQuery = "SELECT name, age FROM table1 t1, table2 t2 WHERE t1.id = t2.id";
        HashMap<String, List<SQLOperation>> expectedOut = new HashMap<>();
        List<SQLOperation> t1op = new ArrayList<>();
        List<SQLOperation> t2op = new ArrayList<>();
        t1op.add(SQLOperation.SELECT);
        t2op.add(SQLOperation.SELECT);
        expectedOut.put("TABLE1", t1op);
        expectedOut.put("TABLE2", t2op);
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery));

        sqlQuery = "SELECT name, age FROM table1, table2 t2 WHERE id = t2.id";
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery));
    }

    @Test
    public void insertQuery() throws SQLException {
        String sqlQuery = "INSERT INTO Employees (id, name) values ('1','Vikram')";
        HashMap<String, List<SQLOperation>> expectedOut = new HashMap<>();
        List<SQLOperation> t1op = new ArrayList<>();
        t1op.add(SQLOperation.INSERT);
        expectedOut.put("EMPLOYEES", t1op);
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery));
    }

    @Test
    public void updateQuery() throws SQLException {
        String sqlQuery = "UPDATE Db.Employees SET id = 1 WHERE id = 2";
        HashMap<String, List<SQLOperation>> expectedOut = new HashMap<>();
        List<SQLOperation> t1op = new ArrayList<>();
        t1op.add(SQLOperation.UPDATE);
        expectedOut.put("DB.EMPLOYEES", t1op);
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery));

        sqlQuery = "UPDATE db.Employees SET id = 1";
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery));
    }

    @Test
    public void insertSelect() throws SQLException {
        String sqlQuery =
                "INSERT INTO table1 (CustomerName, City, Country) SELECT SupplierName, City, Country FROM table2";
        HashMap<String, List<SQLOperation>> expectedOut = new HashMap<>();
        List<SQLOperation> t1op = new ArrayList<>();
        List<SQLOperation> t2op = new ArrayList<>();
        t1op.add(SQLOperation.INSERT);
        t2op.add(SQLOperation.SELECT);
        expectedOut.put("TABLE1", t1op);
        expectedOut.put("TABLE2", t2op);
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery));
    }
    
    @Test
    public void selectJoin() throws SQLException {
        String sqlQuery =
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate " + 
                "FROM Orders " + 
                "INNER JOIN DB.Customers ON Orders.CustomerID=Customers.CustomerID;";
        
        HashMap<String, List<SQLOperation>> expectedOut = new HashMap<>();
        List<SQLOperation> t1op = new ArrayList<>();
        List<SQLOperation> t2op = new ArrayList<>();
        t1op.add(SQLOperation.SELECT);
        t2op.add(SQLOperation.SELECT);
        expectedOut.put("ORDERS", t1op);
        expectedOut.put("DB.CUSTOMERS", t2op);
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery));
    }
}
