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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery, null));
    }

    @Test
    public void selectQuery() throws SQLException {
        String sqlQuery = "SELECT name, age FROM DB.table1 t1;";
        HashMap<String, List<SQLOperation>> expectedOut = new HashMap<>();
        List<SQLOperation> t1op = new ArrayList<>();
        t1op.add(SQLOperation.SELECT);
        expectedOut.put("DB.TABLE1", t1op);
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery, null));
    }
    
    @Test
    public void selectQuery1() throws SQLException {
        String sqlQuery = "SELECT name, age FROM DB.table1;";
        HashMap<String, List<SQLOperation>> expectedOut = new HashMap<>();
        List<SQLOperation> t1op = new ArrayList<>();
        t1op.add(SQLOperation.SELECT);
        expectedOut.put("DB.TABLE1", t1op);
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery, null));
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
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery, null));

        sqlQuery = "SELECT name, age FROM table1, table2 t2 WHERE id = t2.id";
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery, null));
    }

    @Test
    public void insertQuery() throws SQLException {
        String sqlQuery = "INSERT INTO Employees (id, name) values ('1','Vikram')";
        HashMap<String, List<SQLOperation>> expectedOut = new HashMap<>();
        List<SQLOperation> t1op = new ArrayList<>();
        t1op.add(SQLOperation.INSERT);
        expectedOut.put("EMPLOYEES", t1op);
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery, null));
    }

    @Test
    public void updateQuery() throws SQLException {
        String sqlQuery = "UPDATE Db.Employees SET id = 1 WHERE id = 2";
        HashMap<String, List<SQLOperation>> expectedOut = new HashMap<>();
        List<SQLOperation> t1op = new ArrayList<>();
        t1op.add(SQLOperation.UPDATE);
        expectedOut.put("DB.EMPLOYEES", t1op);
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery, null));

        sqlQuery = "UPDATE db.Employees SET id = 1";
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery, null));
    }
    
    @Test
    public void deleteQuery() throws SQLException {
        String sqlQuery = "delete from db.employees where personid = 721 and lastname = 'Lastname'";
        HashMap<String, List<SQLOperation>> expectedOut = new HashMap<>();
        List<SQLOperation> t1op = new ArrayList<>();
        t1op.add(SQLOperation.DELETE);
        expectedOut.put("DB.EMPLOYEES", t1op);
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery, null));
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
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery, null));
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
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(sqlQuery, null));
    }
    
    @Test
    public void userDefinedVariables() throws SQLException {
        String query = "SELECT @start := 1, @finish := 10;";
        HashMap<String, List<SQLOperation>> expectedOut = new HashMap<>();
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(query, new HashSet<String>()));
    }
    
    @Test
    public void userDefinedVariables1() throws SQLException {
        String query = "select @rn /*'*/:=/*'*/ @rn+1 AS rowId, notification_ID, is_for_online_users,is_for_all_roles, msg_header, msg_description,msg_source, start_Time, end_time, priority, created_date, creator_ID,notification_hyperlink, active_YN from (   select notification_ID, is_for_online_users, is_for_all_roles, msg_header, msg_description, msg_source,start_Time, end_time, priority,created_date, creator_ID,notification_hyperlink,active_YN from ( select user_id, notification_id, is_for_online_users, is_for_all_roles, msg_header, msg_description,msg_source,start_Time, end_time, priority, created_date,notification_hyperlink, creator_ID,active_YN from ( select a.notification_ID,a.is_for_online_users,a.is_for_all_roles,a.active_YN, a.msg_header,a.msg_description,a.msg_source,a.start_time,a.end_time,a.priority,a.creator_ID,a.notification_hyperlink,a.created_date,b.role_id,b.recv_user_id  from ep_notification a, ep_role_notification b where a.notification_id = b.notification_id and (end_time is null || SYSDATE() <= end_time ) and (start_time is null || SYSDATE() >= start_time) and a.is_for_all_roles = 'N' ) a, ( select distinct a.user_id, c.role_id, c.app_id, d.APP_NAME from fn_user a, fn_user_role b, fn_role c, fn_app d where COALESCE(c.app_id,1) = d.app_id and a.user_id = b.user_id and a.user_id = ? and b.role_id = c.role_id and (d.enabled='Y' or d.app_id=1) )b where ( a.role_id = b.role_id ) union select ?, notification_id, is_for_online_users, is_for_all_roles, msg_header, msg_description,msg_source,start_Time, end_time, priority, created_date,notification_hyperlink, creator_ID,active_YN from ( select a.notification_ID,a.is_for_online_users,a.is_for_all_roles,a.active_YN, a.msg_header,a.msg_description,a.msg_source,a.start_time,a.end_time,a.priority,a.creator_ID,a.created_date, a.notification_hyperlink,b.role_id,b.recv_user_id  from ep_notification a, ep_role_notification b where a.notification_id = b.notification_id and (end_time is null || SYSDATE() <= end_time ) and (start_time is null || SYSDATE() >= start_time) and a.is_for_all_roles = 'N' ) a where ( a.recv_user_id=? ) union ( select ? user_id, notification_id, is_for_online_users, is_for_all_roles, msg_header, msg_description, msg_source,start_Time, end_time, priority, created_date,notification_hyperlink, creator_ID,active_YN from ep_notification a where a.notification_id and (end_time is null || SYSDATE() <= end_time ) and (start_time is null || SYSDATE() >= start_time) and a.is_for_all_roles = 'Y' ) ) a where active_YN = 'Y' and not exists ( select ID,User_ID,notification_ID,is_viewed,updated_time from ep_user_notification m where user_id = ? and m.notification_id = a.notification_id and is_viewed = 'Y' ) order by priority desc, created_date desc,start_Time desc   ) t, (SELECT @rn /*'*/:=/*'*/ 0) t2 ;";
        HashMap<String, List<SQLOperation>> expectedOut = new HashMap<>();
        Set<String> tables = new HashSet<>();
        tables.add("ep_notification");
        tables.add("ep_role_notification");
        tables.add("fn_user");
        tables.add("fn_user_role");
        tables.add("fn_role");
        tables.add("fn_app");
        tables.add("test_table");
        
        //all reads for this query
        for (String table: tables) {
            if (table.equals("test_table")) {
                continue;
            }
            List<SQLOperation> tableList = new ArrayList<>();
            tableList.add(SQLOperation.SELECT);
            expectedOut.put(table, tableList);
        }
        
        assertEquals(expectedOut, QueryProcessor.parseSqlQuery(query, tables));
    }
}
