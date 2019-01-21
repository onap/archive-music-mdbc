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
package org.onap.music.mdbc.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class MdbcEventualTestClient {



    public static void main(String[] args){
        try {
            Class.forName("org.apache.calcite.avatica.remote.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection connection;
        try {
            connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:30000/test;serialization=protobuf");
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        try {
        connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }


        final String sql = "CREATE TABLE IF NOT EXISTS audit_log (\n" +
                "    id int,\n" +
                "    PersonID int,\n" +
                "    timeID bigint,\n" +
                "    PRIMARY KEY (id)" +
                ");";
        Statement stmt;
        try {
            stmt = connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        boolean execute = true;
//        try {
//            execute = stmt.execute(sql);
//        } catch (SQLException e) {
//            e.printStackTrace();
//            return;
//        }

        if (execute) {
            try {
                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        try {
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        final String insertSQL = "INSERT INTO audit_log VALUES (1, 123, 123456789);";
        final String insertSQL1 = "DELETE FROM audit_log WHERE PersonID=1;";
        final String insertSQL2 = "INSERT INTO audit_log VALUES (1, 123, 123456789);";
        final String insertSQL3 = "UPDATE audit_log SET PersonID=124 where id=1;";
        final String insertSQL4 = "INSERT INTO audit_log VALUES (2, 234, 123456789);";


        Statement insertStmt;
        try {
            insertStmt = connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        try {
            execute = insertStmt.execute(insertSQL);
            execute = insertStmt.execute(insertSQL1);
            execute = insertStmt.execute(insertSQL2);
            execute = insertStmt.execute(insertSQL3);
            execute = insertStmt.execute(insertSQL4);

        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        try {
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        try {
            stmt.close();
            insertStmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
