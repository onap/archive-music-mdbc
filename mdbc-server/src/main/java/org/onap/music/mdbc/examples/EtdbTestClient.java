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

import java.sql.*;
import org.apache.calcite.avatica.remote.Driver;

public class EtdbTestClient {

    public static class Hr {
        public final Employee[] emps = {
                new Employee(100, "Bill"),
                new Employee(200, "Eric"),
                new Employee(150, "Sebastian"),
        };
    }

    public static class Employee {
        public final int empid;
        public final String name;

        public Employee(int empid, String name) {
            this.empid = empid;
            this.name = name;
        }
    }

    public static void main(String[] args){
        try {
            Class.forName("org.apache.calcite.avatica.remote.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection connection;
        try {
            connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:30000;serialization=protobuf");
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


        final String sql = "CREATE TABLE IF NOT EXISTS Persons (\n" +
                "    PersonID int,\n" +
                "    LastName varchar(255),\n" +
                "    FirstName varchar(255),\n" +
                "    Address varchar(255),\n" +
                "    City varchar(255)\n" +
                ");";
        Statement stmt;
        try {
            stmt = connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        boolean execute;
        try {
            execute = stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

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

        final String insertSQL = "INSERT INTO Persons VALUES (1, 'Martinez', 'Juan', 'KACB', 'ATLANTA');";
        Statement insertStmt;
        try {
            insertStmt = connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        try {
            execute = insertStmt.execute(insertSQL);
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
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
