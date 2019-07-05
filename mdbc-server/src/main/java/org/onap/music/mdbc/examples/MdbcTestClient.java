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
import java.util.Scanner;
import org.apache.calcite.avatica.remote.Driver;

public class MdbcTestClient {


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
            String metricURL = "http://localhost:30000/test"; 
            if (args.length>0 && args[0] != null) { 
                metricURL = args[0]; 
            } 
            connection = DriverManager.getConnection("jdbc:avatica:remote:url=" + metricURL+ ";serialization=protobuf"); 
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
                "    City varchar(255),\n" +
                "    PRIMARY KEY (PersonID,LastName)" +
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

        final String insertSQL = "INSERT INTO Persons VALUES (1, 'Smith', 'Juan', 'KACB', 'ATLANTA');";
        final String insertSQL1 = "INSERT INTO Persons2 VALUES (1, 'Smith', 'Juan', 'KACB', 'ATLANTA');";
        final String insertSQL2 = "INSERT INTO Persons3 VALUES (2, 'Smith', 'JOHN', 'GNOC', 'BEDMINSTER');";
        final String insertSQL3 = "UPDATE Persons SET FirstName='JOSH' WHERE LastName='Smith';";
        final String insertSQL4 = "UPDATE Persons2 SET FirstName='JOHN' WHERE LastName='Smith';";
        final String insertSQL5 = "UPDATE Persons SET FirstName='JOHN' WHERE LastName='Smith';";
        final String insertSQL6 = "UPDATE Persons3 SET FirstName='JOHN' WHERE LastName='Smith';";
        
        final String selectSQL1 = "SELECT * FROM Persons;";

        Statement insertStmt;
        try {
            insertStmt = connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        try {
            /*
             * insert into 1
             * insert into 2
             * insert into 3
             * insert into 1,2
             * insert into 1,3
             */
            System.out.println("1");
            execute = insertStmt.execute(insertSQL);
            connection.commit();
            
            connection.close();
            connection = DriverManager.getConnection("jdbc:avatica:remote:url=" + "http://localhost:30000/test"+ ";serialization=protobuf");
            connection.setAutoCommit(false);
            insertStmt = connection.createStatement();
            
            System.out.println("2");
            execute = insertStmt.execute(insertSQL1);
            connection.commit();
            
            connection.close();
            connection = DriverManager.getConnection("jdbc:avatica:remote:url=" + "http://localhost:30000/test"+ ";serialization=protobuf");
            connection.setAutoCommit(false);
            insertStmt = connection.createStatement();
            
            System.out.println("3");
            execute = insertStmt.execute(insertSQL2);
            connection.commit();
            
            connection.close();
            connection = DriverManager.getConnection("jdbc:avatica:remote:url=" + "http://localhost:30000/test"+ ";serialization=protobuf");
            connection.setAutoCommit(false);
            insertStmt = connection.createStatement();
            
            System.out.println("1,2");
            execute = insertStmt.execute(insertSQL3);
            execute = insertStmt.execute(insertSQL4);
            connection.commit();
            
            connection.close();
            connection = DriverManager.getConnection("jdbc:avatica:remote:url=" + "http://localhost:30000/test"+ ";serialization=protobuf");
            connection.setAutoCommit(false);
            insertStmt = connection.createStatement();
            
            System.out.println("1,3");
            
            execute = insertStmt.execute(insertSQL5);
            execute = insertStmt.execute(insertSQL6);
            //execute = insertStmt.execute(insertSQL4);
            connection.commit();
            
            /*
            ResultSet rs = insertStmt.executeQuery(selectSQL1);
            while (rs.next()) {
                System.out.printf("%d, %s, %s\n", rs.getInt("PersonID"), rs.getString("FirstName"), rs.getString("LastName"));
            }
            //pause for user input
            Scanner scanner = new Scanner(System.in);
            System.out.println("Please hit <Enter> to complete the transaction and continue");
            String line = scanner.nextLine();
            scanner.close();
            //*/

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
