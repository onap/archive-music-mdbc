package com.att.research.mdbc.examples;

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
