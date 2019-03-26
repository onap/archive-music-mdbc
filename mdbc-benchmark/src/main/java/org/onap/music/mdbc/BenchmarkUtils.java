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
package org.onap.music.mdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class BenchmarkUtils {
    public static final String driver = "org.apache.calcite.avatica.remote.Driver";
    public static final String mariaDriver = "org.mariadb.jdbc.Driver";
    public static final String postgresDriver = "org.postgresql.Driver";
    public static String TABLE;
    public static String updateBuilder;

    public enum ExecutionType {
        MARIA_DB, COCKROACH_DB, METRIC,POSTGRES
    }

    public static void SetupTable(String table){
        TABLE=table;
        updateBuilder = new StringBuilder()
            .append("UPDATE ")
            .append(table)
            .append(" SET Counter = Counter + 1,")
            .append("City = 'Sandy Springs'")
            .append(";").toString();
    }

    public static void setupCreateTables(Connection connection){
        createTable(connection);
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void setupCreateRows(Connection testConnection, int rows){
        //Empty database
        boolean cleanResult = BenchmarkUtils.cleanTable(testConnection);
        //Add new lines
        BenchmarkUtils.addRowsToTable(Integer.valueOf(rows),testConnection);

        //Commit
        try {
            testConnection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static Connection getConnection(String ip, ExecutionType type, String user, String password) {
        final String connectionUrl = "jdbc:avatica:remote:url=http://" + ip + ":30000;serialization=protobuf";
        final String mariaConnectionUrl = "jdbc:mariadb://" + ip + ":3306/test";
        final String cockroachUrl = "jdbc:postgresql://" + ip + ":26257/test";
        final String postgresUrl = "jdbc:postgresql://" + ip + ":5432/test";

        try {
            switch (type) {
                case MARIA_DB:
                    Class.forName(mariaDriver);
                    break;
                case METRIC:
                    Class.forName(driver);
                    break;
                case COCKROACH_DB:
                case POSTGRES:
                    Class.forName(postgresDriver);
                    break;
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection connection = null;
        try {
            if (type == ExecutionType.METRIC) {
                connection = DriverManager.getConnection(connectionUrl);
            } else {
                Properties connectionProps = new Properties();
                connectionProps.put("user", user);
                if (type == ExecutionType.COCKROACH_DB) {
                    connectionProps.setProperty("sslmode", "disable");
                } else {
                    connectionProps.put("password", password);
                }
                final String url = (type == ExecutionType.MARIA_DB) ? mariaConnectionUrl :
                    (type == ExecutionType.COCKROACH_DB)?cockroachUrl:postgresUrl;
                connection = DriverManager.getConnection(url, connectionProps);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return connection;
    }

    public static void createTable(Connection connection) {
        final String sql = "CREATE TABLE IF NOT EXISTS "+TABLE+" (\n" +
            "    PersonID int,\n" +
            "    Counter int,\n" +
            "    LastName varchar(255),\n" +
            "    FirstName varchar(255),\n" +
            "    Address varchar(255),\n" +
            "    City varchar(255),\n" +
            "    PRIMARY KEY(PersonID)"+
            ");";

        Statement stmt = null;
        try {
            stmt = connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Boolean execute = null;
        try {
            execute = stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }

        try {
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }

        try {
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static boolean cleanTable(Connection testConnection) {
        String cleanCmd = "DELETE FROM "+TABLE+";";
        Statement stmt = null;
        try {
            stmt = testConnection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Boolean execute = null;
        try {
            execute = stmt.execute(cleanCmd);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        try {
            testConnection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        try {
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return execute;
    }

    public static void addRowsToTable(int totalNumberOfRows, Connection testConnection) {
            for (int i = 0; i < totalNumberOfRows; i++) {
                final StringBuilder insertSQLBuilder = new StringBuilder()
                    .append("INSERT INTO "+TABLE+" VALUES (")
                    .append(i)
                    .append(", ")
                    .append(0)
                    .append(", '")
                    .append("Last-")
                    .append(i)
                    .append("', '")
                    .append("First-")
                    .append(i)
                    .append("', 'KACB', 'ATLANTA');");
                Statement stmt = null;
                try {
                    stmt = testConnection.createStatement();
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.exit(1);
                }

                Boolean execute = null;
                try {
                    execute = stmt.execute(insertSQLBuilder.toString());
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
            try {
                testConnection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }


}
