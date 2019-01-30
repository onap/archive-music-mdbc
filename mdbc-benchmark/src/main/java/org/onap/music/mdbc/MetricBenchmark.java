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

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class MetricBenchmark {
    public StringBuilder updateBuilder = new StringBuilder()
        .append("UPDATE PERSONS ")
        .append("SET Counter = Counter + 1,")
        .append("City = 'Sandy Springs'")
        .append(";");
    public String update = updateBuilder.toString();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(MetricBenchmark.class.getSimpleName())
            .param("type", ExecutionType.METRIC.name())
            .build();
        new Runner(opt).run();
    }

    @Benchmark
    public boolean testMethod(MyState state) {
        Statement stmt = null;
        try {
            stmt = state.testConnection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Boolean execute = null;
        try {
            execute = stmt.execute(update);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        try {
            //TODO: check if state need to be consumed by blackhole to guarantee execution
            state.testConnection.commit();
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

    public static enum ExecutionType {
        MARIA_DB, COCKROACH_DB, METRIC
    }

    @State(Scope.Benchmark)
    public static class MyState {
        public final String driver = "org.apache.calcite.avatica.remote.Driver";
        public final String mariaDriver = "org.mariadb.jdbc.Driver";
        public final String cockroachDriver = "org.postgresql.Driver";
        final String user = "root";
        final String password = "metriccluster";
        @Param({"104.209.240.219"})
        public String ip;
        @Param({"1", "10", "50", "80", "100", "200", "300", "400"})
        public int rows;
        @Param({"MARIA_DB", "COCKROACH_DB", "METRIC"})
        public ExecutionType type;

        public Connection testConnection;


        private Connection createConnection() {
            final String connectionUrl = "jdbc:avatica:remote:url=http://" + ip + ":30000;serialization=protobuf";
            final String mariaConnectionUrl = "jdbc:mariadb://" + ip + ":3306/test";
            final String cockroachUrl = "jdbc:postgresql://" + ip + ":26257/test";

            try {
                switch (type) {
                    case MARIA_DB:
                        Class.forName(this.mariaDriver);
                        break;
                    case METRIC:
                        Class.forName(this.driver);
                        break;
                    case COCKROACH_DB:
                        Class.forName(this.cockroachDriver);
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
                    if(type == ExecutionType.COCKROACH_DB){
                        connectionProps.setProperty("sslmode", "disable");
                    }
                    else{
                        connectionProps.put("password", password);
                    }
                    final String url = (type == ExecutionType.MARIA_DB) ? mariaConnectionUrl : cockroachUrl;
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

        private void createTable(Connection connection) {
            final String sql = "CREATE TABLE IF NOT EXISTS PERSONS (\n" +
                "    PersonID int,\n" +
                "    Counter int,\n" +
                "    LastName varchar(255),\n" +
                "    FirstName varchar(255),\n" +
                "    Address varchar(255),\n" +
                "    City varchar(255)\n" +
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

        private boolean cleanTable() {
            String cleanCmd = "DELETE FROM PERSONS;";
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

        private void addRowsToTable(int totalNumberOfRows) {
            for (int i = 0; i < totalNumberOfRows; i++) {
                final StringBuilder insertSQLBuilder = new StringBuilder()
                    .append("INSERT INTO PERSONS VALUES (")
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

        @Setup(Level.Iteration)
        public void doSetup() {
            Connection connection = createConnection();
            createTable(connection);
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
            //Setup connection
            testConnection = createConnection();
            //Empty database
            boolean cleanResult = cleanTable();
            //Add new lines
            addRowsToTable(Integer.valueOf(rows));

            //Commit
            try {
                testConnection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        @TearDown(Level.Iteration)
        public void doTearDown() {
            System.out.println("Do TearDown");
            try {
                testConnection.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }


    }
}
