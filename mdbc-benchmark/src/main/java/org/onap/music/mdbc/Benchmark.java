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

import java.sql.*;
import java.util.*;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

public class Benchmark
{

    @Parameter(names = { "-m", "--ismariadb" },
        description = "Is MARIADB evaluation")
    private boolean isMariaDb = false;


    @Parameter(names = { "-h", "-help", "--help" }, help = true,
        description = "Print the help message")
    private boolean help = false;

    public static class MyState {
        private Connection createConnection(){
            try {
                Class.forName(this.driver);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }
            Connection connection=null;
            try {
                if(!isMariaDb) {
                    connection = DriverManager.getConnection(connectionUrl);
                }
                else{
                    Properties connectionProps = new Properties();
                    connectionProps.put("user", user);
                    connectionProps.put("password", password);
                    connection = DriverManager.getConnection(connectionUrl,connectionProps);
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

        private void createTable(Connection connection){
            final String sql = "CREATE TABLE IF NOT EXISTS Persons (\n" +
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

            Boolean execute=null;
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

        private boolean cleanTable(){
            String cleanCmd = "DELETE FROM `Persons`;";
            Statement stmt = null;
            try {
                stmt = testConnection.createStatement();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }

            Boolean execute=null;
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

        private void addRowsToTable(int totalNumberOfRows){
            for(int i=0; i<totalNumberOfRows; i++) {
                final StringBuilder insertSQLBuilder = new StringBuilder()
                    .append("INSERT INTO Persons VALUES (")
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

        public void doSetup() {
            System.out.println("Do Global Setup");
            Connection connection = createConnection();
            createTable(connection);
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        public void doWarmup(int rows) {
            System.out.println("Do Setup");
            //Setup connection
            testConnection = createConnection();

            //Empty database
            boolean cleanResult = cleanTable();

            //Add new lines
            addRowsToTable(rows);

            //Commit
            try {
                testConnection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }

        }

        public void doTearDown() {
            System.out.println("Do TearDown");
            try {
                testConnection.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        boolean isMariaDb = false;
        String user = "root";
        String password = "metriccluster";
        public String driver = "org.apache.calcite.avatica.remote.Driver";
        //public final String driver = "org.mariadb.jdbc.Driver";

        public String connectionUrl = "jdbc:avatica:remote:url=http://localhost:30000;serialization=protobuf";
        //public final String connectionUrl = "jdbc:mariadb://localhost:3306/test";

        public Connection testConnection;
    }

    public static void testMethod(MyState state) {
        //UPDATE table_name
        //SET column1 = value1, column2 = value2, ...
        //WHERE condition;
        final StringBuilder updateBuilder = new StringBuilder()
            .append("UPDATE Persons ")
            .append("SET Counter = Counter + 1,")
            .append("City = 'Sandy Springs'")
            .append(";");
        Statement stmt = null;
        try {
            stmt = state.testConnection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Boolean execute = null;
        try {
            execute = stmt.execute(updateBuilder.toString());
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        try {
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
    }

    private static void printResults(Map<Integer,List<Long>> results){
        System.out.println("ROWS,P1,P5,P10,P25,P50,P75,P90,P95,P99,AVG,DEVIATION");
        for(Map.Entry<Integer,List<Long>> result : results.entrySet()) {
            Percentile resultsPercentiles = new Percentile();
            double[] tmpList= new double[result.getValue().size()];
            int counter = 0;
            for(Long val : result.getValue()) {
                tmpList[counter++]= Double.valueOf(val);
            }
            resultsPercentiles.setData(tmpList);
            final double average = result.getValue()
                .stream()
                .mapToDouble((x) -> x.doubleValue())
                .summaryStatistics()
                .getAverage();

            final double rawSum = result.getValue()
                .stream()
                .mapToDouble((x) -> Math.pow(x.doubleValue() - average,
                    2.0))
                .sum();

            final double deviation = Math.sqrt(rawSum / (result.getValue().size() - 1));
            System.out.printf("%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f\n",
                result.getKey(),
                resultsPercentiles.evaluate(1.0),
                resultsPercentiles.evaluate(5.0),
                resultsPercentiles.evaluate(10.0),
                resultsPercentiles.evaluate(25.0),
                resultsPercentiles.evaluate(50.0),
                resultsPercentiles.evaluate(75.0),
                resultsPercentiles.evaluate(90.0),
                resultsPercentiles.evaluate(95.0),
                resultsPercentiles.evaluate(99.0),
                average,
                deviation
            );
        }
    }

    public static void main( String[] args )
    {
        final Benchmark testApp = new Benchmark();
        @SuppressWarnings("deprecation")
        JCommander jc = new JCommander(testApp, args);
        if (testApp.help) {
            jc.usage();
            System.exit(1);
            return;
        }
        MyState state = new MyState();
        if(testApp.isMariaDb){
            state.isMariaDb = true;
            state.driver = "org.mariadb.jdbc.Driver";
            state.connectionUrl = "jdbc:mariadb://192.168.1.30:3306/test";

        }
        else{
            state.isMariaDb = false;
            state.driver = "org.apache.calcite.avatica.remote.Driver";
            state.connectionUrl = "jdbc:avatica:remote:url=http://localhost:30000;serialization=protobuf";

        }
        //iterations
        Map<Integer, List<Long>> results = new HashMap<>();
        final int totalIterations = 20;
        final int[] rows = { 1,10,100, 500, 1000};
        for(int row: rows) {
            System.out.println("Running for rows: "+Integer.toString(row));
            results.put(row,new ArrayList<Long>());
            for (int i = 0; i < totalIterations; i++) {
                System.out.println("Running iteration: "+Integer.toString(i));
                state.doSetup();
                state.doWarmup(row);
                long startTime = System.nanoTime();
                testMethod(state);
                long endTime = System.nanoTime();
                results.get(row).add(endTime - startTime);
                state.doTearDown();
            }
        }
        printResults(results);
    }
}
