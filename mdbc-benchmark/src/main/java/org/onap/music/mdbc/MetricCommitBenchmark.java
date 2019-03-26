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

import static org.onap.music.mdbc.BenchmarkUtils.setupCreateRows;
import static org.onap.music.mdbc.BenchmarkUtils.setupCreateTables;

import org.onap.music.mdbc.BenchmarkUtils.ExecutionType;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class MetricCommitBenchmark {


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(MetricBenchmark.class.getSimpleName())
            .param("type", ExecutionType.METRIC.name())
            .forks(1)
            .threads(1)
            .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void testMethod(MyState state) {
        try {
            state.testConnection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    @State(Scope.Benchmark)
    public static class MyState {

        String update;
        final String user = "root";
        final String password = "metriccluster";
        @Param({"104.209.240.219"})
        public String ip;
        @Param({"1", "10", "50", "80", "100", "200", "300", "400"})
        public int rows;
        @Param({"MARIA_DB", "COCKROACH_DB", "METRIC","POSTGRES"})
        public ExecutionType type;
        @Param({"PERSONS"})
        public String table;

        public Connection testConnection;

        public Statement stmt;

        private Connection createConnection() {
            return BenchmarkUtils.getConnection(ip,type,user,password);
        }

        @Setup(Level.Trial)
        public void doTrialSetup(){
            BenchmarkUtils.SetupTable(table);
            update=BenchmarkUtils.updateBuilder;
        }

        @Setup(Level.Invocation)
        public void doInvocationSetup(){
            try {
                stmt = testConnection.createStatement();
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
        }

        @TearDown(Level.Invocation)
        public void doInvocationTearDown(){
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        @Setup(Level.Iteration)
        public void doSetup() {
            Connection connection = createConnection();
            setupCreateTables(connection);
            //Setup connection
            testConnection = createConnection();
            setupCreateRows(testConnection,rows);
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
