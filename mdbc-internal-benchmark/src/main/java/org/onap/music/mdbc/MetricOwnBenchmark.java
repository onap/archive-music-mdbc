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

import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.mdbc.mixins.DBInterface;
import org.onap.music.mdbc.mixins.MusicInterface;
import org.onap.music.mdbc.mixins.MusicMixin;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class MetricOwnBenchmark{

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(MetricOwnBenchmark.class.getSimpleName())
            .threads(1)
            .forks(1)
            .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void testMethod(MyState state, Blackhole blackhole) {
        try {
            state.conn.preStatementHook(state.update);
        } catch (MDBCServiceException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @State(Scope.Benchmark)
    public static class MyState {
        public String update="SELECT * FROM PERSONS;";
        final String user = OwnUtils.SQL_USER;
        final String password = OwnUtils.SQL_PASSWORD;
        public final Range range = new Range(OwnUtils.TABLE);
        @Param({"104.209.240.219"})
        public String ip;
        //Rows per transaction (e.g. size of each tx digest)
        @Param({"1", "10", "50", "80", "100", "200", "300", "400"})
        public int rows;
        //Transaction before each ownership transition (e.g. size of redo log)
        @Param({"1","10","100"})
        public int updates;
        //Number of ownership transitions before measurement (e.g. number of mri rows"
        @Param({"1","10","100"})
        public int transitions;
        private MusicInterface musicMixin = null;
        private MdbcConnection conn;
        private DBInterface dbMixin;
        private MdbcServerLogic meta;
        private String id;

        private void setupServer(){
            meta = OwnUtils.setupServer(user, password);
        }

        private StateManager getManager(){
            return meta.getStateManager();
        }

        @Setup(Level.Trial)
        public void doTrialSetup(){
            OwnUtils.dropAll(ip);
            setupServer();
            StateManager manager = getManager();
            musicMixin=manager.getMusicInterface();
            OwnUtils.initMri((MusicMixin) musicMixin,range,meta, rows, updates, transitions);
            meta=null;
        }

        @TearDown(Level.Trial)
        public void doTrialTearDown(){
            meta=null;
        }

        @Setup(Level.Invocation)
        public void doInvocationSetup(){
            setupServer();
            id = UUID.randomUUID().toString();
            conn = (MdbcConnection) getManager().getConnection(id);
            try {
                OwnUtils.dropAndCreateTable(conn.getDBInterface());
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }

        @TearDown(Level.Invocation)
        public void doInvocationTearDown(){
            OwnUtils.deleteLastMriRow((MusicMixin) musicMixin);
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
            getManager().closeConnection(id);
        }

    }
}
