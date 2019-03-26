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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.mixins.DBInterface;
import org.onap.music.mdbc.mixins.MusicInterface;
import org.onap.music.mdbc.mixins.MusicMixin;
import org.onap.music.mdbc.tables.MusicTxDigestId;
import org.onap.music.mdbc.tables.StagingTable;
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
public class MetricThreadJoinBenchmark {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(MetricThreadJoinBenchmark.class.getSimpleName())
            .threads(1)
            .forks(1)
            .build();
        new Runner(opt).run();
    }

    public void testMethod1(final MyState state) {
        final String lockId = state.conn.getPartition().getLockId();
        final UUID MRIIndex = state.conn.getPartition().getMRIIndex();
        Thread t1=null;
        Thread t2=null;
        if(state.runTxDigest) {
            final Runnable insertDigestCallable = new Runnable() {
                @Override
                public void run() {
                    OwnUtils.hardcodedAddtransaction(110);
                }
            };
            t1 = new Thread(insertDigestCallable);
            t1.start();
        }
        if(state.runRedo) {
            final Runnable appendCallable = new Runnable() {
                @Override
                public void run() {
                    OwnUtils.hardcodedAppendToRedo(MRIIndex,lockId);
                }
            };
            t2 = new Thread(appendCallable);
            t2.start();
        }

        try {
            if(state.runTxDigest) {
                t1.join();
            }
            if(state.runRedo) {
                t2.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Benchmark
    public void testMethod2(final MyState state) {
        final String lockId = state.conn.getPartition().getLockId();
        final UUID MRIIndex = state.conn.getPartition().getMRIIndex();
        OwnUtils.hardcodedAddtransaction(110);
        OwnUtils.hardcodedAppendToRedo(MRIIndex,lockId);
    }

    @State(Scope.Benchmark)
    public static class MyState {
        public MusicTxDigestId musicTxDigestId,copyTxDigestId;
        public StagingTable copy,current;
        final String user = OwnUtils.SQL_USER;
        final String password = OwnUtils.SQL_PASSWORD;
        public final Range range = new Range(OwnUtils.TABLE);
        @Param({"127.0.0.1"})
        public String ip;
        @Param({"1", "10", "50", "80", "100", "200", "300", "400"})
        public int rows;
        @Param({"true","false"})
        public boolean runRedo;
        @Param({"true","false"})
        public boolean runTxDigest;
        private MusicInterface musicMixin = null;
        private MdbcConnection conn;
        private DBInterface dbMixin;
        private MdbcServerLogic meta;
        private String id;
        public ExecutorService commitExecutorThreads;

        private void setupServer(){
            meta = OwnUtils.setupServer(user, password);
        }

        private StateManager getManager(){
            return meta.getStateManager();
        }

        private void assignManager() {
            StateManager manager = getManager();
            musicMixin=manager.getMusicInterface();
        }

        @Setup(Level.Trial)
        public void doTrialSetup(){
            commitExecutorThreads = Executors.newFixedThreadPool(4);
            OwnUtils.dropAll(ip);
            setupServer();
            assignManager();
            OwnUtils.initMri((MusicMixin) musicMixin,range,meta, rows,0,0);
            id = UUID.randomUUID().toString();
            conn = (MdbcConnection) getManager().getConnection(id);
            try {
                Statement stmt = conn.createStatement();
                stmt.execute(OwnUtils.UPDATE);
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
            musicTxDigestId = OwnUtils.setupCommit(conn.getPartition(), conn.getTransactionDigest());
            current=conn.getTransactionDigest();
        }

        @TearDown(Level.Trial)
        public void doTrialTearDown(){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
            meta=null;
            commitExecutorThreads.shutdown();
        }

        @Setup(Level.Invocation)
        public void doInvocationSetup(){
            try {
                copy = new StagingTable(current);
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
                System.exit(1);
            }
            copyTxDigestId = new MusicTxDigestId(MDBCUtils.generateUniqueKey(), -1);
        }
    }
}
