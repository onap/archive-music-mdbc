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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.mdbc.mixins.DBInterface;
import org.onap.music.mdbc.mixins.MusicInterface;
import org.onap.music.mdbc.mixins.MusicMixin;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class MetricPeekLockBenchmark {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(MetricPeekLockBenchmark.class.getSimpleName())
            .threads(1)
            .forks(1)
            .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void testMethod(MyState state, Blackhole blackhole) {
        String table = state.table_prepend_name+OwnUtils.MRI_TABLE_NAME;
        String selectQuery = "select * from "+OwnUtils.KEYSPACE+"."+table+" where key='"+state.key+"' LIMIT 1;";
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(selectQuery);
        ResultSet results=null;
        SimpleStatement statement = new SimpleStatement(queryObject.getQuery(), queryObject.getValues().toArray());
        statement.setConsistencyLevel(ConsistencyLevel.ONE);
        statement.enableTracing();
        results = state.dsHandle.getSession().execute(statement);
        blackhole.consume(results);
    }

    public void printInfo(ResultSet results){
        ExecutionInfo executionInfo = results.getExecutionInfo();
        System.out.println(executionInfo.getQueriedHost().getAddress());
        System.out.println(executionInfo.getQueriedHost().getDatacenter());
        System.out.println(executionInfo.getQueriedHost().getRack());
        final QueryTrace trace = executionInfo.getQueryTrace();
        System.out.printf(
            "'%s' to %s took %dμs%n",
            trace.getRequestType(), trace.getCoordinator(), trace.getDurationMicros());
        for (QueryTrace.Event event : trace.getEvents()) {
            System.out.printf(
                "  %d - %s - %s%n",
                event.getSourceElapsedMicros(), event.getSource(), event.getDescription());
        }
    }

    @State(Scope.Benchmark)
    public static class MyState {
        public MusicDataStore dsHandle;
        public String key;
        private String table_prepend_name = "lockQ_";
        final String user = OwnUtils.SQL_USER;
        final String password = OwnUtils.SQL_PASSWORD;
        public final Range range = new Range(OwnUtils.TABLE);
        @Param({"104.209.240.219"})
        public String ip;
        private MusicInterface musicMixin = null;
        private MdbcConnection conn;
        private DBInterface dbMixin;
        private MdbcServerLogic meta;
        private String id;
        public MusicRangeInformationRow lastRow;

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
            OwnUtils.dropAll(ip);
            setupServer();
            try {
                dsHandle=MusicDataStoreHandle.getDSHandle();
            } catch (MusicServiceException e) {
                e.printStackTrace();
                System.exit(1);
            }
            assignManager();
            OwnUtils.initMri((MusicMixin) musicMixin,range,meta, 1,1,1);
            id = UUID.randomUUID().toString();
            conn = (MdbcConnection) getManager().getConnection(id);
            lastRow = OwnUtils.getLastRow((MusicMixin) musicMixin);
            key = lastRow.getPartitionIndex().toString();
        }

        @TearDown(Level.Trial)
        public void doTrialTearDown(){
            try {
                conn.close();
            } catch (SQLException e) {
                System.exit(1);
            }
            meta=null;
        }
    }
}
