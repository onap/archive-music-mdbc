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
package org.onap.music.mdbc.configurations;

import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.MDBCUtils;
import org.onap.music.mdbc.Range;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class NodeConfiguration {

    private static transient final EELFLoggerDelegate LOG = EELFLoggerDelegate.getLogger(NodeConfiguration.class);

    public DatabasePartition partition;
    public String nodeName;
    public String sqlDatabaseName;

    public NodeConfiguration(String tables, UUID mriIndex, String sqlDatabaseName, String node){
        //	public DatabasePartition(List<Range> knownRanges, UUID mriIndex, String mriTable, String lockId, String musicTxDigestTable) {
        partition = new DatabasePartition(toRanges(tables), mriIndex, null) ;
        this.nodeName = node;
        this.sqlDatabaseName = sqlDatabaseName;
    }

    protected List<Range> toRanges(String tables){
        List<Range> newRange = new ArrayList<>();
        String[] tablesArray=tables.split(",");
        for(String table: tablesArray) {
            newRange.add(new Range(table));
        }
        return newRange;
    }

    public String toJson() {
        GsonBuilder builder = new GsonBuilder();
        builder.setPrettyPrinting().serializeNulls();;
        Gson gson = builder.create();
        return gson.toJson(this);
    }

    public void saveToFile(String file){
        try {
            String serialized = this.toJson();
            MDBCUtils.saveToFile(serialized,file,LOG);
        } catch (IOException e) {
            e.printStackTrace();
            // Exit with error
            System.exit(1);
        }
    }

    public static NodeConfiguration readJsonFromFile( String filepath) throws FileNotFoundException {
        BufferedReader br;
        try {
            br = new BufferedReader(
                    new FileReader(filepath));
        } catch (FileNotFoundException e) {
            LOG.error(EELFLoggerDelegate.errorLogger,"File was not found when reading json"+e);
            throw e;
        }
        Gson gson = new Gson();
        NodeConfiguration config = gson.fromJson(br, NodeConfiguration.class);
        return config;
    }
}
