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

import com.datastax.driver.core.ResultSet;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.MDBCUtils;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.mixins.MusicMixin;
import org.onap.music.mdbc.tables.MusicRangeInformationRow;

import com.google.gson.Gson;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TablesConfiguration {

    private final String TIT_TABLE_NAME = "transactioninformation";
    private final String MUSIC_TX_DIGEST_TABLE_NAME = "musictxdigest";

    private transient static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(TablesConfiguration.class);
    private List<PartitionInformation> partitions;
    String tableToPartitionName;
    private String musicNamespace;
    private String partitionInformationTableName;
    private String redoHistoryTableName;
    private String sqlDatabaseName;

    public TablesConfiguration(){}

    /**
     * This functions initalize all the corresponding tables and rows
     * @return a list of node configurations to be used when starting each of the servers
     * @throws MDBCServiceException
     * @apiNote This function assumes that when used, there is not associated redo history in the tables to the tables that are going to be managed by this configuration file
     */
    public List<NodeConfiguration> initializeAndCreateNodeConfigurations() throws MDBCServiceException {
        logger.info("initializing the required spaces");

        List<NodeConfiguration> nodeConfigs = new ArrayList<>();
        if(partitions == null){
            logger.error("Partitions was not correctly initialized");
            throw new MDBCServiceException("Partition was not correctly initialized");
        }
        for(PartitionInformation partitionInfo : partitions){
            String mriTableName = partitionInfo.mriTableName;
            checkIfMriDoesNotExists(mriTableName,partitionInfo);
            String partitionId;
            if(partitionInfo.partitionId==null || partitionInfo.partitionId.isEmpty()){
                //1) Create a row in the partition info table
                partitionId = MDBCUtils.generateTimebasedUniqueKey().toString();
            }
            else{
                partitionId = partitionInfo.partitionId;
            }
            //2) Create a row in the transaction information table

            logger.info("Creating empty row with id "+partitionId);
            MusicMixin.createEmptyMriRow(musicNamespace,partitionInfo.mriTableName,UUID.fromString(partitionId),
                partitionInfo.owner,null,partitionInfo.getTables(),true);

            //3) Create config for this node
            StringBuilder newStr = new StringBuilder();
            for(Range r: partitionInfo.tables){
                newStr.append(r.toString().toUpperCase()).append(",");
            }

            logger.info("Appending row to configuration "+partitionId);
            nodeConfigs.add(new NodeConfiguration(newStr.toString(),"",UUID.fromString(partitionId),
            		sqlDatabaseName, partitionInfo.owner));
        }
        return nodeConfigs;
    }

    private void checkIfMriDoesNotExists(String mriTableName, PartitionInformation partition) throws MDBCServiceException {
        //If exists, check if empty
        StringBuilder checkRowsInTableString = new StringBuilder("SELECT * FROM ")
            .append(musicNamespace)
            .append(".")
            .append(mriTableName)
            .append("';");
        PreparedQueryObject checkRowsInTable = new PreparedQueryObject();
        checkRowsInTable.appendQueryString(checkRowsInTableString.toString());
        final ResultSet resultSet = MusicCore.quorumGet(checkRowsInTable);
        while(resultSet!=null && !resultSet.isExhausted()){
            final MusicRangeInformationRow mriRowFromCassandraRow = MusicMixin.getMRIRowFromCassandraRow(resultSet.one());
            List<Range> ranges = mriRowFromCassandraRow.getDBPartition().getSnapshot();
            for(Range range: partition.getTables()) {
                if (Range.overlaps(ranges,range.getTable())){
                    throw new MDBCServiceException("MRI row already exists");
                }
            }
        }
    }



    public static TablesConfiguration readJsonFromFile(String filepath) throws FileNotFoundException {
        BufferedReader br;
        try {
            br = new BufferedReader(
                    new FileReader(filepath));
        } catch (FileNotFoundException e) {
            logger.error(EELFLoggerDelegate.errorLogger,"File was not found when reading json"+e);
            throw e;
        }
        Gson gson = new Gson();
        TablesConfiguration config = gson.fromJson(br, TablesConfiguration.class);
        return config;
    }

    public class PartitionInformation{
        private List<Range> tables;
        private String owner;
        private String mriTableName;
        private String partitionId;

        public List<Range> getTables() {
            return tables;
        }

        public void setTables(List<Range> tables) {
            this.tables = tables;
        }

        public String getOwner() {
            return owner;
        }

        public void setOwner(String owner) {
            this.owner = owner;
        }

        public String getMriTableName() {
            return mriTableName;
        }

        public void setMriTableName(String mriTableName) {
            this.mriTableName = mriTableName;
        }

        public String getPartitionId() {
            return partitionId;
        }

        public void setPartitionId(String partitionId) {
            this.partitionId = partitionId;
        }

    }
}
