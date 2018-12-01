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

import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.MDBCUtils;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.RedoRow;
import org.onap.music.mdbc.mixins.MusicMixin;
import org.onap.music.mdbc.tables.MusicTxDigest;

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
    private String internalNamespace;
    private int internalReplicationFactor;
    private String musicNamespace;
    private String tableToPartitionName;
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
        initInternalNamespace();

        List<NodeConfiguration> nodeConfigs = new ArrayList<>();
        if(partitions == null){
            logger.error("Partitions was not correctly initialized");
            throw new MDBCServiceException("Partition was not correctly initialized");
        }
        for(PartitionInformation partitionInfo : partitions){
            String mriTableName = partitionInfo.mriTableName;
            //0) Create the corresponding Music Range Information table

            String partitionId;
            if(partitionInfo.partitionId==null || partitionInfo.partitionId.isEmpty()){
                if(partitionInfo.replicationFactor==0){
                    logger.error("Replication factor and partition id are both empty, and this is an invalid configuration" );
                    throw new MDBCServiceException("Replication factor and partition id are both empty, and this is an invalid configuration");
                }
                //1) Create a row in the partition info table
                //partitionId = DatabaseOperations.createPartitionInfoRow(musicNamespace,pitName,partitionInfo.replicationFactor,partitionInfo.tables,null);

            }
            else{
                partitionId = partitionInfo.partitionId;
            }
            //2) Create a row in the transaction information table
            UUID mriTableIndex = MDBCUtils.generateTimebasedUniqueKey();
            //3) Add owner and tit information to partition info table
            RedoRow newRedoRow = new RedoRow(mriTableName,mriTableIndex);
            //DatabaseOperations.updateRedoRow(musicNamespace,pitName,partitionId,newRedoRow,partitionInfo.owner,null);
            //4) Update ttp with the new partition
            //for(String table: partitionInfo.tables) {
                //DatabaseOperations.updateTableToPartition(musicNamespace, ttpName, table, partitionId, null);
            //}
            //5) Add it to the redo history table
            //DatabaseOperations.createRedoHistoryBeginRow(musicNamespace,rhName,newRedoRow,partitionId,null);
            //6) Create config for this node
            StringBuilder newStr = new StringBuilder();
            for(Range r: partitionInfo.tables){
                newStr.append(r.toString()).append(",");
            }
            nodeConfigs.add(new NodeConfiguration(newStr.toString(),mriTableIndex,
            		sqlDatabaseName, partitionInfo.owner));
        }
        return nodeConfigs;
    }

    private void initInternalNamespace() throws MDBCServiceException {
        StringBuilder createKeysTableCql = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
        .append(internalNamespace)
        .append(".unsynced_keys (key text PRIMARY KEY);");
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(createKeysTableCql.toString());
        try {
            MusicCore.createTable(internalNamespace,"unsynced_keys", queryObject,"critical");
        } catch (MusicServiceException e) {
            logger.error("Error creating unsynced keys table" );
            throw new MDBCServiceException("Error creating unsynced keys table", e);
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
        private String mtxdTableName;
        private String partitionId;
        private int replicationFactor;

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

        public int getReplicationFactor() {
            return replicationFactor;
        }

        public void setReplicationFactor(int replicationFactor) {
            this.replicationFactor = replicationFactor;
        }

        public String getMtxdTableName(){
           return mtxdTableName;
        }

        public void setMtxdTableName(String mtxdTableName) {
            this.mtxdTableName = mtxdTableName;
        }
    }
}
