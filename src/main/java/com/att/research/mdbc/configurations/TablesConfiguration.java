package com.att.research.mdbc.configurations;

import com.att.research.exceptions.MDBCServiceException;
import com.att.research.logging.EELFLoggerDelegate;
import com.att.research.mdbc.DatabaseOperations;
import com.att.research.mdbc.RedoRow;
import com.att.research.mdbc.mixins.CassandraMixin;
import com.google.gson.Gson;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicPureCassaCore;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

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
        DatabaseOperations.createNamespace(musicNamespace, internalReplicationFactor);
        List<NodeConfiguration> nodeConfigs = new ArrayList<>();
        String ttpName = (tableToPartitionName==null || tableToPartitionName.isEmpty())?CassandraMixin.TABLE_TO_PARTITION_TABLE_NAME:tableToPartitionName;
        DatabaseOperations.CreateTableToPartitionTable(musicNamespace,ttpName);
        String pitName = (partitionInformationTableName==null || partitionInformationTableName.isEmpty())?CassandraMixin.PARTITION_INFORMATION_TABLE_NAME:partitionInformationTableName;
        DatabaseOperations.CreatePartitionInfoTable(musicNamespace,pitName);
        String rhName = (redoHistoryTableName==null || redoHistoryTableName.isEmpty())?CassandraMixin.REDO_HISTORY_TABLE_NAME:redoHistoryTableName;
        DatabaseOperations.CreateRedoHistoryTable(musicNamespace,rhName);
        if(partitions == null){
            logger.error("Partitions was not correctly initialized");
            throw new MDBCServiceException("Partition was not correctly initialized");
        }
        for(PartitionInformation partitionInfo : partitions){
            String titTableName = partitionInfo.mriTableName;
            titTableName = (titTableName==null || titTableName.isEmpty())?TIT_TABLE_NAME:titTableName;
            //0) Create the corresponding TIT table
            DatabaseOperations.CreateMusicRangeInformationTable(musicNamespace,titTableName);
            String musicTxDigestTableName = partitionInfo.mtxdTableName;
            musicTxDigestTableName = (musicTxDigestTableName==null || musicTxDigestTableName.isEmpty())? MUSIC_TX_DIGEST_TABLE_NAME :musicTxDigestTableName;
            DatabaseOperations.CreateMusicTxDigest(-1,musicNamespace,musicTxDigestTableName);
            //0) Create the corresponding TIT table
            String partitionId;
            if(partitionInfo.partitionId==null || partitionInfo.partitionId.isEmpty()){
                if(partitionInfo.replicationFactor==0){
                    logger.error("Replication factor and partition id are both empty, and this is an invalid configuration" );
                    throw new MDBCServiceException("Replication factor and partition id are both empty, and this is an invalid configuration");
                }
                //1) Create a row in the partition info table
                partitionId = DatabaseOperations.createPartitionInfoRow(musicNamespace,pitName,partitionInfo.replicationFactor,partitionInfo.tables,null);

            }
            else{
                partitionId = partitionInfo.partitionId;
            }
            //2) Create a row in the transaction information table
            String titIndex = DatabaseOperations.CreateEmptyTitRow(musicNamespace,titTableName,partitionId,null);
            //3) Add owner and tit information to partition info table
            RedoRow newRedoRow = new RedoRow(titTableName,titIndex);
            DatabaseOperations.updateRedoRow(musicNamespace,pitName,partitionId,newRedoRow,partitionInfo.owner,null);
            //4) Update ttp with the new partition
            for(String table: partitionInfo.tables) {
                DatabaseOperations.updateTableToPartition(musicNamespace, ttpName, table, partitionId, null);
            }
            //5) Add it to the redo history table
            DatabaseOperations.createRedoHistoryBeginRow(musicNamespace,rhName,newRedoRow,partitionId,null);
            //6) Create config for this node
            nodeConfigs.add(new NodeConfiguration(String.join(",",partitionInfo.tables),titIndex,titTableName,partitionId,sqlDatabaseName,partitionInfo.owner,musicTxDigestTableName));
        }
        return nodeConfigs;
    }

    private void initInternalNamespace() throws MDBCServiceException {
        DatabaseOperations.createNamespace(internalNamespace,internalReplicationFactor);
        StringBuilder createKeysTableCql = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
        .append(internalNamespace)
        .append(".unsynced_keys (key text PRIMARY KEY);");
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(createKeysTableCql.toString());
        try {
            MusicPureCassaCore.createTable(internalNamespace,"unsynced_keys", queryObject,"critical");
        } catch (MusicServiceException e) {
            logger.error("Error creating unsynced keys table" );
            throw new MDBCServiceException("Error creating unsynced keys table");
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
        private List<String> tables;
        private String owner;
        private String mriTableName;
        private String mtxdTableName;
        private String partitionId;
        private int replicationFactor;

        public List<String> getTables() {
            return tables;
        }

        public void setTables(List<String> tables) {
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
