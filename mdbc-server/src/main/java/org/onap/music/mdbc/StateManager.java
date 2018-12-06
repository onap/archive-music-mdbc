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

import java.util.ArrayList;
import java.util.List;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.logging.format.AppMessages;
import org.onap.music.logging.format.ErrorSeverity;
import org.onap.music.logging.format.ErrorTypes;
import org.onap.music.mdbc.mixins.DBInterface;
import org.onap.music.mdbc.mixins.MixinFactory;
import org.onap.music.mdbc.mixins.MusicInterface;
import org.onap.music.mdbc.tables.MusicTxDigest;
import org.onap.music.mdbc.tables.TxCommitProgress;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * \TODO Implement an interface for the server logic and a factory 
 * @author Enrique Saurez
 */
public class StateManager {

	//\TODO We need to fix the auto-commit mode and multiple transactions with the same connection

	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(StateManager.class);

	/**
	 * This is the interface used by all the MusicSqlManagers, 
	 * that are created by the MDBC Server 
	 * @see MusicInterface 
     */
    private MusicInterface musicInterface;
    /**
     * This is the Running Queries information table.
     * It mainly contains information about the entities 
     * that have being committed so far.
     */
    private TxCommitProgress transactionInfo;
    private Map<String,MdbcConnection> mdbcConnections;
    private String sqlDBName;
    private String sqlDBUrl;
    
    String musicmixin;
    String cassandraUrl;
    private Properties info;
    
    /** Identifier for this server instance */
    private String mdbcServerName;
    private Map<String,DatabasePartition> connectionRanges;//Each connection owns its own database partition
    

	public StateManager(String sqlDBUrl, Properties info, String mdbcServerName, String sqlDBName) throws MDBCServiceException {
        this.sqlDBName = sqlDBName;
        this.sqlDBUrl = sqlDBUrl;
        this.info = info;
        this.mdbcServerName = mdbcServerName;
    
        this.connectionRanges = new HashMap<>();
        this.transactionInfo = new TxCommitProgress();
        //\fixme this might not be used, delete?
        try {
			info.load(this.getClass().getClassLoader().getResourceAsStream("music.properties"));
			info.putAll(MDBCUtils.getMdbcProperties());
		} catch (IOException e) {
			logger.error(EELFLoggerDelegate.errorLogger, e.getMessage());
		}
        cassandraUrl = info.getProperty(Configuration.KEY_CASSANDRA_URL, Configuration.CASSANDRA_URL_DEFAULT);
        musicmixin = info.getProperty(Configuration.KEY_MUSIC_MIXIN_NAME, Configuration.MUSIC_MIXIN_DEFAULT);
        
        initMusic();
        initSqlDatabase(); 

        
        MusicTxDigest txDaemon = new MusicTxDigest(this);
        txDaemon.startBackgroundDaemon(Integer.parseInt(
        		info.getProperty(Configuration.TX_DAEMON_SLEEPTIME_S, Configuration.TX_DAEMON_SLEEPTIME_S_DEFAULT))); 
    }

    /**
     * Initialize all the  interfaces and datastructures
     * @throws MDBCServiceException
     */
    protected void initMusic() throws MDBCServiceException {
        this.musicInterface = MixinFactory.createMusicInterface(musicmixin, mdbcServerName, info);
        this.mdbcConnections = new HashMap<>();
    }
    
    protected void initSqlDatabase() throws MDBCServiceException {
        try {
            //\TODO: pass the driver as a variable
            Class.forName("org.mariadb.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            return;
        }
        try {
            Connection sqlConnection = DriverManager.getConnection(this.sqlDBUrl, this.info);
            StringBuilder sql = new StringBuilder("CREATE DATABASE IF NOT EXISTS ")
                    .append(sqlDBName)
                    .append(";");
            Statement stmt = sqlConnection.createStatement();
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            throw new MDBCServiceException(e.getMessage(), e);
        }
    }

    public MusicInterface getMusicInterface() {
    	return this.musicInterface;
    }
    
    public List<DatabasePartition> getRanges() {
        return new ArrayList<>(connectionRanges.values());
	}


    public void closeConnection(String connectionId){
        //\TODO check if there is a race condition
        if(mdbcConnections.containsKey(connectionId)) {
            transactionInfo.deleteTxProgress(connectionId);
            try {
                Connection conn = mdbcConnections.get(connectionId);
                if(conn!=null)
                    conn.close();
            } catch (SQLException e) {
                logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL,
                        ErrorTypes.GENERALSERVICEERROR);
            }
            mdbcConnections.remove(connectionId);
        }
        if(connectionRanges.containsKey(connectionId)){
            //TODO release lock?
            connectionRanges.remove(connectionId);
        }
    }

    /**
     * Opens a connection into database, setting up all necessary triggers, etc
     * @param id UUID of a connection
     * @param information
     */
	public Connection openConnection(String id) {
		Connection sqlConnection;
    	MdbcConnection newConnection;
        try {
            //TODO: pass the driver as a variable
            Class.forName("org.mariadb.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.QUERYERROR, ErrorSeverity.CRITICAL,
                    ErrorTypes.QUERYERROR);
        }

        //Create connection to local SQL DB
		try {
			sqlConnection = DriverManager.getConnection(this.sqlDBUrl+"/"+this.sqlDBName, this.info);
		} catch (SQLException e) {
		    logger.error("sql connection was not created correctly");
			logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.QUERYERROR, ErrorSeverity.CRITICAL,
                    ErrorTypes.QUERYERROR);
			sqlConnection = null;
		}
		//check if a range was already created for this connection
        //TODO: later we could try to match it to some more sticky client id
        DatabasePartition ranges;
        if(connectionRanges.containsKey(id)){
            ranges=connectionRanges.get(id);
        }
        else{
        	//TODO: we don't need to create a partition for each connection
            ranges=new DatabasePartition(musicInterface.generateUniqueKey());
            connectionRanges.put(id,ranges);
        }
		//Create MDBC connection
    	try {
			newConnection = new MdbcConnection(id,this.sqlDBUrl+"/"+this.sqlDBName, sqlConnection, info, this.musicInterface,
                transactionInfo,ranges, this);
		} catch (MDBCServiceException e) {
			logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL,
                    ErrorTypes.QUERYERROR);
			newConnection = null;
		}
		logger.info(EELFLoggerDelegate.applicationLogger,"Connection created for connection: "+id);

    	transactionInfo.createNewTransactionTracker(id, sqlConnection);
    	if(newConnection != null) {
            mdbcConnections.put(id,newConnection);
        }
    	return newConnection;
	}
    
    
    /**
     * This function returns the connection to the corresponding transaction 
     * @param id of the transaction, created using
     * @return
     */
    public Connection getConnection(String id) {
        if(mdbcConnections.containsKey(id)) {
            //\TODO: Verify if this make sense
            // Intent: reinitialize transaction progress, when it already completed the previous tx for the same connection
            if(transactionInfo.isComplete(id)) {
                transactionInfo.reinitializeTxProgress(id);
            }
            return mdbcConnections.get(id);
        }

        return openConnection(id);
    }
    
    public DatabasePartition own(String mdbcConnectionId, List<Range> ranges, DBInterface dbi) throws MDBCServiceException {
        DatabasePartition partition = musicInterface.own(ranges, connectionRanges.get(mdbcConnectionId));
        List<UUID> oldRangeIds = partition.getOldMRIIds();
        //\TODO: do in parallel for all range ids
        for(UUID oldRange : oldRangeIds) {
            MusicTxDigest.replayDigestForPartition(musicInterface, oldRange,dbi);
        }
        logger.info("Partition: " + partition.getMRIIndex() + " now owns " + ranges);
        connectionRanges.put(mdbcConnectionId, partition);
        return partition;
    }

 

	public void initializeSystem() {
		//\TODO Prefetch data to system using the data ranges as guide 
		throw new UnsupportedOperationException("Function initialize system needs to be implemented id MdbcStateManager");
	}
}
