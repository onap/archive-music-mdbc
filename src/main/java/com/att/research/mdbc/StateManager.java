package com.att.research.mdbc;

import com.att.research.exceptions.MDBCServiceException;
import com.att.research.logging.EELFLoggerDelegate;
import com.att.research.logging.format.AppMessages;
import com.att.research.logging.format.ErrorSeverity;
import com.att.research.logging.format.ErrorTypes;
import com.att.research.mdbc.mixins.MixinFactory;
import com.att.research.mdbc.mixins.MusicInterface;
import com.att.research.mdbc.mixins.MusicMixin;
import com.att.research.mdbc.mixins.TxCommitProgress;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
    private MusicInterface musicManager;
    /**
     * This is the Running Queries information table.
     * It mainly contains information about the entities 
     * that have being committed so far.
     */
    private TxCommitProgress transactionInfo;
    
    private Map<String,MdbcConnection> mdbcConnections;

    private String sqlDatabase;

    private String url;
    
    private Properties info;
    
    @SuppressWarnings("unused")
	private DatabasePartition ranges;
    
    public StateManager(String url, Properties info, DatabasePartition ranges, String sqlDatabase) throws MDBCServiceException {
        this.sqlDatabase=sqlDatabase;
    	this.ranges=ranges;
    	this.url = url;
    	this.info = info;
    	this.transactionInfo = new TxCommitProgress();
    	//\fixme this is not really used, delete!
        String cassandraUrl  = info.getProperty(Configuration.KEY_CASSANDRA_URL, Configuration.CASSANDRA_URL_DEFAULT);
        String mixin  = info.getProperty(Configuration.KEY_MUSIC_MIXIN_NAME, Configuration.MUSIC_MIXIN_DEFAULT);
        this.musicManager = MixinFactory.createMusicInterface(mixin, cassandraUrl, info,ranges);
        this.musicManager.createKeyspace();
        try {
            this.musicManager.initializeMdbcDataStructures();
        } catch (MDBCServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
            throw(e);
        }
        MusicMixin.loadProperties();
        this.mdbcConnections = new HashMap<>();
        initSqlDatabase();
    }

    protected void initSqlDatabase() throws MDBCServiceException {
        try {
            //\TODO: pass the driver as a variable
            Class.forName("org.mariadb.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
            return;
        }
        try {
            Connection sqlConnection = DriverManager.getConnection(this.url, this.info);
            StringBuilder sql = new StringBuilder("CREATE DATABASE IF NOT EXISTS ")
                    .append(sqlDatabase)
                    .append(";");
            Statement stmt = sqlConnection.createStatement();
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
            throw new MDBCServiceException(e.getMessage());
        }
    }

    public void CloseConnection(String connectionId){
        //\TODO check if there is a race condition
        if(mdbcConnections.containsKey(connectionId)) {
            transactionInfo.deleteTxProgress(connectionId);
            try {
                Connection conn = mdbcConnections.get(connectionId);
                if(conn!=null)
                    conn.close();
            } catch (SQLException e) {
                logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
            }
            mdbcConnections.remove(connectionId);
        }
    }

    public void OpenConnection(String id, Properties information){
       if(!mdbcConnections.containsKey(id)){
           Connection sqlConnection;
           MdbcConnection newConnection;
           //Create connection to local SQL DB
           //\TODO: create function to generate connection outside of open connection and get connection
           try {
               //\TODO: pass the driver as a variable
               Class.forName("org.mariadb.jdbc.Driver");
           }
           catch (ClassNotFoundException e) {
               // TODO Auto-generated catch block
               logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
               return;
           }
           try {
               sqlConnection = DriverManager.getConnection(this.url+"/"+this.sqlDatabase, this.info);
           } catch (SQLException e) {
               logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.QUERYERROR, ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
               sqlConnection = null;
           }
           //Create MDBC connection
           try {
               newConnection = new MdbcConnection(id, this.url+"/"+this.sqlDatabase, sqlConnection, info, this.musicManager, transactionInfo,ranges);
           } catch (MDBCServiceException e) {
               logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
               newConnection = null;
               return;
           }
           logger.info(EELFLoggerDelegate.applicationLogger,"Connection created for connection: "+id);
           transactionInfo.createNewTransactionTracker(id, sqlConnection);
           if(newConnection != null) {
               mdbcConnections.put(id,newConnection);
           }
       }
    }

    /**
     * This function returns the connection to the corresponding transaction 
     * @param id of the transaction, created using
     * @return
     */
    public Connection GetConnection(String id) {
    	if(mdbcConnections.containsKey(id)) {
    		//\TODO: Verify if this make sense
    		// Intent: reinitialize transaction progress, when it already completed the previous tx for the same connection
    		if(transactionInfo.isComplete(id)) {
    			transactionInfo.reinitializeTxProgress(id);
    		}
    		return mdbcConnections.get(id);
    	}

    	Connection sqlConnection;
    	MdbcConnection newConnection;
        try {
            //TODO: pass the driver as a variable
            Class.forName("org.mariadb.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.QUERYERROR, ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
        }

        //Create connection to local SQL DB
		try {
			sqlConnection = DriverManager.getConnection(this.url+"/"+this.sqlDatabase, this.info);
		} catch (SQLException e) {
		    logger.error("sql connection was not created correctly");
			logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.QUERYERROR, ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
			sqlConnection = null;
		}
		//Create MDBC connection
    	try {
			newConnection = new MdbcConnection(id,this.url+"/"+this.sqlDatabase, sqlConnection, info, this.musicManager, transactionInfo,ranges);
		} catch (MDBCServiceException e) {
			logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
			newConnection = null;
		}
		logger.info(EELFLoggerDelegate.applicationLogger,"Connection created for connection: "+id);

    	transactionInfo.createNewTransactionTracker(id, sqlConnection);
    	if(newConnection != null) {
            mdbcConnections.put(id,newConnection);
        }
    	return newConnection;
    }

	public void InitializeSystem() {
		//\TODO Prefetch data to system using the data ranges as guide 
		throw new UnsupportedOperationException("Function initialize system needs to be implemented id MdbcStateManager");
	}
}
