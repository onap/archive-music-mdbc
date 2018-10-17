package com.att.research.mdbc;

import java.sql.Connection;
import java.util.*;

import org.json.JSONObject;

import com.att.research.mdbc.mixins.DBInterface;
import com.att.research.mdbc.mixins.MixinFactory;
import com.att.research.mdbc.mixins.MusicInterface;
import com.att.research.mdbc.mixins.Utils;
import com.att.research.mdbc.tables.StagingTable;
import com.att.research.mdbc.tables.TxCommitProgress;
import com.att.research.exceptions.MDBCServiceException;
import com.att.research.exceptions.QueryException;
import com.att.research.logging.*;
import com.att.research.logging.format.AppMessages;
import com.att.research.logging.format.ErrorSeverity;
import com.att.research.logging.format.ErrorTypes;

/**
* <p>
* MUSIC SQL Manager - code that helps take data written to a SQL database and seamlessly integrates it
* with <a href="https://github.com/att/music">MUSIC</a> that maintains data in a No-SQL data-store
* (<a href="http://cassandra.apache.org/">Cassandra</a>) and protects access to it with a distributed
* locking service (based on <a href="https://zookeeper.apache.org/">Zookeeper</a>).
* </p>
* <p>
* This code will support transactions by taking note of the value of the autoCommit flag, and of calls
* to <code>commit()</code> and <code>rollback()</code>.  These calls should be made by the user's JDBC
* client.
* </p>
*
* @author  Bharath Balasubramanian, Robert Eby
*/
public class MusicSqlManager {

    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicSqlManager.class);

    private final DBInterface dbi;
    private final MusicInterface mi;
    private final Set<String> table_set;
    private final HashMap<Range, StagingTable> transactionDigest;
    private boolean autocommit;            // a copy of the autocommit flag from the JDBC Connection

    /**
     * Build a MusicSqlManager for a DB connection.  This construct may only be called by getMusicSqlManager(),
     * which will ensure that only one MusicSqlManager is created per URL.
     * This is the location where the appropriate mixins to use for the MusicSqlManager should be determined.
     * They should be picked based upon the URL and the properties passed to this constructor.
     * <p>
     * At the present time, we only support the use of the H2Mixin (for access to a local H2 database),
     * with the CassandraMixin (for direct access to a Cassandra noSQL DB as the persistence layer).
     * </p>
     *
     * @param url  the JDBC URL which was used to connection to the database
     * @param conn the actual connection to the database
     * @param info properties passed from the initial JDBC connect() call
     * @throws MDBCServiceException
     */
    public MusicSqlManager(String url, Connection conn, Properties info, MusicInterface mi) throws MDBCServiceException {
        try {
            info.putAll(Utils.getMdbcProperties());
            String mixinDb = info.getProperty(Configuration.KEY_DB_MIXIN_NAME, Configuration.DB_MIXIN_DEFAULT);
            this.dbi = MixinFactory.createDBInterface(mixinDb, this, url, conn, info);
            this.mi = mi;
            this.table_set = Collections.synchronizedSet(new HashSet<String>());
            this.autocommit = true;
            this.transactionDigest = new HashMap<Range, StagingTable>();

        } catch (Exception e) {
            throw new MDBCServiceException(e.getMessage());
        }
    }

    public void setAutoCommit(boolean b, String txId, TxCommitProgress progressKeeper, DatabasePartition partition) throws MDBCServiceException {
        if (b != autocommit) {
            autocommit = b;
            logger.debug(EELFLoggerDelegate.applicationLogger, "autocommit changed to " + b);
            if (b) {
                // My reading is that turning autoCOmmit ON should automatically commit any outstanding transaction
                if (txId == null || txId.isEmpty()) {
                    logger.error(EELFLoggerDelegate.errorLogger, "Connection ID is null", AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
                    throw new MDBCServiceException("tx id is null");
                }
                commit(txId, progressKeeper, partition);
            }
        }
    }

    /**
     * Close this MusicSqlManager.
     */
    public void close() {
        if (dbi != null) {
            dbi.close();
        }
    }

    /**
     * Code to be run within the DB driver before a SQL statement is executed.  This is where tables
     * can be synchronized before a SELECT, for those databases that do not support SELECT triggers.
     *
     * @param sql the SQL statement that is about to be executed
     */
    public void preStatementHook(final String sql) {
        dbi.preStatementHook(sql);
    }

    /**
     * Code to be run within the DB driver after a SQL statement has been executed.  This is where remote
     * statement actions can be copied back to Cassandra/MUSIC.
     *
     * @param sql the SQL statement that was executed
     */
    public void postStatementHook(final String sql) {
        dbi.postStatementHook(sql, transactionDigest);
    }

    /**
     * Synchronize the list of tables in SQL with the list in MUSIC. This function should be called when the
     * proxy first starts, and whenever there is the possibility that tables were created or dropped.  It is synchronized
     * in order to prevent multiple threads from running this code in parallel.
     */
    public synchronized void synchronizeTables() throws QueryException {
        Set<String> set1 = dbi.getSQLTableSet();    // set of tables in the database
        logger.debug(EELFLoggerDelegate.applicationLogger, "synchronizing tables:" + set1);
        for (String tableName : set1) {
            // This map will be filled in if this table was previously discovered
            if (!table_set.contains(tableName) && !dbi.getReservedTblNames().contains(tableName)) {
                logger.info(EELFLoggerDelegate.applicationLogger, "New table discovered: " + tableName);
                try {
                    TableInfo ti = dbi.getTableInfo(tableName);
                    mi.initializeMusicForTable(ti, tableName);
                    //\TODO Verify if table info can be modify in the previous step, if not this step can be deleted
                    ti = dbi.getTableInfo(tableName);
                    mi.createDirtyRowTable(ti, tableName);
                    dbi.createSQLTriggers(tableName);
                    table_set.add(tableName);
                    synchronizeTableData(tableName);
                    logger.debug(EELFLoggerDelegate.applicationLogger, "synchronized tables:" +
                            table_set.size() + "/" + set1.size() + "tables uploaded");
                } catch (Exception e) {
                    logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
                    //logger.error(EELFLoggerDelegate.errorLogger, "Exception synchronizeTables: "+e);
                    throw new QueryException();
                }
            }
        }

//			Set<String> set2 = getMusicTableSet(music_ns);
        // not working - fix later
//			for (String tbl : set2) {
//				if (!set1.contains(tbl)) {
//					logger.debug("Old table dropped: "+tbl);
//					dropSQLTriggers(tbl, conn);
//					// ZZTODO drop camunda table ?
//				}
//			}
    }

    /**
     * On startup, copy dirty data from Cassandra to H2. May not be needed.
     *
     * @param tableName
     */
    public void synchronizeTableData(String tableName) {
        // TODO - copy MUSIC -> H2
        dbi.synchronizeData(tableName);
    }

    /**
     * This method is called whenever there is a SELECT on a local SQL table, and should be called by the underlying databases
     * triggering mechanism.  It first checks the local dirty bits table to see if there are any keys in Cassandra whose value
     * has not yet been sent to SQL.  If there are, the appropriate values are copied from Cassandra to the local database.
     * Under normal execution, this function behaves as a NOP operation.
     *
     * @param tableName This is the table on which the SELECT is being performed
     */
    public void readDirtyRowsAndUpdateDb(String tableName) {
        mi.readDirtyRowsAndUpdateDb(dbi, tableName);
    }


    /**
     * This method gets the primary key that the music interfaces uses by default.
     * If the front end uses a primary key, this will not match what is used in the MUSIC interface
     *
     * @return
     */
    public String getMusicDefaultPrimaryKeyName() {
        return mi.getMusicDefaultPrimaryKeyName();
    }

    /**
     * Asks music interface to provide the function to create a primary key
     * e.g. uuid(), 1, "unique_aksd419fjc"
     *
     * @return
     */
    public String generateUniqueKey() {
        //
        return mi.generateUniqueKey();
    }


    /**
     * Perform a commit, as requested by the JDBC driver.  If any row updates have been delayed,
     * they are performed now and copied into MUSIC.
     *
     * @throws MDBCServiceException
     */
    public synchronized void commit(String txId, TxCommitProgress progressKeeper, DatabasePartition partition) throws MDBCServiceException {
        logger.debug(EELFLoggerDelegate.applicationLogger, " commit ");
        // transaction was committed -- add all the updates into the REDO-Log in MUSIC
        try {
            mi.commitLog(dbi, partition, transactionDigest, txId, progressKeeper);
        } catch (MDBCServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
            throw e;
        }
    }

    /**
     * Perform a rollback, as requested by the JDBC driver.  If any row updates have been delayed,
     * they are discarded.
     */
    public synchronized void rollback() {
        // transaction was rolled back - discard the updates
        logger.debug(EELFLoggerDelegate.applicationLogger, "Rollback");
        ;
        transactionDigest.clear();
    }

    /**
     * Get all
     *
     * @param table
     * @param dbRow
     * @return
     */
    public String getMusicKeyFromRowWithoutPrimaryIndexes(String table, JSONObject dbRow) {
        TableInfo ti = dbi.getTableInfo(table);
        return mi.getMusicKeyFromRowWithoutPrimaryIndexes(ti, table, dbRow);
    }

    public String getMusicKeyFromRow(String table, JSONObject dbRow) {
        TableInfo ti = dbi.getTableInfo(table);
        return mi.getMusicKeyFromRow(ti, table, dbRow);
    }

    /**
     * Returns all keys that matches the current sql statement, and not in already updated keys.
     *
     * @param sql the query that we are getting keys for
     * @deprecated
     */
    public ArrayList<String> getMusicKeys(String sql) {
        ArrayList<String> musicKeys = new ArrayList<String>();
        //\TODO See if this is required
		/*
		try {
			net.sf.jsqlparser.statement.Statement stmt = CCJSqlParserUtil.parse(sql);
			if (stmt instanceof Insert) {
				Insert s = (Insert) stmt;
				String tbl = s.getTable().getName();
				musicKeys.add(generatePrimaryKey());
			} else {
				String tbl;
				String where = "";
				if (stmt instanceof Update){
					Update u = (Update) stmt;
					tbl = u.getTables().get(0).getName();
					where = u.getWhere().toString();
				} else if (stmt instanceof Delete) {
					Delete d = (Delete) stmt;
					tbl = d.getTable().getName();
					if (d.getWhere()!=null) {
						where = d.getWhere().toString();
					}
				} else {
					System.err.println("Not recognized sql type");
					tbl = "";
				}
				String dbiSelect = "SELECT * FROM " + tbl;
				if (!where.equals("")) {
					dbiSelect += "WHERE" + where;
				}
				ResultSet rs = dbi.executeSQLRead(dbiSelect);
				musicKeys.addAll(getMusicKeysWhere(tbl, Utils.parseResults(dbi.getTableInfo(tbl), rs)));
				rs.getStatement().close();
			}
		} catch (JSQLParserException | SQLException e) {
			
			e.printStackTrace();
		}
		System.err.print("MusicKeys:");
		for(String musicKey:musicKeys) {
			System.out.print(musicKey + ",");
		}
		*/
        return musicKeys;
    }

    public void own(List<Range> ranges) {
        throw new java.lang.UnsupportedOperationException("function not implemented yet");
    }

    public void appendRange(String rangeId, List<Range> ranges) {
        throw new java.lang.UnsupportedOperationException("function not implemented yet");
    }

    public void relinquish(String ownerId, String rangeId) {
        throw new java.lang.UnsupportedOperationException("function not implemented yet");
    }
}
