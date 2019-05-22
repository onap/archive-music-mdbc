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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.logging.format.AppMessages;
import org.onap.music.logging.format.ErrorSeverity;
import org.onap.music.logging.format.ErrorTypes;
import org.onap.music.mdbc.query.SQLOperation;
import org.onap.music.mdbc.query.SQLOperationType;
import com.datastax.driver.core.utils.UUIDs;

public class Utils {


    public static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(Utils.class);

    public static Properties retrieveMdbcProperties() {
        Properties pr = null;
        try {
            pr = new Properties();
            pr.load(Utils.class.getResourceAsStream("/mdbc.properties"));
        } catch (IOException e) {
            logger.error(EELFLoggerDelegate.errorLogger, "Could not load property file: " + e.getMessage());
        }
        return pr;
    }

    public static String getDefaultMusicMixin() {
        Properties pr = retrieveMdbcProperties();
        if (pr == null)
            return null;
        String defaultMusicMixin = pr.getProperty("DEFAULT_MUSIC_MIXIN");
        return defaultMusicMixin;
    }

    public static String getDefaultDBMixin() {
        Properties pr = retrieveMdbcProperties();
        if (pr == null)
            return null;
        String defaultMusicMixin = pr.getProperty("DEFAULT_DB_MIXIN");
        return defaultMusicMixin;
    }

    public static void registerDefaultDrivers() {
        Properties pr = null;
        try {
            pr = new Properties();
            pr.load(Utils.class.getResourceAsStream("/mdbc.properties"));
        } catch (IOException e) {
            logger.error("Could not load property file > " + e.getMessage());
        }

        String drivers = pr.getProperty("DEFAULT_DRIVERS");
        for (String driver : drivers.split("[ ,]")) {
            logger.info(EELFLoggerDelegate.applicationLogger, "Registering jdbc driver '" + driver + "'");
            try {
                Class.forName(driver.trim());
            } catch (ClassNotFoundException e) {
                logger.error(EELFLoggerDelegate.errorLogger, "Driver class " + driver + " not found.");
            }
        }
    }

    public static void saveToFile(String serializedContent, String filename, EELFLoggerDelegate logger) throws IOException {
        try (PrintWriter fout = new PrintWriter(filename)) {
            fout.println(serializedContent);
        } catch (FileNotFoundException e) {
            if(logger!=null){
                logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.IOERROR, ErrorTypes.UNKNOWN, ErrorSeverity.CRITICAL);
            }
            else {
                e.printStackTrace();
            }
            throw e;
        }
    }

    /**
     * This functions is used to generate cassandra uuid
     * @return a random UUID that can be used for fields of type uuid
     */
    public static UUID generateUniqueKey() {
    	return UUIDs.random();
    }

    /**
     *  This function is used to generate time based cassandra uuid
     * @return a timebased UUID that can be used for fields of type uuid in MUSIC/Cassandra
     */
    public static UUID generateTimebasedUniqueKey() {return UUIDs.timeBased();}

    

    public static List<Range> getTables(Map<String,List<SQLOperation>> queryParsed){
    	return Utils.getTables(null, queryParsed);
    }

    public static List<Range> getTables(String defaultDatabaseName, Map<String,List<SQLOperation>> queryParsed){
        List<Range> ranges = new ArrayList<>();
        for(String table: queryParsed.keySet()){
            String[] parts = table.split("\\.");
            if(parts.length==2){
    			ranges.add(new Range(table));
    		}
    		else if(parts.length==1 && defaultDatabaseName!=null){
    			ranges.add(new Range(defaultDatabaseName+"."+table));
            }
            else{
            	throw new IllegalArgumentException("Table should either have only one '.' or none at all, the table "
    				+ "received is "+table);
    		}
    
    	}
        return ranges;
    }

    /**
     * determine the type of operation contained in the table to query map
     * 
     * @param tableToQueryType
     * @return write if any table has a write query. Read otherwise
     */
    public static SQLOperationType getOperationType(Map<String, List<SQLOperation>> tableToQueryType) {
        for (List<org.onap.music.mdbc.query.SQLOperation> tablesOps : tableToQueryType.values()) {
            for (org.onap.music.mdbc.query.SQLOperation op : tablesOps) {
                if (op.getOperationType() != SQLOperationType.READ) {
                    return SQLOperationType.WRITE;
                }
            }
        }
        return SQLOperationType.READ;
    }
}
