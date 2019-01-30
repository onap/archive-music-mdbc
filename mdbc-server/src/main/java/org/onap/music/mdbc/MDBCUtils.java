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

import java.io.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.logging.format.AppMessages;
import org.onap.music.logging.format.ErrorSeverity;
import org.onap.music.logging.format.ErrorTypes;
import org.onap.music.mdbc.mixins.MusicMixin;
import org.onap.music.mdbc.mixins.Utils;
import org.onap.music.mdbc.tables.Operation;
import org.onap.music.mdbc.tables.StagingTable;

import com.datastax.driver.core.utils.UUIDs;

import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONObject;

public class MDBCUtils {

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

	public static Properties getMdbcProperties() {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = Utils.class.getClassLoader().getResourceAsStream("mdbc.properties");
			prop.load(input);
		} catch (Exception e) {
			Utils.logger.warn(EELFLoggerDelegate.applicationLogger, "Could not load mdbc.properties."
					+ "Proceeding with defaults " + e.getMessage());
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					Utils.logger.error(EELFLoggerDelegate.errorLogger, e.getMessage());
				}
			}
		}
		return prop;
	}

	public static List<Range> getTables(Map<String,List<String>> queryParsed){
	    List<Range> ranges = new ArrayList<>();
	    for(String table: queryParsed.keySet()){
	       ranges.add(new Range(table));
        }
	    return ranges;
    }
}