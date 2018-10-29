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

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.SyntaxError;
import org.onap.music.datastore.CassaDataStore;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.main.MusicUtil;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestUtils {

    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(TestUtils.class);

    public static void createKeyspace(String keyspace, Session session) {
        String queryOp = "CREATE KEYSPACE " +
                keyspace +
                " WITH REPLICATION " +
                "= {'class':'SimpleStrategy', 'replication_factor':1}; ";
        ResultSet res=null;
        try {
            res = session.execute(queryOp);
        }
        catch(QueryExecutionException e){
            fail("Failure executing creation of keyspace with error: " + e.getMessage());
        } catch(SyntaxError e){
            fail("Failure executing creation of keyspace with syntax error: " + e.getMessage());
        }
        assertTrue("Keyspace "+keyspace+" is already being used, please change it to avoid loosing data",res.wasApplied());
    }

    public static void deleteKeyspace(String keyspace, Session session){
        String queryBuilder = "DROP KEYSPACE " +
                keyspace +
                ";";
        ResultSet res = session.execute(queryBuilder);
        assertTrue("Keyspace "+keyspace+" doesn't exist and it should",res.wasApplied());
    }

    public static HashSet<String> getMriColNames(){
        return new HashSet<>(
                Arrays.asList("rangeid","keys","txredolog","ownerid","metricprocessid")
        );
    }

    public static HashSet<String> getMtdColNames(){
        return new HashSet<>(
                Arrays.asList("txid","transactiondigest")
        );
    }

    public static HashMap<String, DataType> getMriColTypes(Cluster cluster){
        HashMap<String, DataType> expectedTypes = new HashMap<>();
        expectedTypes.put("rangeid",DataType.uuid());
        expectedTypes.put("keys",DataType.set(DataType.text()));
        ProtocolVersion currentVer =  cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
        assertNotNull("Protocol version for cluster is invalid", currentVer);
        CodecRegistry registry = cluster.getConfiguration().getCodecRegistry();
        assertNotNull("Codec registry for cluster is invalid", registry);
        expectedTypes.put("txredolog",DataType.list(TupleType.of(currentVer,registry,DataType.text(),DataType.uuid())));
        expectedTypes.put("ownerid",DataType.text());
        expectedTypes.put("metricprocessid",DataType.text());
        return expectedTypes;
    }

    public static HashMap<String, DataType> getMtdColTypes(){
        HashMap<String,DataType> expectedTypes = new HashMap<>();
        expectedTypes.put("txid",DataType.uuid());
        expectedTypes.put("transactiondigest",DataType.text());
        return expectedTypes;
    }


    public static void checkRowsInTable(String keyspace, String tableName, CassaDataStore ds,
                                        HashSet<String> expectedColumns, HashMap<String,DataType> expectedTypes){
        TableMetadata table = ds.returnColumnMetadata(keyspace,tableName);
        assertNotNull("Error obtaining metadata of table, there may be an error with its creation", table);
        List<ColumnMetadata> columnsMeta = table.getColumns();
        checkDataTypeForTable(columnsMeta,expectedColumns,expectedTypes);
    }

    public static void checkDataTypeForTable(List<ColumnMetadata> columnsMeta, HashSet<String> expectedColumns,
                               HashMap<String,DataType> expectedTypes){
        for(ColumnMetadata cMeta : columnsMeta){
            String columnName = cMeta.getName();
            DataType type = cMeta.getType();
            assertTrue("Invalid column name: "+columnName,expectedColumns.contains(columnName));
            assertTrue("Fix the contents of expectedtypes for column: "+columnName,
                    expectedTypes.containsKey(columnName));
            assertEquals("Invalid type for column: "+columnName,
                    expectedTypes.get(columnName),type);
        }
    }

    public static void readPropertiesFile(Properties prop) {
        try {
            String fileLocation = MusicUtil.getMusicPropertiesFilePath();
            InputStream fstream = new FileInputStream(fileLocation);
            prop.load(fstream);
            fstream.close();
        } catch (FileNotFoundException e) {
            logger.error("Configuration file not found");

        } catch (IOException e) {
            // TODO Auto-generated catch block
            logger.error("Exception when reading file: "+e.toString());
        }
    }


    public static void populateMusicUtilsWithProperties(Properties prop){
        //TODO: Learn how to do this properly within music
        String[] propKeys = MusicUtil.getPropkeys();
        for (int k = 0; k < propKeys.length; k++) {
            String key = propKeys[k];
            if (prop.containsKey(key) && prop.get(key) != null) {
                switch (key) {
                    case "cassandra.host":
                        MusicUtil.setMyCassaHost(prop.getProperty(key));
                        break;
                    case "music.ip":
                        MusicUtil.setDefaultMusicIp(prop.getProperty(key));
                        break;
                    case "debug":
                        MusicUtil.setDebug(Boolean
                                .getBoolean(prop.getProperty(key).toLowerCase()));
                        break;
                    case "version":
                        MusicUtil.setVersion(prop.getProperty(key));
                        break;
                    case "music.rest.ip":
                        MusicUtil.setMusicRestIp(prop.getProperty(key));
                        break;
                    case "music.properties":
                        MusicUtil.setMusicPropertiesFilePath(prop.getProperty(key));
                        break;
                    case "lock.lease.period":
                        MusicUtil.setDefaultLockLeasePeriod(
                                Long.parseLong(prop.getProperty(key)));
                        break;
                    case "my.id":
                        MusicUtil.setMyId(Integer.parseInt(prop.getProperty(key)));
                        break;
                    case "all.ids":
                        String[] ids = prop.getProperty(key).split(":");
                        MusicUtil.setAllIds(new ArrayList<String>(Arrays.asList(ids)));
                        break;
                    case "public.ip":
                        MusicUtil.setPublicIp(prop.getProperty(key));
                        break;
                    case "all.public.ips":
                        String[] ips = prop.getProperty(key).split(":");
                        if (ips.length == 1) {
                            // Future use
                        } else if (ips.length > 1) {
                            MusicUtil.setAllPublicIps(
                                    new ArrayList<String>(Arrays.asList(ips)));
                        }
                        break;
                    case "cassandra.user":
                        MusicUtil.setCassName(prop.getProperty(key));
                        break;
                    case "cassandra.password":
                        MusicUtil.setCassPwd(prop.getProperty(key));
                        break;
                    case "aaf.endpoint.url":
                        MusicUtil.setAafEndpointUrl(prop.getProperty(key));
                        break;
                    default:
                        System.out.println("No case found for " + key);
                }
            }
        }


    }
}