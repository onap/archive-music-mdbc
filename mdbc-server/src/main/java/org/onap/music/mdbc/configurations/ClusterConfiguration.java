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
package org.onap.music.mdbc.configurations;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.main.MusicCore;
import org.onap.music.mdbc.Utils;
import org.onap.music.mdbc.mixins.MusicInterface;
import org.onap.music.mdbc.mixins.MusicMixin;

public class ClusterConfiguration {
    private String internalNamespace;
    private int internalReplicationFactor;
    private String musicNamespace;
    private int musicReplicationFactor;
    private String mriTableName;
    private String mtxdTableName;
    private String eventualMtxdTableName;
    private String nodeInfoTableName;
    private String rangeDependencyTableName;

    private transient static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(ClusterConfiguration.class);

    public void initNamespaces(MusicMixin mi) throws MDBCServiceException {
        mi.createKeyspace(internalNamespace,internalReplicationFactor);
        mi.createKeyspace(musicNamespace,musicReplicationFactor);
    }

    public void initTables(MusicMixin mi) throws MDBCServiceException {
        mi.createMusicRangeInformationTable(musicNamespace, mriTableName);
        mi.createMusicTxDigest(mtxdTableName,musicNamespace, -1);
        mi.createMusicEventualTxDigest(eventualMtxdTableName,musicNamespace, -1);
        mi.createMusicNodeInfoTable(nodeInfoTableName,musicNamespace,-1);
        mi.createMusicRangeDependencyTable(musicNamespace,rangeDependencyTableName);
    }

    private void initInternalTable() throws MDBCServiceException {
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

     public static ClusterConfiguration readJsonFromFile(String filepath) throws FileNotFoundException {
        BufferedReader br;
        try {
            br = new BufferedReader(
                    new FileReader(filepath));
        } catch (FileNotFoundException e) {
            logger.error(EELFLoggerDelegate.errorLogger,"File was not found when reading json"+e);
            throw e;
        }
        Gson gson = new Gson();
        ClusterConfiguration config = gson.fromJson(br, ClusterConfiguration.class);
        return config;
    }


}
