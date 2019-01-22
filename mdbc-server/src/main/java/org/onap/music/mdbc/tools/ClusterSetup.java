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
package org.onap.music.mdbc.tools;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.FileNotFoundException;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.main.MusicUtil;
import org.onap.music.mdbc.configurations.ClusterConfiguration;

public class ClusterSetup {
    public static final EELFLoggerDelegate LOG = EELFLoggerDelegate.getLogger(ClusterSetup.class);

    @Parameter(names = { "-c", "--configuration" }, required = true,
            description = "This is the input file that is going to have the configuration to setup the cluster")
    private String configurationFile;
    @Parameter(names = { "-h", "-help", "--help" }, help = true,
            description = "Print the help message")
    private boolean help = false;

    private ClusterConfiguration inputConfig;

    public ClusterSetup(){}


    public void readInput(){
        LOG.info("Reading inputs");
        try {
            inputConfig = ClusterConfiguration.readJsonFromFile(configurationFile);
        } catch (FileNotFoundException e) {
            LOG.error("Input file is invalid or not found");
            System.exit(1);
        }
    }

    public void createAll() throws MDBCServiceException {
        inputConfig.initNamespaces();
        inputConfig.initTables();
    }

    public static void main(String[] args) {
        LOG.info("Starting cassandra cluster initializer");
        LOG.info("Using music file configuration:"+ MusicUtil.getMusicPropertiesFilePath());
        ClusterSetup configs = new ClusterSetup();
        @SuppressWarnings("deprecation")
        JCommander jc = new JCommander(configs, args);
        if (configs.help) {
            jc.usage();
            System.exit(1);
            return;
        }
        configs.readInput();
        try {
            configs.createAll();
        } catch (MDBCServiceException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}
