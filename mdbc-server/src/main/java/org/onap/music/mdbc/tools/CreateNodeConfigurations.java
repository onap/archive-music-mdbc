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
package org.onap.music.mdbc.tools;

import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.configurations.NodeConfiguration;
import org.onap.music.mdbc.configurations.TablesConfiguration;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.FileNotFoundException;
import java.util.List;

public class CreateNodeConfigurations {
    public static final EELFLoggerDelegate LOG = EELFLoggerDelegate.getLogger(CreateNodeConfigurations.class);

    private String tables;
    @Parameter(names = { "-t", "--table-configurations" }, required = true,
            description = "This is the input file that is going to have the configuration for all the tables and partitions")
    private String tableConfigurationsFile;
    @Parameter(names = { "-b", "--basename" }, required = true,
            description = "This base name for all the outputs files that are going to be created")
    private String basename;
    @Parameter(names = { "-o", "--output-dir" }, required = true,
            description = "This is the output directory that is going to contain all the configuration file to be generated")
    private String outputDirectory;
    @Parameter(names = { "-h", "-help", "--help" }, help = true,
            description = "Print the help message")
    private boolean help = false;

    private TablesConfiguration inputConfig;

    public CreateNodeConfigurations(){}


    public void readInput(){
        try {
            inputConfig = TablesConfiguration.readJsonFromFile(tableConfigurationsFile);
        } catch (FileNotFoundException e) {
            LOG.error("Input file is invalid or not found");
            System.exit(1);
        }
    }

    public void createAndSaveNodeConfigurations(){
        List<NodeConfiguration> nodes = null;
        try {
            nodes = inputConfig.initializeAndCreateNodeConfigurations();
        } catch (MDBCServiceException e) {
            e.printStackTrace();
        }
        int counter = 0;
        for(NodeConfiguration nodeConfig : nodes){
            String name = (nodeConfig.nodeName==null||nodeConfig.nodeName.isEmpty())?Integer.toString(counter++): nodeConfig.nodeName;
            nodeConfig.saveToFile(outputDirectory+"/"+basename+"-"+name+".json");
        }
    }

    public static void main(String[] args) {
        CreateNodeConfigurations configs = new CreateNodeConfigurations();
        @SuppressWarnings("deprecation")
        JCommander jc = new JCommander(configs, args);
        if (configs.help) {
            jc.usage();
            System.exit(1);
            return;
        }
        configs.readInput();
        configs.createAndSaveNodeConfigurations();
        System.exit(0);
    }
}
