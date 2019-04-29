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

import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.configurations.NodeConfiguration;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

public class CreatePartition {
    public static final EELFLoggerDelegate LOG = EELFLoggerDelegate.getLogger(CreatePartition.class);

    @Parameter(names = { "-t", "--tables" }, required = true,
           description = "This is the tables that are assigned to this ")
   private String tables;
    @Parameter(names = { "-e", "--tables" }, required = true,
           description = "This is the tables that are assigned to this ")
   private String eventual;
    @Parameter(names = { "-f", "--file" }, required = true,
            description = "This is the output file that is going to have the configuration for the ranges")
    private String file;
    @Parameter(names = { "-i", "--mri-index" }, required = true,
            description = "Index in the Mri Table")
    private String mriIndex;
    @Parameter(names = { "-m", "--mri-table-name" }, required = true,
            description = "Mri Table name")
    private boolean help = false;

    NodeConfiguration config;

    public CreatePartition(){
    }

    public void convert(){
        String[] tablesArray=eventual.split(",");
        ArrayList<String> eventualTables = (new ArrayList<>(Arrays.asList(tablesArray)));
        config = new NodeConfiguration(tables, eventualTables,UUID.fromString(mriIndex),"test","");
    }

    public void saveToFile(){
        config.saveToFile(file);
    }

    public static void main(String[] args) {

        CreatePartition newPartition = new CreatePartition();
        @SuppressWarnings("deprecation")
        JCommander jc = new JCommander(newPartition, args);
        if (newPartition.help) {
            jc.usage();
            System.exit(1);
            return;
        }
        newPartition.convert();
        newPartition.saveToFile();
    }
}
