package com.att.research.mdbc.tools;

import com.att.research.logging.EELFLoggerDelegate;
import com.att.research.mdbc.configurations.NodeConfiguration;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class CreatePartition {
    public static final EELFLoggerDelegate LOG = EELFLoggerDelegate.getLogger(CreatePartition.class);

    @Parameter(names = { "-t", "--tables" }, required = true,
           description = "This is the tables that are assigned to this ")
   private String tables;
    @Parameter(names = { "-f", "--file" }, required = true,
            description = "This is the output file that is going to have the configuration for the ranges")
    private String file;
    @Parameter(names = { "-i", "--mri-index" }, required = true,
            description = "Index in the Mri Table")
    private String mriIndex;
    @Parameter(names = { "-m", "--mri-table-name" }, required = true,
            description = "Mri Table name")
    private String mriTable;
     @Parameter(names = { "-r", "--music-tx-digest-table-name" }, required = true,
                         description = "Music Transaction Digest Table name")
     private String mtxdTable;
    @Parameter(names = { "-p", "--partition-id" }, required = true,
            description = "Partition Id")
    private String partitionId;
    @Parameter(names = { "-h", "-help", "--help" }, help = true,
            description = "Print the help message")
    private boolean help = false;

    NodeConfiguration config;

    public CreatePartition(){
    }

    public void convert(){
        config = new NodeConfiguration(tables, mriIndex,mriTable,partitionId,"test","", mtxdTable);
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
