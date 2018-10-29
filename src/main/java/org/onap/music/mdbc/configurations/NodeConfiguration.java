package org.onap.music.mdbc.configurations;

import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.DatabasePartition;
import org.onap.music.mdbc.MDBCUtils;
import org.onap.music.mdbc.Range;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class NodeConfiguration {

    private static transient final EELFLoggerDelegate LOG = EELFLoggerDelegate.getLogger(NodeConfiguration.class);

    public String sqlDatabaseName;
    public DatabasePartition partition;
    public String nodeName;

    public NodeConfiguration(String tables, UUID mriIndex, String mriTableName, String sqlDatabaseName, String node, String redoRecordsTable){
        //	public DatabasePartition(List<Range> knownRanges, UUID mriIndex, String mriTable, String lockId, String musicTxDigestTable) {
        partition = new DatabasePartition(toRanges(tables), mriIndex,  mriTableName, null, redoRecordsTable) ;
        this.sqlDatabaseName = sqlDatabaseName;
        this.nodeName = node;
    }

    protected List<Range> toRanges(String tables){
        List<Range> newRange = new ArrayList<>();
        String[] tablesArray=tables.split(",");
        for(String table: tablesArray) {
            newRange.add(new Range(table));
        }
        return newRange;
    }

    public String toJson() {
        GsonBuilder builder = new GsonBuilder();
        builder.setPrettyPrinting().serializeNulls();;
        Gson gson = builder.create();
        return gson.toJson(this);
    }

    public void saveToFile(String file){
        try {
            String serialized = this.toJson();
            MDBCUtils.saveToFile(serialized,file,LOG);
        } catch (IOException e) {
            e.printStackTrace();
            // Exit with error
            System.exit(1);
        }
    }

    public static NodeConfiguration readJsonFromFile( String filepath) throws FileNotFoundException {
        BufferedReader br;
        try {
            br = new BufferedReader(
                    new FileReader(filepath));
        } catch (FileNotFoundException e) {
            LOG.error(EELFLoggerDelegate.errorLogger,"File was not found when reading json"+e);
            throw e;
        }
        Gson gson = new Gson();
        NodeConfiguration config = gson.fromJson(br, NodeConfiguration.class);
        return config;
    }
}
