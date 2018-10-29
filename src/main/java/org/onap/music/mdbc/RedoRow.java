package org.onap.music.mdbc;

import java.util.UUID;

public class RedoRow {
    private String redoTableName;
    private UUID redoRowIndex;

    public RedoRow(){}

    public RedoRow(String redoTableName, UUID redoRowIndex){
        this.redoRowIndex = redoRowIndex;
        this.redoTableName = redoTableName;
    }

    public String getRedoTableName() {
        return redoTableName;
    }

    public void setRedoTableName(String redoTableName) {
        this.redoTableName = redoTableName;
    }

    public UUID getRedoRowIndex() {
        return redoRowIndex;
    }

    public void setRedoRowIndex(UUID redoRowIndex) {
        this.redoRowIndex = redoRowIndex;
    }
}
