package org.onap.music.mdbc;

public class RedoRow {
    private String redoTableName;
    private String redoRowIndex;

    public RedoRow(){}

    public RedoRow(String redoTableName, String redoRowIndex){
        this.redoRowIndex = redoRowIndex;
        this.redoTableName = redoTableName;
    }

    public String getRedoTableName() {
        return redoTableName;
    }

    public void setRedoTableName(String redoTableName) {
        this.redoTableName = redoTableName;
    }

    public String getRedoRowIndex() {
        return redoRowIndex;
    }

    public void setRedoRowIndex(String redoRowIndex) {
        this.redoRowIndex = redoRowIndex;
    }
}
