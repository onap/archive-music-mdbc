package org.onap.music.mdbc.tables;

import java.io.Serializable;

import org.json.JSONObject;
import org.json.JSONTokener;

public final class Operation implements Serializable{

	private static final long serialVersionUID = -1215301985078183104L;

	final OperationType TYPE;
	final String NEW_VAL;

	public Operation(OperationType type, String newVal) {
		TYPE = type;
		NEW_VAL = newVal;
	}

	public JSONObject getNewVal(){
        JSONObject newRow  = new JSONObject(new JSONTokener(NEW_VAL));
        return newRow;
    }

    public OperationType getOperationType() {
    	return this.TYPE;
    }

    @Override
    public boolean equals(Object o){
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Operation r = (Operation) o;
        return TYPE.equals(r.TYPE) && NEW_VAL.equals(r.NEW_VAL);
    }
}
