package com.att.research.mdbc.tables;

import java.io.Serializable;

import org.json.JSONObject;
import org.json.JSONTokener;

public final class Operation implements Serializable{

	private static final long serialVersionUID = -1215301985078183104L;

	final OperationType TYPE;
	final String OLD_VAL;
	final String NEW_VAL;

	public Operation(OperationType type, String newVal, String oldVal) {
		TYPE = type;
		NEW_VAL = newVal;
		OLD_VAL = oldVal;
	}

	public JSONObject getNewVal(){
        JSONObject newRow  = new JSONObject(new JSONTokener(NEW_VAL));
        return newRow;
    }

    public JSONObject getOldVal(){
        JSONObject keydata = new JSONObject(new JSONTokener(OLD_VAL));
        return keydata;
    }
    
    public OperationType getOperationType() {
    	return this.TYPE;
    }
}
