package org.onap.music.mdbc;

import java.io.Serializable;


/**
 * This class represent a range of the whole database 
 * For now a range represents directly a table in Cassandra
 * In the future we may decide to partition ranges differently
 * @author Enrique Saurez 
 */
public class Range implements Serializable {

	private static final long serialVersionUID = 1610744496930800088L;

	final public String table;

	public Range(String table) {
		this.table = table;
	}

	public String toString(){return table;}

	/**
	 * Compares to Range types
	 * @param other the other range against which this is compared 
	 * @return the equality result
	 */
	public boolean equal(Range other) {
		return (table == other.table);
	}
	
	public boolean overlaps(Range other) {
		return table == other.table;
	}
}