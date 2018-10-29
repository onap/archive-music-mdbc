package org.onap.music.mdbc;

import java.io.Serializable;
import java.util.Objects;


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
	 * @param o the other range against which this is compared
	 * @return the equality result
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Range r = (Range) o;
		return (table.equals(r.table));
	}

	@Override
	public int hashCode(){
		return Objects.hash(table);
	}
	
	public boolean overlaps(Range other) {
		return table == other.table;
	}
}