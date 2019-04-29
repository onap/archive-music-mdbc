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
package org.onap.music.mdbc;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import org.onap.music.logging.EELFLoggerDelegate;
import org.onap.music.mdbc.mixins.MusicMixin;


/**
 * This class represent a range of the whole database 
 * For now a range represents directly a table in Cassandra
 * In the future we may decide to partition ranges differently
 * @author Enrique Saurez 
 */
public class Range implements Cloneable{

    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(Range.class);

	private String table;

	public Range(String table) {
		final String[] split = table.split("\\.");
		if(split.length!=2){
			logger.debug("Table should contain schema, received in constructor: " + table);
//			throw new IllegalArgumentException("Table should always contain the schema, table received in "
//				+ "constructor is "+table);
		}
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
        return (this.overlaps(r)) && (r.overlaps(this));
	}

	@Override
	public int hashCode(){
        return table.toUpperCase().hashCode();
	}

	@Override
	public Range clone() {
	    Range newRange = null;
	    try{
            newRange = (Range) super.clone();
            newRange.table = this.table;
        }
        catch(CloneNotSupportedException cns){
	        //\TODO add logging
        }
        return newRange;

    }

    public static boolean overlaps(List<Range> ranges, String table){
		//\TODO check if parallel stream makes sense here
        return ranges.stream().map((Range r) -> r.table.toUpperCase().equals(table.toUpperCase())).anyMatch((Boolean b) -> b);
	}

	public boolean overlaps(Range other) {
		return table.toUpperCase().equals(other.table.toUpperCase());
	}

    public String getTable() {
        return table;
    }

}
