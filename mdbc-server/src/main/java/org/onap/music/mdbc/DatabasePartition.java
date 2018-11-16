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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

import org.onap.music.logging.EELFLoggerDelegate;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.onap.music.mdbc.tables.MriReference;

/**
 * A database range contain information about what ranges should be hosted in the current MDBC instance
 * A database range with an empty map, is supposed to contain all the tables in Music.  
 * @author Enrique Saurez 
 */
public class DatabasePartition {
	private transient static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(DatabasePartition.class);

	private UUID musicRangeInformationIndex;//Index that can be obtained either from
	private String lockId;
	protected List<Range> ranges;

	/**
	 * Each range represents a partition of the database, a database partition is a union of this partitions. 
	 * The only requirement is that the ranges are not overlapping.
	 */
	
	public DatabasePartition(UUID mriIndex) {
		this(new ArrayList<Range>(), mriIndex,"");
	}
	
	public DatabasePartition(List<Range> knownRanges, UUID mriIndex, String lockId) {
		ranges = knownRanges;
		
		if(mriIndex != null) {
			this.setMusicRangeInformationIndex(mriIndex);
		}
		else {
			this.setMusicRangeInformationIndex(null);
		}
		this.setLockId(lockId);
		
	}

	public UUID getMusicRangeInformationIndex() {
		return musicRangeInformationIndex;
	}

	public void setMusicRangeInformationIndex(UUID musicRangeInformationIndex) {
		this.musicRangeInformationIndex = musicRangeInformationIndex;
	}

	/**
	 * Add a new range to the ones own by the local MDBC 
	 * @param newRange range that is being added
	 * @throws IllegalArgumentException 
	 */
	public synchronized void addNewRange(Range newRange) {
		//Check overlap
		for(Range r : ranges) {
			if(r.overlaps(newRange)) {
				throw new IllegalArgumentException("Range is already contain by a previous range");
			}
		}
		ranges.add(newRange);
	}
	
	/**
	 * Delete a range that is being modified
	 * @param rangeToDel limits of the range
	 */
	public synchronized void deleteRange(Range rangeToDel) {
		if(!ranges.contains(rangeToDel)) {
			logger.error(EELFLoggerDelegate.errorLogger,"Range doesn't exist");
			throw new IllegalArgumentException("Invalid table"); 
		}
		ranges.remove(rangeToDel);
	}
	
	/**
	 * Get all the ranges that are currently owned
	 * @return ranges
	 */
	public synchronized Range[] getSnapshot() {
		return (Range[]) ranges.toArray();
	}
	
	/**
	 * Serialize the ranges
	 * @return serialized ranges
	 */
    public String toJson() {
    	GsonBuilder builder = new GsonBuilder();
    	builder.setPrettyPrinting().serializeNulls();;
        Gson gson = builder.create();
        return gson.toJson(this);	
    }
    
    /**
     * Function to obtain the configuration 
     * @param filepath path to the database range
     * @return a new object of type DatabaseRange
     * @throws FileNotFoundException
     */
    
    public static DatabasePartition readJsonFromFile( String filepath) throws FileNotFoundException {
    	 BufferedReader br;
		try {
			br = new BufferedReader(  
			         new FileReader(filepath));
		} catch (FileNotFoundException e) {
			logger.error(EELFLoggerDelegate.errorLogger,"File was not found when reading json"+e);
			throw e;
		}
    	Gson gson = new Gson();
    	DatabasePartition range = gson.fromJson(br, DatabasePartition.class);	
    	return range;
    }

	public String getLockId() {
		return lockId;
	}

	public void setLockId(String lockId) {
		this.lockId = lockId;
	}

}
