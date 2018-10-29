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

	private String musicRangeInformationTable;//Table that currently contains the REDO log for this partition
	private UUID musicRangeInformationIndex;//Index that can be obtained either from
	private String musicTxDigestTable;
	private String lockId;
	protected List<Range> ranges;

	/**
	 * Each range represents a partition of the database, a database partition is a union of this partitions. 
	 * The only requirement is that the ranges are not overlapping.
	 */
	
	public DatabasePartition() {
		ranges = new ArrayList<>();
	}
	
	public DatabasePartition(List<Range> knownRanges, UUID mriIndex, String mriTable, String lockId, String musicTxDigestTable) {
		if(knownRanges != null) {
			ranges = knownRanges;
		}
		else {
			ranges = new ArrayList<>();
		}

		if(musicTxDigestTable != null) {
            this.setMusicTxDigestTable(musicTxDigestTable);
        }
        else{
            this.setMusicTxDigestTable("");
        }

		if(mriIndex != null) {
			this.setMusicRangeInformationIndex(mriIndex);
		}
		else {
			this.setMusicRangeInformationIndex(null);
		}
		
		if(mriTable != null) {
			this.setMusicRangeInformationTable(mriTable);
		}
		else {
			this.setMusicRangeInformationTable("");
		}

		if(lockId != null) {
			this.setLockId(lockId);
		}
		else {
			this.setLockId("");
		}	
	}

	public String getMusicRangeInformationTable() {
		return musicRangeInformationTable;
	}

	public void setMusicRangeInformationTable(String musicRangeInformationTable) {
		this.musicRangeInformationTable = musicRangeInformationTable;
	}

	public MriReference getMusicRangeInformationIndex() {
		return new MriReference(musicRangeInformationTable,musicRangeInformationIndex);
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

    public String getMusicTxDigestTable() {
        return musicTxDigestTable;
    }

    public void setMusicTxDigestTable(String musicTxDigestTable) {
        this.musicTxDigestTable = musicTxDigestTable;
    }
}
