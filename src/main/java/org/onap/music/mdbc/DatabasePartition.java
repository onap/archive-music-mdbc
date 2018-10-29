package org.onap.music.mdbc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;

import org.onap.music.logging.EELFLoggerDelegate;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * A database range contain information about what ranges should be hosted in the current MDBC instance
 * A database range with an empty map, is supposed to contain all the tables in Music.  
 * @author Enrique Saurez 
 */
public class DatabasePartition {
	private transient static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(DatabasePartition.class);

	private String musicRangeInformationTable;//Table that currently contains the REDO log for this partition
	private String musicRangeInformationIndex;//Index that can be obtained either from
	private String musicTxDigestTable;
	private String partitionId;
	private String lockId;
	protected Set<Range> ranges;

	/**
	 * Each range represents a partition of the database, a database partition is a union of this partitions. 
	 * The only requirement is that the ranges are not overlapping.
	 */
	
	public DatabasePartition() {
		ranges = new HashSet<>();
	}
	
	public DatabasePartition(Set<Range> knownRanges, String mriIndex, String mriTable, String partitionId, String lockId, String musicTxDigestTable) {
		if(knownRanges != null) {
			ranges = knownRanges;
		}
		else {
			ranges = new HashSet<>();
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
			this.setMusicRangeInformationIndex("");
		}
		
		if(mriTable != null) {
			this.setMusicRangeInformationTable(mriTable);
		}
		else {
			this.setMusicRangeInformationTable("");
		}
		
		if(partitionId != null) {
			this.setPartitionId(partitionId);
		}
		else {
			this.setPartitionId("");
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

	public String getMusicRangeInformationIndex() {
		return musicRangeInformationIndex;
	}

	public void setMusicRangeInformationIndex(String musicRangeInformationIndex) {
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
		if(!ranges.contains(newRange)) {
			ranges.add(newRange);
		}
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

	public String getPartitionId() {
		return partitionId;
	}

	public void setPartitionId(String partitionId) {
		this.partitionId = partitionId;
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
