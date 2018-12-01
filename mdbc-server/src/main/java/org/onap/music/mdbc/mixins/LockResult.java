package org.onap.music.mdbc.mixins;

import java.util.List;
import java.util.UUID;
import org.onap.music.mdbc.Range;

public class LockResult{
    private final UUID musicRangeInformationIndex;
    private final String ownerId;
    private List<Range> ranges;
    private final boolean newLock;

    public LockResult(UUID rowId, String ownerId, boolean newLock, List<Range> ranges){
        this.musicRangeInformationIndex = rowId;
        this.ownerId=ownerId;
        this.newLock=newLock;
        this.ranges=ranges;
    }
    public String getOwnerId(){
        return ownerId;
    }
    public boolean isNewLock(){
        return newLock;
    }
    public UUID getIndex() {return musicRangeInformationIndex;}
    public List<Range> getRanges() {return ranges;}
    public void addRange(Range range){ranges.add(range);}
}