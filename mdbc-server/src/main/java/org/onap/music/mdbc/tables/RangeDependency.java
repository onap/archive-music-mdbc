package org.onap.music.mdbc.tables;

import org.onap.music.mdbc.Range;

import java.util.List;

public class RangeDependency {
    final Range baseRange;
    final List<Range> dependentRanges;

    public RangeDependency(Range baseRange, List<Range> dependentRanges){
       this.baseRange=baseRange;
       this.dependentRanges=dependentRanges;
    }
    public Range getRange(){
        return baseRange;
    }
    public List<Range> dependentRanges(){
        return dependentRanges;
    }
}
