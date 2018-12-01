package org.onap.music.mdbc.tables;

import java.util.Comparator;

public class MriRowComparator implements Comparator<MusicRangeInformationRow> {

    @Override
    public int compare(MusicRangeInformationRow o1, MusicRangeInformationRow o2) {
        return Long.compare(o1.getTimestamp(),o2.getTimestamp());
    }
}
