package org.onap.music.mdbc.tables;

import java.util.List;
import org.apache.commons.lang.NotImplementedException;
import org.onap.music.exceptions.MDBCServiceException;
import org.onap.music.mdbc.Range;
import org.onap.music.mdbc.mixins.DBInterface;
import org.onap.music.mdbc.mixins.MusicInterface;

public class MusicSynchronization {
    public static void SynchronizeTables(DBInterface dbi, MusicInterface mi, List<Range> ranges) throws MDBCServiceException {
        throw new NotImplementedException("Need to be implemented");
    }

}
