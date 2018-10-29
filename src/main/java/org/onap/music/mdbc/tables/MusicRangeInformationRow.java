package org.onap.music.mdbc.tables;

import java.util.List;
import java.util.UUID;

public final class MusicRangeInformationRow {
	public final UUID index;
	public final PartitionInformation partition;
	public final List<MusixTxDigestId> redoLog;

	public MusicRangeInformationRow(UUID index, List<MusixTxDigestId> redoLog, PartitionInformation partition) {
		this.index = index;
		this.redoLog = redoLog;
		this.partition = partition;
	}
}
