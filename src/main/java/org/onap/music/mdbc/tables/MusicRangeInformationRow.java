package org.onap.music.mdbc.tables;

import java.util.List;
import java.util.UUID;

public final class MusicRangeInformationRow {
	public final UUID index;
	public final PartitionInformation partition;
	public final List<MusicTxDigestId> redoLog;
	public final String ownerId;
	public final String metricProcessId;

	public MusicRangeInformationRow(UUID index, List<MusicTxDigestId> redoLog, PartitionInformation partition,
                                    String ownerId, String metricProcessId) {
		this.index = index;
		this.redoLog = redoLog;
		this.partition = partition;
		this.ownerId = ownerId;
		this.metricProcessId = metricProcessId;
	}

}
