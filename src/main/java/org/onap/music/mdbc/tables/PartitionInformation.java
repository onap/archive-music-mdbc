package org.onap.music.mdbc.tables;

import org.onap.music.mdbc.Range;

import java.util.List;

public class PartitionInformation {
	public final List<Range> ranges;

	public PartitionInformation(List<Range> ranges) {
		this.ranges=ranges;
	}
}
