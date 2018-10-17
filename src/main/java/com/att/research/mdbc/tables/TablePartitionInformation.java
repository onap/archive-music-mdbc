package com.att.research.mdbc.tables;

import java.util.List;

public final class TablePartitionInformation {
	public final String table;
	public final String partition;
	public final List<String> oldPartitions;

	public TablePartitionInformation(String table, String partition, List<String> oldPartitions) {
		this.table = table;
		this.partition = partition;
		this.oldPartitions = oldPartitions;
	}
}
