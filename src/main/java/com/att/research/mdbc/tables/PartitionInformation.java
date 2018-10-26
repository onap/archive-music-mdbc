package com.att.research.mdbc.tables;

import java.util.List;

public class PartitionInformation {
	public final List<String> tables;

	public PartitionInformation(List<String> tables) {
		this.tables=tables;
	}
}
