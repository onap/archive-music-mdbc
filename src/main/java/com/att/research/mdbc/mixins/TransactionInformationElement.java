package com.att.research.mdbc.mixins;

import java.util.List;

public final class TransactionInformationElement {
	public final String index;
	public final List<RedoRecordId> redoLog;
	public final String partition;
	public final int latestApplied;
	public final boolean applied;
	
	public TransactionInformationElement(String index, List<RedoRecordId> redoLog, String partition, int latestApplied, boolean applied) {
		this.index = index;
		this.redoLog = redoLog;
		this.partition = partition;
		this.latestApplied = latestApplied;
		this.applied = applied;
	}
}
