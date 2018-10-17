package com.att.research.mdbc.tables;

public final class RedoRecordId {
	public final String leaseId;
	public final String commitId;

	public RedoRecordId(String leaseId, String commitId) {
		this.leaseId = leaseId;
		this.commitId = commitId;
	}

	public boolean isEmpty() {
		return (this.leaseId==null || this.leaseId.isEmpty())&&(this.commitId==null||this.commitId.isEmpty());
	}
}
