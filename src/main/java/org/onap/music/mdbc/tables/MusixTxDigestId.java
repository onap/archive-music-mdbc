package org.onap.music.mdbc.tables;

import java.util.UUID;

public final class MusixTxDigestId {
	public final UUID tablePrimaryKey;

	public MusixTxDigestId(UUID primaryKey) {
		this.tablePrimaryKey= primaryKey;
	}

	public boolean isEmpty() {
		return (this.tablePrimaryKey==null);
	}
}
