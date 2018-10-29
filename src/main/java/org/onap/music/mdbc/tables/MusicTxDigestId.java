package org.onap.music.mdbc.tables;

import java.util.UUID;

public final class MusicTxDigestId {
	public final UUID tablePrimaryKey;

	public MusicTxDigestId(UUID primaryKey) {
		this.tablePrimaryKey= primaryKey;
	}

	public boolean isEmpty() {
		return (this.tablePrimaryKey==null);
	}
}
