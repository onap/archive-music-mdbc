package org.onap.music.mdbc.tables;

import java.util.UUID;

public final class MriReference {
	public final String table;
	public final UUID index;

	public MriReference(String table, UUID index) {
		this.table = table;
		this.index=  index;
	}

}
