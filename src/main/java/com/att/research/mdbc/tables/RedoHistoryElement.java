package com.att.research.mdbc.tables;

import java.util.List;

public final class RedoHistoryElement {
	public final String partition;
	public final TitReference current;
	public final List<TitReference> previous;
	
	public RedoHistoryElement(String partition, TitReference current, List<TitReference> previous) {
		this.partition = partition;
		this.current = current;
		this.previous = previous;
	}
}
