package com.att.research.mdbc.mixins;

import java.util.List;

public class PartitionInformation {
	public final String partition;
	public final TitReference tit; 
	public final List<String> tables;
	public final int replicationFactor;
	public final String currentOwner;
	
	public PartitionInformation(String partition, TitReference tit, List<String> tables, int replicationFactor, String currentOwner) {
		this.partition=partition;
		this.tit=tit;
		this.tables=tables;
		this.replicationFactor=replicationFactor;
		this.currentOwner=currentOwner;
	}
}
