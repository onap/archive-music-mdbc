/*
 * ============LICENSE_START====================================================
 * org.onap.music.mdbc
 * =============================================================================
 * Copyright (C) 2018 AT&T Intellectual Property. All rights reserved.
 * =============================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ============LICENSE_END======================================================
 */
package org.onap.music.mdbc.tables;

import java.util.List;
import java.util.UUID;

import org.onap.music.mdbc.DatabasePartition;

public final class MusicRangeInformationRow {
	private final UUID partitionIndex;
	private final List<MusicTxDigestId> redoLog;
	private final String ownerId;
	private final String metricProcessId;

	public MusicRangeInformationRow(UUID partitionIndex, List<MusicTxDigestId> redoLog,
                                    String ownerId, String metricProcessId) {
		this.partitionIndex = partitionIndex;
		this.redoLog = redoLog;
		this.ownerId = ownerId;
		this.metricProcessId = metricProcessId;
	}

	public UUID getPartitionIndex() {
		return partitionIndex;
	}

	public List<MusicTxDigestId> getRedoLog() {
		return redoLog;
	}

	public String getOwnerId() {
		return ownerId;
	}

	public String getMetricProcessId() {
		return metricProcessId;
	}
	
}
