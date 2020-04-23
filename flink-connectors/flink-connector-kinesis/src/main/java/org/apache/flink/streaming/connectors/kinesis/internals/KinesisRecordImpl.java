/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.internals;

import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisRecord;

class KinesisRecordImpl implements KinesisRecord {
	private final byte[] data;
	private final String stream;
	private final String shardId;
	private final String partitionKey;
	private final String sequenceNumber;
	private final long approximateArrivalTimestamp;

	public KinesisRecordImpl(
			byte[] data,
			String stream,
			String shardId,
			String partitionKey,
			String sequenceNumber,
			long approximateArrivalTimestamp) {
		this.data = data;
		this.stream = stream;
		this.shardId = shardId;
		this.partitionKey = partitionKey;
		this.sequenceNumber = sequenceNumber;
		this.approximateArrivalTimestamp = approximateArrivalTimestamp;
	}

	@Override
	public byte[] getData() {
		return data;
	}

	@Override
	public String getStream() {
		return stream;
	}

	@Override
	public String getShardId() {
		return shardId;
	}

	@Override
	public String getPartitionKey() {
		return partitionKey;
	}

	@Override
	public String getSequenceNumber() {
		return sequenceNumber;
	}

	@Override
	public long getApproximateArrivalTimestamp() {
		return approximateArrivalTimestamp;
	}
}
