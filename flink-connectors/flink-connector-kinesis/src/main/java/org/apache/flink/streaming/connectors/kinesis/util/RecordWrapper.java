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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;

import java.util.Objects;

/**
 * The wrapper that holds the watermark handling related parameters
 * of a record produced by the shard consumer thread.
 *
 * @param <RecordT>
 */
public class RecordWrapper<RecordT> {
	final int shardStateIndex;
	final SequenceNumber lastSequenceNumber;
	final Watermark watermark;
	final boolean lastInBatch;
	final long timestamp;
	final RecordT value;

	public RecordWrapper(
			RecordT record,
			long timestamp,
			int shardStateIndex,
			SequenceNumber lastSequenceNumber,
			Watermark watermark,
			boolean lastInBatch) {
		this.value = record;
		this.timestamp = timestamp;
		this.shardStateIndex = shardStateIndex;
		this.lastSequenceNumber = lastSequenceNumber;
		this.watermark = watermark;
		this.lastInBatch = lastInBatch;
	}

	public boolean isLastInBatch() {
		return lastInBatch;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public int getShardStateIndex() {
		return shardStateIndex;
	}

	public SequenceNumber getLastSequenceNumber() {
		return lastSequenceNumber;
	}

	public Watermark getWatermark() {
		return watermark;
	}

	public RecordT getValue() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		RecordWrapper<?> that = (RecordWrapper<?>) o;
		return shardStateIndex == that.shardStateIndex &&
			lastInBatch == that.lastInBatch &&
			timestamp == that.timestamp &&
			Objects.equals(lastSequenceNumber, that.lastSequenceNumber) &&
			Objects.equals(watermark, that.watermark) &&
			Objects.equals(value, that.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(shardStateIndex, lastSequenceNumber, watermark, lastInBatch, timestamp, value);
	}

	@Override
	public String toString() {
		return "RecordWrapper{" +
			"shardStateIndex=" + shardStateIndex +
			", lastSequenceNumber=" + lastSequenceNumber +
			", watermark=" + watermark +
			", lastInBatch=" + lastInBatch +
			", timestamp=" + timestamp +
			", value=" + value +
			'}';
	}
}
