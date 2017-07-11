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

package org.apache.flink.cep.operator;

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.common.typeutils.base.CollectionSerializerConfigSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * Custom type serializer implementation to serialize priority queues.
 *
 * @param <T> Type of the priority queue's elements
 */
public class PriorityQueueSerializer<T> extends TypeSerializer<PriorityQueue<StreamRecord<T>>> {

	private static final long serialVersionUID = -231980397616187715L;

	private static final int INITIAL_PRIORITY_QUEUE_CAPACITY = 11;

	private final TypeSerializer<StreamRecord<T>> elementSerializer;

	PriorityQueueSerializer(final TypeSerializer<StreamRecord<T>> elementSerializer) {
		this.elementSerializer = elementSerializer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<PriorityQueue<StreamRecord<T>>> duplicate() {
		return new PriorityQueueSerializer<>(elementSerializer.duplicate());
	}

	@Override
	public PriorityQueue<StreamRecord<T>> createInstance() {
		return new PriorityQueue<>(INITIAL_PRIORITY_QUEUE_CAPACITY, new StreamRecordComparator<T>());
	}

	@Override
	public PriorityQueue<StreamRecord<T>> copy(PriorityQueue<StreamRecord<T>> from) {
		PriorityQueue<StreamRecord<T>> result = createInstance();

		for (StreamRecord<T> element : from) {
			result.offer(elementSerializer.copy((element)));
		}

		return result;
	}

	@Override
	public PriorityQueue<StreamRecord<T>> copy(
		PriorityQueue<StreamRecord<T>> from,
		PriorityQueue<StreamRecord<T>> reuse) {
		reuse.clear();

		for (StreamRecord<T> element : from) {
			reuse.offer(elementSerializer.copy(element));
		}

		return reuse;
	}

	@Override
	public int getLength() {
		return 0;
	}

	@Override
	public void serialize(PriorityQueue<StreamRecord<T>> record, DataOutputView target) throws IOException {
		target.writeInt(record.size());

		for (StreamRecord<T> element : record) {
			elementSerializer.serialize(element, target);
		}
	}

	@Override
	public PriorityQueue<StreamRecord<T>> deserialize(DataInputView source) throws IOException {
		PriorityQueue<StreamRecord<T>> result = createInstance();

		return deserialize(result, source);
	}

	@Override
	public PriorityQueue<StreamRecord<T>> deserialize(
		PriorityQueue<StreamRecord<T>> reuse,
		DataInputView source) throws IOException {
		reuse.clear();

		int numberEntries = source.readInt();

		for (int i = 0; i < numberEntries; i++) {
			reuse.offer(elementSerializer.deserialize(source));
		}

		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {

	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PriorityQueueSerializer) {
			@SuppressWarnings("unchecked")
			PriorityQueueSerializer<T> other = (PriorityQueueSerializer<T>) obj;

			return elementSerializer.equals(other.elementSerializer);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof PriorityQueueSerializer;
	}

	@Override
	public int hashCode() {
		return Objects.hash(elementSerializer);
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting & compatibility
	// --------------------------------------------------------------------------------------------

	@Override
	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		return new CollectionSerializerConfigSnapshot<>(elementSerializer);
	}

	@Override
	public CompatibilityResult<PriorityQueue<StreamRecord<T>>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof CollectionSerializerConfigSnapshot) {
			Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> previousElemSerializerAndConfig =
				((CollectionSerializerConfigSnapshot) configSnapshot).getSingleNestedSerializerAndConfig();

			CompatibilityResult<StreamRecord<T>> compatResult = CompatibilityUtil.resolveCompatibilityResult(
				previousElemSerializerAndConfig.f0,
				UnloadableDummyTypeSerializer.class,
				previousElemSerializerAndConfig.f1,
				elementSerializer);

			if (!compatResult.isRequiresMigration()) {
				return CompatibilityResult.compatible();
			} else if (compatResult.getConvertDeserializer() != null) {
				return CompatibilityResult.requiresMigration(
					new PriorityQueueSerializer<>(
						new TypeDeserializerAdapter<>(compatResult.getConvertDeserializer())));
			}
		}

		return CompatibilityResult.requiresMigration();
	}
}
