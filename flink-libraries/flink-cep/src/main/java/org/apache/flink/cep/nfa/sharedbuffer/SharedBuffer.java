/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOVICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Vhe ASF licenses this file
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

package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This is a helper class of {@link SharedBufferAccessor}. It do the cache of the underlay sharedBuffer state
 * during a nfa process. It can reduce the state access when the ref change is requested several times on
 * a same {@code Lockable} Object. And it also implements the {@code AutoCloseable} interface to flush the
 * cache to underlay state automatically.
 */
public class SharedBuffer<V> {

	private static final String entriesStateName = "sharedBuffer-entries";
	private static final String eventsStateName = "sharedBuffer-events";
	private static final String eventsCountStateName = "sharedBuffer-events-count";

	/** The buffer holding the unique events seen so far. */
	private MapState<EventId, Lockable<V>> eventsBuffer;

	/** The number of events seen so far in the stream per timestamp. */
	private MapState<Long, Integer> eventsCount;
	private MapState<NodeId, Lockable<SharedBufferNode>> entries;

	/** The cache of eventsBuffer State. */
	private Map<EventId, Lockable<V>> eventsBufferCache = new HashMap<>();

	/** The cache of sharedBufferNode. */
	private Map<NodeId, Lockable<SharedBufferNode>> entryCache = new HashMap<>();

	public SharedBuffer(KeyedStateStore stateStore, TypeSerializer<V> valueSerializer) {
		this.eventsBuffer = stateStore.getMapState(
			new MapStateDescriptor<>(
				eventsStateName,
				EventId.EventIdSerializer.INSTANCE,
				new Lockable.LockableTypeSerializer<>(valueSerializer)));

		this.entries = stateStore.getMapState(
			new MapStateDescriptor<>(
				entriesStateName,
				NodeId.NodeIdSerializer.INSTANCE,
				new Lockable.LockableTypeSerializer<>(new SharedBufferNode.SharedBufferNodeSerializer())));

		this.eventsCount = stateStore.getMapState(
			new MapStateDescriptor<>(
				eventsCountStateName,
				LongSerializer.INSTANCE,
				IntSerializer.INSTANCE));
	}

	/**
	 * Initializes underlying state with given map of events and entries. Should be used only in case of migration from
	 * old state.
	 *
	 * @param events  map of events with assigned unique ids
	 * @param entries map of SharedBufferNodes
	 * @throws Exception Thrown if the system cannot access the state.
	 * @deprecated Only for state migration!
	 */
	@Deprecated
	public void init(
		Map<EventId, Lockable<V>> events,
		Map<NodeId, Lockable<SharedBufferNode>> entries) throws Exception {
		eventsBuffer.putAll(events);
		this.entries.putAll(entries);

		Map<Long, Integer> maxIds = events.keySet().stream().collect(Collectors.toMap(
			EventId::getTimestamp,
			EventId::getId,
			Math::max
		));
		eventsCount.putAll(maxIds);
	}

	public SharedBufferAccessor<V> getAccessor() {
		return new SharedBufferAccessor<>(this);
	}

	void advanceTime(long timestamp) throws Exception {
		Iterator<Long> iterator = eventsCount.keys().iterator();
		while (iterator.hasNext()) {
			Long next = iterator.next();
			if (next < timestamp) {
				iterator.remove();
			}
		}
	}

	EventId registerEvent(V value, long timestamp) throws Exception {
		Integer id = eventsCount.get(timestamp);
		if (id == null) {
			id = 0;
		}

		EventId eventId = new EventId(id, timestamp);
		Lockable<V> lockableValue = new Lockable<>(value, 1);
		eventsBuffer.put(eventId, lockableValue);
		eventsCount.put(timestamp, id + 1);
		eventsBufferCache.put(eventId, lockableValue);
		return eventId;
	}

	/**
    * Checks if there is no elements in the buffer.
    *
    * @return true if there is no elements in the buffer
    * @throws Exception Thrown if the system cannot access the state.
    */
	public boolean isEmpty() throws Exception {
		return Iterables.isEmpty(eventsBuffer.keys()) && Iterables.isEmpty(eventsBufferCache.keySet());
	}

	/**
	 * Put an event to cache.
	 * @param eventId id of the event
	 * @param event event body
	 */
	void cacheEvent(EventId eventId, Lockable<V> event) {
		this.eventsBufferCache.put(eventId, event);
	}

	/**
	 * Put a ShareBufferNode to cache.
	 * @param nodeId id of the event
	 * @param entry SharedBufferNode
	 */
	void cacheEntry(NodeId nodeId, Lockable<SharedBufferNode> entry) {
		this.entryCache.put(nodeId, entry);
	}

	/**
	 * Put an event to cache.
	 * @param eventId id of the event
	 */
	void removeEvent(EventId eventId) throws Exception {
		this.eventsBufferCache.remove(eventId);
		this.eventsBuffer.remove(eventId);
	}

	/**
	 * Put a ShareBufferNode to cache.
	 * @param nodeId id of the event
	 */
	void removeEntry(NodeId nodeId) throws Exception {
		this.entryCache.remove(nodeId);
		this.entries.remove(nodeId);
	}

	/**
	 * Try to get the sharedBufferNode from state iff the node has not been quered during this turn process.
	 * @param nodeId id of the event
	 * @return SharedBufferNode
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	Lockable<SharedBufferNode> getEntry(NodeId nodeId) throws Exception {
		Lockable<SharedBufferNode> entry = entryCache.get(nodeId);
		return  entry != null ? entry : entries.get(nodeId);
	}

	Lockable<V> getEvent(EventId eventId) throws Exception {
		Lockable<V> event = eventsBufferCache.get(eventId);
		return event != null ? event : eventsBuffer.get(eventId);
	}

	/**
	 * Flush the event and node in map to state.
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	void flushCache() throws Exception {
		if (!entryCache.isEmpty()) {
			entries.putAll(entryCache);
			entryCache.clear();
		}
		if (!eventsBufferCache.isEmpty()) {
			eventsBuffer.putAll(eventsBufferCache);
			eventsBufferCache.clear();
		}
	}
}
