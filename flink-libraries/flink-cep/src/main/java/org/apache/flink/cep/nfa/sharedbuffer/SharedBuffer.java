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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.cep.nfa.DeweyNumber;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a helper class of {@link SharedBufferAccessor}. It do the cache of the underlay sharedBuffer state
 * during a nfa process. It can reduce the state access when the ref change is requested several times on
 * a same {@code Lockable} Object. And it also implements the {@code AutoCloseable} interface to flush the
 * cache to underlay state automatically.
 */
public class SharedBuffer<V> implements AutoCloseable {

	/** The cache of eventsBuffer State. */
	private Map<EventId, Lockable<V>> eventsBufferCache = new HashMap<>();

	/** The cache of sharedBufferNode. */
	private Map<NodeId, Lockable<SharedBufferNode>> entryCache = new HashMap<>();

	private MapState<EventId, Lockable<V>> eventsBuffer;
	private MapState<NodeId, Lockable<SharedBufferNode>> entries;

	private SharedBufferAccessor<V> sharedBufferAccessor;

	public SharedBuffer(SharedBufferAccessor<V> accessor) {
		this.eventsBuffer = accessor.getEventsBuffer();
		this.entries = accessor.getEntries();
		this.sharedBufferAccessor = accessor;
		accessor.setSharedBuffer(this);
	}

	public void advanceTime(long timestamp) throws Exception {
		sharedBufferAccessor.advanceTime(timestamp);
	}

	public EventId registerEvent(V value, long timestamp) throws Exception {
		return sharedBufferAccessor.registerEvent(value, timestamp);
	}

	public NodeId put(
		final String stateName,
		final EventId eventId,
		@Nullable final NodeId previousNodeId,
		final DeweyNumber version) throws Exception {
		return sharedBufferAccessor.put(stateName, eventId, previousNodeId, version);
	}

	public List<Map<String, List<EventId>>> extractPatterns(
		final NodeId nodeId,
		final DeweyNumber version) throws Exception {
		return sharedBufferAccessor.extractPatterns(nodeId, version);
	}

	public Map<String, List<V>> materializeMatch(Map<String, List<EventId>> match) {
		return sharedBufferAccessor.materializeMatch(match, new HashMap<>());
	}

	public Map<String, List<V>> materializeMatch(Map<String, List<EventId>> match, Map<EventId, V> cache) {
		return sharedBufferAccessor.materializeMatch(match, cache);
	}

	public void lockNode(final NodeId node) throws Exception {
		sharedBufferAccessor.lockNode(node);
	}

	public void releaseNode(final NodeId node) throws Exception {
		sharedBufferAccessor.releaseNode(node);
	}

	public void releaseEvent(EventId eventId) throws Exception {
		sharedBufferAccessor.releaseEvent(eventId);
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
	protected void cacheEvent(EventId eventId, Lockable<V> event) {
		this.eventsBufferCache.put(eventId, event);
	}

	/**
	 * Put a ShareBufferNode to cache.
	 * @param nodeId id of the event
	 * @param entry SharedBufferNode
	 */
	protected void cacheEntry(NodeId nodeId, Lockable<SharedBufferNode> entry) {
		this.entryCache.put(nodeId, entry);
	}


	/**
	 * Try to get the sharedBufferNode from state iff the node has not been quered during this turn process.
	 * @param nodeId id of the event
	 * @return SharedBufferNode
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	protected Lockable<SharedBufferNode> getEntry(NodeId nodeId) throws Exception {
		Lockable<SharedBufferNode> entry = entryCache.get(nodeId);
		return  entry != null ? entry : entries.get(nodeId);
	}

	protected Lockable<V> getEvent(EventId eventId) throws Exception {
		Lockable<V> event = eventsBufferCache.get(eventId);
		return event != null ? event : eventsBuffer.get(eventId);
	}

	protected Map<EventId, Lockable<V>> getEventsBufferCache() {
		return eventsBufferCache;
	}

	protected Map<NodeId, Lockable<SharedBufferNode>> getEntryCache() {
		return entryCache;
	}

	/**
	 * Flush the event and node in map to state.
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	private void flushCache() throws Exception {
		if (!entryCache.isEmpty()) {
			entries.putAll(entryCache);
			entryCache.clear();
		}
		if (!eventsBufferCache.isEmpty()) {
			eventsBuffer.putAll(eventsBufferCache);
			eventsBufferCache.clear();
		}
	}

	@Override
	public void close() throws Exception {
		flushCache();
	}
}
