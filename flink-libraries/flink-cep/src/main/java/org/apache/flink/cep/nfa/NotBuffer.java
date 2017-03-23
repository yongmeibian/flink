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
package org.apache.flink.cep.nfa;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;

public class NotBuffer<K, V> {

	private final Map<K, ActiveState<K, V>> activeNotStates = new HashMap<>();

	private final LinkedListMultimap<K, SharedBuffer.ValueTimeWrapper<V>> events = LinkedListMultimap.create();

	public void putActiveState(K previousState, FilterFunction<V> condition) {
		ActiveState<K, V> currentEntry = activeNotStates.get(previousState);
		if (currentEntry != null) {
			currentEntry.referenceCounter++;
		} else {
			currentEntry = new ActiveState<>(previousState, condition);
		}
		activeNotStates.put(previousState, currentEntry);
	}

	public void removeActiveState(K previousState) {
		ActiveState<K, V> currentEntry = activeNotStates.get(previousState);
		if (currentEntry != null) {
			currentEntry.referenceCounter--;
			if (currentEntry.referenceCounter == 0) {
				activeNotStates.remove(previousState);
//				events.removeAll(previousState);
			} else {
				activeNotStates.put(previousState, currentEntry);
			}
		}
	}

	public void putValueIfViolates(K previousState, V event, long timestamp) {
		try {
			final ActiveState<K, V> kvActiveState = activeNotStates.get(previousState);
			if (kvActiveState != null && kvActiveState.condition.filter(event)) {
				events.put(previousState, new SharedBuffer.ValueTimeWrapper<V>(event, timestamp));
			}
		} catch (Exception e) {
			throw new RuntimeException("Failure happened in filter function.", e);
		}
	}

	public List<SharedBuffer.ValueTimeWrapper<V>> getPotentialViolations(K previousState) {
		return events.get(previousState);
	}


	private static class ActiveState<K, V> {
		private K key;
		private FilterFunction<V> condition;
		private int referenceCounter = 1;

		public ActiveState(K key, FilterFunction<V> condition) {
			this.key = key;
			this.condition = condition;
		}
	}

	//TODO pruning
}
