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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.compiler.NFAFactory;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 *
 * @param <IN1>
 * @param <KEY>
 * @param <OUT>
 */
public abstract class AbstractCoCepPatternOperator<IN1, KEY, OUT> extends AbstractStreamOperator<OUT>
	implements TwoInputStreamOperator<IN1, Pattern<IN1, ?>, OUT>, Triggerable<KEY, VoidNamespace> {

	private static final long serialVersionUID = -4166778210774160757L;

	private final boolean isProcessingTime;

	private final TypeSerializer<IN1> inputSerializer;

	///////////////			State			//////////////

	private static final String NFA_OPERATOR_STATE_NAME = "nfaOperatorMapStateName";
	private static final String DEFAULT_NFA_FACTORY_STATE_NAME = "nfaOperatorStateName";
	private static final String EVENT_QUEUE_STATE_NAME = "eventQueuesStateName";
	private static final String NFA_FACTORY_STATE_NAME = "nfaFactoryStateName";

	private transient MapState<String, NFA<IN1>> nfaOperatorState;
	private transient MapState<Long, List<IN1>> elementQueueState;
	private transient ValueState<NFA<IN1>> defaultNfaOperatorState;

	private transient ListState<NFAFactory<IN1>> nfaFactoryState;
	private final NFAFactory<IN1> nfaFactory;

	private transient InternalTimerService<VoidNamespace> timerService;

	/**
	 * The last seen watermark. This will be used to
	 * decide if an incoming element is late or not.
	 */
	private long lastWatermark;

	public AbstractCoCepPatternOperator(
		final TypeSerializer<IN1> inputSerializer,
		final boolean isProcessingTime,
		final NFAFactory<IN1> nfaFactory) {

		this.inputSerializer = Preconditions.checkNotNull(inputSerializer);
		this.isProcessingTime = Preconditions.checkNotNull(isProcessingTime);
		this.nfaFactory = Preconditions.checkNotNull(nfaFactory);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		if (defaultNfaOperatorState == null) {
			defaultNfaOperatorState = getRuntimeContext().getState(
				new ValueStateDescriptor<>(DEFAULT_NFA_FACTORY_STATE_NAME, new NFA.NFASerializer<IN1>(inputSerializer))
			);
		}

		if (nfaOperatorState == null) {
			nfaOperatorState = getRuntimeContext().getMapState(
				new MapStateDescriptor<>(
					NFA_OPERATOR_STATE_NAME,
					StringSerializer.INSTANCE,
					new NFA.NFAWithoutStatesSerializer<>(inputSerializer)
				));
		}

		if (elementQueueState == null) {
			elementQueueState = getRuntimeContext().getMapState(
				new MapStateDescriptor<>(
					EVENT_QUEUE_STATE_NAME,
					LongSerializer.INSTANCE,
					new ListSerializer<>(inputSerializer)
				)
			);
		}

		if (nfaFactoryState == null) {
			nfaFactoryState = context.getOperatorStateStore().getListState(
				new ListStateDescriptor<NFAFactory<IN1>>(NFA_FACTORY_STATE_NAME, new NFAFactory.NFAFactorySerializer<>())
			);
		}
	}

	@Override
	public void open() throws Exception {
		super.open();

		timerService = getInternalTimerService(
			"watermark-callbacks",
			VoidNamespaceSerializer.INSTANCE,
			this);
	}

	@Override
	public void processElement2(StreamRecord<Pattern<IN1, ?>> element) throws Exception {
		final Pattern<IN1, ?> pattern = element.getValue();
		final NFAFactory<IN1> inNfaFactory = NFACompiler.compileFactory(pattern, true/*TODO*/);

		nfaFactoryState.add(inNfaFactory);
	}

	@Override
	public void processElement1(StreamRecord<IN1> element) throws Exception {
		if (isProcessingTime) {
			// there can be no out of order elements in processing time
			for (NFAFactory<IN1> factory : nfaFactoryState.get()) {
				NFA<IN1> nfa = getNFA("ABC");
				processEvent(nfa, element.getValue(), getProcessingTimeService().getCurrentProcessingTime());
				updateNFA("ABC", nfa);
			}
		} else {

			long timestamp = element.getTimestamp();
			IN1 value = element.getValue();

			// In event-time processing we assume correctness of the watermark.
			// Events with timestamp smaller than the last seen watermark are considered late.
			// Late events are put in a dedicated side output, if the user has specified one.

			if (timestamp >= lastWatermark) {

				// we have an event with a valid timestamp, so
				// we buffer it until we receive the proper watermark.

				saveRegisterWatermarkTimer();

				List<IN1> elementsForTimestamp =  elementQueueState.get(timestamp);
				if (elementsForTimestamp == null) {
					elementsForTimestamp = new ArrayList<>();
				}

				if (getExecutionConfig().isObjectReuseEnabled()) {
					// copy the StreamRecord so that it cannot be changed
					elementsForTimestamp.add(inputSerializer.copy(value));
				} else {
					elementsForTimestamp.add(element.getValue());
				}
				elementQueueState.put(timestamp, elementsForTimestamp);
			}
		}
	}

	/**
	 * Registers a timer for {@code current watermark + 1}, this means that we get triggered
	 * whenever the watermark advances, which is what we want for working off the queue of
	 * buffered elements.
	 */
	private void saveRegisterWatermarkTimer() {
		long currentWatermark = timerService.currentWatermark();
		// protect against overflow
		if (currentWatermark + 1 > currentWatermark) {
			timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, currentWatermark + 1);
		}
	}

	private static final String DUMMY_NFA_KEY = "DEFAULT_NFA";

	@Override
	public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {

		// 1) get the queue of pending elements for the key and the corresponding NFA,
		// 2) process the pending elements in event time order by feeding them in the NFA
		// 3) advance the time to the current watermark, so that expired patterns are discarded.
		// 4) update the stored state for the key, by only storing the new NFA and priority queue iff they
		//		have state to be used later.
		// 5) update the last seen watermark.

		// STEP 1
		PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();
		NFA<IN1> nfa = getNFA(DUMMY_NFA_KEY);

		// STEP 2
		while (!sortedTimestamps.isEmpty() && sortedTimestamps.peek() <= timerService.currentWatermark()) {
			long timestamp = sortedTimestamps.poll();
			for (IN1 element: elementQueueState.get(timestamp)) {
				processEvent(nfa, element, timestamp);
			}
			elementQueueState.remove(timestamp);
		}

		// STEP 3
		advanceTime(nfa, timerService.currentWatermark());

		// STEP 4
		if (sortedTimestamps.isEmpty()) {
			elementQueueState.clear();
		}
		updateNFA(DUMMY_NFA_KEY, nfa);

		if (!sortedTimestamps.isEmpty() || !nfa.isEmpty()) {
			saveRegisterWatermarkTimer();
		}

		// STEP 5
		updateLastSeenWatermark(timerService.currentWatermark());
	}

	@Override
	public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
		// not used
	}

	private void updateLastSeenWatermark(long timestamp) {
		this.lastWatermark = timestamp;
	}

	private NFA<IN1> getNFA(String key) throws Exception {
		final NFA<IN1> oldNfa = defaultNfaOperatorState.value();
		if (oldNfa != null) {
			oldNfa.resetNFAChanged();
			nfaOperatorState.put(key, oldNfa);
			defaultNfaOperatorState.clear();
		}

		NFA<IN1> nfa = nfaOperatorState.get(key);
		final NFAFactory<IN1> nfaFactory = getNfaFactory(key);

		if (nfa != null) {
			nfa.setNfaFactory(nfaFactory);
			return nfa;
		} else {
			return nfaFactory.createNFA();
		}
	}

	private NFAFactory<IN1> getNfaFactory(String key) throws Exception {
		NFAFactory<IN1> nfaFactory = null;
		for (NFAFactory<IN1> factory : nfaFactoryState.get()) {
			if (true/*TODO key handling*/) {
				nfaFactory = factory;
				break;
			}
		}
		if (nfaFactory == null) {
			return this.nfaFactory;
		} else {
			return nfaFactory;
		}
	}

	private void updateNFA(String key, NFA<IN1> nfa) throws Exception {
		if (nfa.isNFAChanged()) {
			if (nfa.isEmpty()) {
				nfaOperatorState.clear();
			} else {
				nfa.resetNFAChanged();
				nfaOperatorState.put(key, nfa);
			}
		}
	}

	private PriorityQueue<Long> getSortedTimestamps() throws Exception {
		PriorityQueue<Long> sortedTimestamps = new PriorityQueue<>();
		for (Long timestamp: elementQueueState.keys()) {
			sortedTimestamps.offer(timestamp);
		}
		return sortedTimestamps;
	}

	/**
	 * Process the given event by giving it to the NFA and outputting the produced set of matched
	 * event sequences.
	 *
	 * @param nfa NFA to be used for the event detection
	 * @param event The current event to be processed
	 * @param timestamp The timestamp of the event
	 */
	protected abstract void processEvent(NFA<IN1> nfa, IN1 event, long timestamp);

	/**
	 * Advances the time for the given NFA to the given timestamp. This can lead to pruning and
	 * timeouts.
	 *
	 * @param nfa to advance the time for
	 * @param timestamp to advance the time to
	 */
	protected abstract void advanceTime(NFA<IN1> nfa, long timestamp);


	//////////////////////			Testing Methods			//////////////////////

	@VisibleForTesting
	public boolean hasNonEmptyNFA(KEY key) throws Exception {
		setCurrentKey(key);
		return nfaOperatorState.get(DUMMY_NFA_KEY) != null;
	}

	@VisibleForTesting
	public boolean hasNonEmptyPQ(KEY key) throws Exception {
		setCurrentKey(key);
		return elementQueueState.keys().iterator().hasNext();
	}

	@VisibleForTesting
	public int getPQSize(KEY key) throws Exception {
		setCurrentKey(key);
		int counter = 0;
		for (List<IN1> elements: elementQueueState.values()) {
			counter += elements.size();
		}
		return counter;
	}
}
