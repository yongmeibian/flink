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

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.cep.nfa.compiler.NFAFactory;
import org.apache.flink.cep.nfa.compiler.NFAStateNameHandler;
import org.apache.flink.cep.operator.AbstractKeyedCEPPatternOperator;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

/**
 * Non-deterministic finite automaton implementation.
 *
 * <p>The {@link AbstractKeyedCEPPatternOperator CEP operator}
 * keeps one NFA per key, for keyed input streams, and a single global NFA for non-keyed ones.
 * When an event gets processed, it updates the NFA's internal state machine.
 *
 * <p>An event that belongs to a partially matched sequence is kept in an internal
 * {@link SharedBuffer buffer}, which is a memory-optimized data-structure exactly for
 * this purpose. Events in the buffer are removed when all the matched sequences that
 * contain them are:
 * <ol>
 *  <li>emitted (success)</li>
 *  <li>discarded (patterns containing NOT)</li>
 *  <li>timed-out (windowed patterns)</li>
 * </ol>
 *
 * <p>The implementation is strongly based on the paper "Efficient Pattern Matching over Event Streams".
 *
 * @param <T> Type of the processed events
 * @see <a href="https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf">
 * https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf</a>
 */
public class NFA<T> implements Serializable {

	private static final long serialVersionUID = 2957674889294717265L;

	/////////////////////			Backwards Compatibility Fields			/////////////////////

	/**
	 * @deprecated Used only for backwards compatibility.
	 * Look at the {@link #eventSharedBuffer} for its replacement.
	 */
	@Deprecated
	private final SharedBuffer<State<T>, T> sharedBuffer = null;

	/**
	 * @deprecated Used only for backward compatibility.
	 */
	@Deprecated
	private int startEventCounter;

	//////////////////			End of Backwards Compatibility Fields			//////////////////

	/**
	 * @deprecated Used only for backwards compatibility.
	 */
	@Deprecated
	private Set<State<T>> states;

	/**
	 * The length of a windowed pattern, as specified using the
	 * {@link org.apache.flink.cep.pattern.Pattern#within(Time)}  Pattern.within(Time)}
	 * method.
	 */
	@Deprecated
	private long windowTime;

	/**
	 * A flag indicating if we want timed-out patterns (in case of windowed patterns)
	 * to be emitted ({@code true}), or silently discarded ({@code false}).
	 */
	@Deprecated
	private boolean handleTimeout;

	//////////////////			End of Backwards Compatibility Fields			//////////////////

	/**
	 * Current set of {@link ComputationState computation states} within the state machine.
	 * These are the "active" intermediate states that are waiting for new matching
	 * events to transition to new valid states.
	 */
	private transient Queue<ComputationState<T>> computationStates;

	/**
	 * Buffer used to store the matched events.
	 */
	private SharedBuffer<String, T> eventSharedBuffer;

	private transient NFAFactory<T> nfaFactory;

	/**
	 * Flag indicating whether the matching status of the state machine has changed.
	 */
	private boolean nfaChanged;

	public NFA(final NFAFactory<T> nfaFactory) {

		this.eventSharedBuffer = new SharedBuffer<>();
		this.computationStates = new LinkedList<>();
		this.nfaChanged = false;

		this.nfaFactory = nfaFactory;
		addStartStates(nfaFactory.getStates());
	}

	private NFA() {
		this.eventSharedBuffer = new SharedBuffer<>();
		this.computationStates = new LinkedList<>();
		this.nfaChanged = false;
	}

	public void setNfaFactory(NFAFactory<T> nfaFactory) {
		this.nfaFactory = nfaFactory;
	}

	public void addStartStates(final Collection<State<T>> newStates) {
		for (State<T> state: newStates) {
			if (state.isStart()) {
				computationStates.add(ComputationState.createStartState(this, state));
			}
		}
	}

	/**
	 * Check if the NFA has finished processing all incoming data so far. That is
	 * when the buffer keeping the matches is empty.
	 *
	 * @return {@code true} if there are no elements in the {@link SharedBuffer},
	 * {@code false} otherwise.
	 */
	public boolean isEmpty() {
		return eventSharedBuffer.isEmpty();
	}

	/**
	 * Check if the matching status of the NFA has changed so far.
	 *
	 * @return {@code true} if matching status has changed, {@code false} otherwise
	 */
	public boolean isNFAChanged() {
		return nfaChanged;
	}

	/**
	 * Reset {@link #nfaChanged} to {@code false}.
	 */
	public void resetNFAChanged() {
		this.nfaChanged = false;
	}

	/**
	 * Processes the next input event. If some of the computations reach a final state then the
	 * resulting event sequences are returned. If computations time out and timeout handling is
	 * activated, then the timed out event patterns are returned.
	 *
	 * <p>If computations reach a stop state, the path forward is discarded and currently constructed path is returned
	 * with the element that resulted in the stop state.
	 *
	 * @param event The current event to be processed or null if only pruning shall be done
	 * @param timestamp The timestamp of the current event
	 * @return Tuple of the collection of matched patterns (e.g. the result of computations which have
	 * reached a final state) and the collection of timed out patterns (if timeout handling is
	 * activated)
	 */
	public Tuple2<Collection<Map<String, List<T>>>, Collection<Tuple2<Map<String, List<T>>, Long>>> process(final T event, final long timestamp) {
		final int numberComputationStates = computationStates.size();
		final Collection<Map<String, List<T>>> result = new ArrayList<>();
		final Collection<Tuple2<Map<String, List<T>>, Long>> timeoutResult = new ArrayList<>();

		// iterate over all current computations
		for (int i = 0; i < numberComputationStates; i++) {
			ComputationState<T> computationState = computationStates.poll();

			final Collection<ComputationState<T>> newComputationStates;

			if (!computationState.isStartState() &&
				nfaFactory.getWindowTime() > 0L &&
				timestamp - computationState.getStartTimestamp() >= nfaFactory.getWindowTime()) {

				if (nfaFactory.isTimeoutHandling()) {
					// extract the timed out event pattern
					Map<String, List<T>> timedOutPattern = extractCurrentMatches(computationState);
					timeoutResult.add(Tuple2.of(timedOutPattern, timestamp));
				}

				eventSharedBuffer.release(
						NFAStateNameHandler.getOriginalNameFromInternal(computationState.getPreviousState().getName()),
						computationState.getEvent(),
						computationState.getTimestamp(),
						computationState.getCounter());

				newComputationStates = Collections.emptyList();
				nfaChanged = true;
			} else if (event != null) {
				newComputationStates = computeNextStates(computationState, event, timestamp);

				if (newComputationStates.size() != 1) {
					nfaChanged = true;
				} else if (!newComputationStates.iterator().next().equals(computationState)) {
					nfaChanged = true;
				}
			} else {
				newComputationStates = Collections.singleton(computationState);
			}

			//delay adding new computation states in case a stop state is reached and we discard the path.
			final Collection<ComputationState<T>> statesToRetain = new ArrayList<>();
			//if stop state reached in this path
			boolean shouldDiscardPath = false;
			for (final ComputationState<T> newComputationState: newComputationStates) {

				if (newComputationState.isFinalState()) {
					// we've reached a final state and can thus retrieve the matching event sequence
					Map<String, List<T>> matchedPattern = extractCurrentMatches(newComputationState);
					result.add(matchedPattern);

					// remove found patterns because they are no longer needed
					eventSharedBuffer.release(
							NFAStateNameHandler.getOriginalNameFromInternal(
									newComputationState.getPreviousState().getName()),
							newComputationState.getEvent(),
							newComputationState.getTimestamp(),
							newComputationState.getCounter());
				} else if (newComputationState.isStopState()) {
					//reached stop state. release entry for the stop state
					shouldDiscardPath = true;
					eventSharedBuffer.release(
							NFAStateNameHandler.getOriginalNameFromInternal(
									newComputationState.getPreviousState().getName()),
							newComputationState.getEvent(),
							newComputationState.getTimestamp(),
							newComputationState.getCounter());
				} else {
					// add new computation state; it will be processed once the next event arrives
					statesToRetain.add(newComputationState);
				}
			}

			if (shouldDiscardPath) {
				// a stop state was reached in this branch. release branch which results in removing previous event from
				// the buffer
				for (final ComputationState<T> state : statesToRetain) {
					eventSharedBuffer.release(
							NFAStateNameHandler.getOriginalNameFromInternal(
									state.getPreviousState().getName()),
							state.getEvent(),
							state.getTimestamp(),
							state.getCounter());
				}
			} else {
				computationStates.addAll(statesToRetain);
			}

		}

		// prune shared buffer based on window length
		if (nfaFactory.getWindowTime() > 0L) {
			long pruningTimestamp = timestamp - nfaFactory.getWindowTime();

			if (pruningTimestamp < timestamp) {
				// the check is to guard against underflows

				// remove all elements which are expired
				// with respect to the window length
				if (eventSharedBuffer.prune(pruningTimestamp)) {
					nfaChanged = true;
				}
			}
		}

		return Tuple2.of(result, timeoutResult);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof NFA) {
			@SuppressWarnings("unchecked")
			NFA<T> other = (NFA<T>) obj;

			return eventSharedBuffer.equals(other.eventSharedBuffer) &&
				windowTime == other.windowTime;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(eventSharedBuffer, windowTime);
	}

	private static <T> boolean isEquivalentState(final State<T> s1, final State<T> s2) {
		return s1.getName().equals(s2.getName());
	}

	public NFAFactory<T> getNFAFactory() {
		return nfaFactory;
	}

	/**
	 * Class for storing resolved transitions. It counts at insert time the number of
	 * branching transitions both for IGNORE and TAKE actions.
 	 */
	private static class OutgoingEdges<T> {
		private List<StateTransition<T>> edges = new ArrayList<>();

		private final State<T> currentState;

		private int totalTakeBranches = 0;
		private int totalIgnoreBranches = 0;

		OutgoingEdges(final State<T> currentState) {
			this.currentState = currentState;
		}

		void add(StateTransition<T> edge) {

			if (!isSelfIgnore(edge)) {
				if (edge.getAction() == StateTransitionAction.IGNORE) {
					totalIgnoreBranches++;
				} else if (edge.getAction() == StateTransitionAction.TAKE) {
					totalTakeBranches++;
				}
			}

			edges.add(edge);
		}

		int getTotalIgnoreBranches() {
			return totalIgnoreBranches;
		}

		int getTotalTakeBranches() {
			return totalTakeBranches;
		}

		List<StateTransition<T>> getEdges() {
			return edges;
		}

		private boolean isSelfIgnore(final StateTransition<T> edge) {
			return isEquivalentState(edge.getTargetState(), currentState) &&
				edge.getAction() == StateTransitionAction.IGNORE;
		}
	}


	/**
	 * Computes the next computation states based on the given computation state, the current event,
	 * its timestamp and the internal state machine. The algorithm is:
	 *<ol>
	 *     <li>Decide on valid transitions and number of branching paths. See {@link OutgoingEdges}</li>
	 * 	   <li>Perform transitions:
	 * 	   	<ol>
	 *          <li>IGNORE (links in {@link SharedBuffer} will still point to the previous event)</li>
	 *          <ul>
	 *              <li>do not perform for Start State - special case</li>
	 *          	<li>if stays in the same state increase the current stage for future use with number of outgoing edges</li>
	 *          	<li>if after PROCEED increase current stage and add new stage (as we change the state)</li>
	 *          	<li>lock the entry in {@link SharedBuffer} as it is needed in the created branch</li>
	 *      	</ul>
	 *      	<li>TAKE (links in {@link SharedBuffer} will point to the current event)</li>
	 *          <ul>
	 *              <li>add entry to the shared buffer with version of the current computation state</li>
	 *              <li>add stage and then increase with number of takes for the future computation states</li>
	 *              <li>peek to the next state if it has PROCEED path to a Final State, if true create Final
	 *              ComputationState to emit results</li>
	 *          </ul>
	 *      </ol>
	 *     </li>
	 * 	   <li>Handle the Start State, as it always have to remain </li>
	 *     <li>Release the corresponding entries in {@link SharedBuffer}.</li>
	 *</ol>
	 *
	 * @param computationState Current computation state
	 * @param event Current event which is processed
	 * @param timestamp Timestamp of the current event
	 * @return Collection of computation states which result from the current one
	 */
	private Collection<ComputationState<T>> computeNextStates(
			final ComputationState<T> computationState,
			final T event,
			final long timestamp) {

		final OutgoingEdges<T> outgoingEdges = createDecisionGraph(computationState, event);

		// Create the computing version based on the previously computed edges
		// We need to defer the creation of computation states until we know how many edges start
		// at this computation state so that we can assign proper version
		final List<StateTransition<T>> edges = outgoingEdges.getEdges();
		int takeBranchesToVisit = Math.max(0, outgoingEdges.getTotalTakeBranches() - 1);
		int ignoreBranchesToVisit = outgoingEdges.getTotalIgnoreBranches();
		int totalTakeToSkip = Math.max(0, outgoingEdges.getTotalTakeBranches() - 1);

		final List<ComputationState<T>> resultingComputationStates = new ArrayList<>();
		for (StateTransition<T> edge : edges) {
			switch (edge.getAction()) {
				case IGNORE: {
					if (!computationState.isStartState()) {
						final DeweyNumber version;
						if (isEquivalentState(edge.getTargetState(), computationState.getState())) {
							//Stay in the same state (it can be either looping one or singleton)
							final int toIncrease = calculateIncreasingSelfState(
								outgoingEdges.getTotalIgnoreBranches(),
								outgoingEdges.getTotalTakeBranches());
							version = computationState.getVersion().increase(toIncrease);
						} else {
							//IGNORE after PROCEED
							version = computationState.getVersion()
								.increase(totalTakeToSkip + ignoreBranchesToVisit)
								.addStage();
							ignoreBranchesToVisit--;
						}

						addComputationState(
								resultingComputationStates,
								edge.getTargetState(),
								computationState.getPreviousState(),
								computationState.getEvent(),
								computationState.getCounter(),
								computationState.getTimestamp(),
								version,
								computationState.getStartTimestamp()
						);
					}
				}
				break;
				case TAKE:
					final State<T> nextState = edge.getTargetState();
					final State<T> currentState = edge.getSourceState();
					final State<T> previousState = computationState.getPreviousState();

					final T previousEvent = computationState.getEvent();

					final DeweyNumber currentVersion = computationState.getVersion().increase(takeBranchesToVisit);
					final DeweyNumber nextVersion = new DeweyNumber(currentVersion).addStage();
					takeBranchesToVisit--;

					final int counter;
					final long startTimestamp;
					if (computationState.isStartState()) {
						startTimestamp = timestamp;
						counter = eventSharedBuffer.put(
							NFAStateNameHandler.getOriginalNameFromInternal(
									currentState.getName()),
							event,
							timestamp,
							currentVersion);
					} else {
						startTimestamp = computationState.getStartTimestamp();
						counter = eventSharedBuffer.put(
							NFAStateNameHandler.getOriginalNameFromInternal(
									currentState.getName()),
							event,
							timestamp,
							NFAStateNameHandler.getOriginalNameFromInternal(
									previousState.getName()),
							previousEvent,
							computationState.getTimestamp(),
							computationState.getCounter(),
							currentVersion);
					}

					addComputationState(
							resultingComputationStates,
							nextState,
							currentState,
							event,
							counter,
							timestamp,
							nextVersion,
							startTimestamp);

					//check if newly created state is optional (have a PROCEED path to Final state)
					final State<T> finalState = findFinalStateAfterProceed(nextState, event, computationState);
					if (finalState != null) {
						addComputationState(
								resultingComputationStates,
								finalState,
								currentState,
								event,
								counter,
								timestamp,
								nextVersion,
								startTimestamp);
					}
					break;
			}
		}

		if (computationState.isStartState()) {
			int totalBranches = calculateIncreasingSelfState(
					outgoingEdges.getTotalIgnoreBranches(),
					outgoingEdges.getTotalTakeBranches());

			DeweyNumber startVersion = computationState.getVersion().increase(totalBranches);
			ComputationState<T> startState = ComputationState.createStartState(this, computationState.getState(), startVersion);
			resultingComputationStates.add(startState);
		}

		if (computationState.getEvent() != null) {
			// release the shared entry referenced by the current computation state.
			eventSharedBuffer.release(
					NFAStateNameHandler.getOriginalNameFromInternal(
							computationState.getPreviousState().getName()),
					computationState.getEvent(),
					computationState.getTimestamp(),
					computationState.getCounter());
		}

		return resultingComputationStates;
	}

	private void addComputationState(
			List<ComputationState<T>> computationStates,
			State<T> currentState,
			State<T> previousState,
			T event,
			int counter,
			long timestamp,
			DeweyNumber version,
			long startTimestamp) {
		ComputationState<T> computationState = ComputationState.createState(
				this, currentState, previousState, event, counter, timestamp, version, startTimestamp);
		computationStates.add(computationState);

		String originalStateName = NFAStateNameHandler.getOriginalNameFromInternal(previousState.getName());
		eventSharedBuffer.lock(originalStateName, event, timestamp, counter);
	}

	private State<T> findFinalStateAfterProceed(State<T> state, T event, ComputationState<T> computationState) {
		final Stack<State<T>> statesToCheck = new Stack<>();
		statesToCheck.push(state);

		try {
			while (!statesToCheck.isEmpty()) {
				final State<T> currentState = statesToCheck.pop();
				for (StateTransition<T> transition : currentState.getStateTransitions()) {
					if (transition.getAction() == StateTransitionAction.PROCEED &&
							checkFilterCondition(computationState, transition.getCondition(), event)) {
						if (transition.getTargetState().isFinal()) {
							return transition.getTargetState();
						} else {
							statesToCheck.push(transition.getTargetState());
						}
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Failure happened in filter function.", e);
		}

		return null;
	}

	private int calculateIncreasingSelfState(int ignoreBranches, int takeBranches) {
		return takeBranches == 0 && ignoreBranches == 0 ? 0 : ignoreBranches + Math.max(1, takeBranches);
	}

	private OutgoingEdges<T> createDecisionGraph(ComputationState<T> computationState, T event) {
		final OutgoingEdges<T> outgoingEdges = new OutgoingEdges<>(computationState.getState());

		final Stack<State<T>> states = new Stack<>();
		states.push(computationState.getState());

		//First create all outgoing edges, so to be able to reason about the Dewey version
		while (!states.isEmpty()) {
			State<T> currentState = states.pop();
			Collection<StateTransition<T>> stateTransitions = currentState.getStateTransitions();

			// check all state transitions for each state
			for (StateTransition<T> stateTransition : stateTransitions) {
				try {
					if (checkFilterCondition(computationState, stateTransition.getCondition(), event)) {
						// filter condition is true
						switch (stateTransition.getAction()) {
							case PROCEED:
								// simply advance the computation state, but apply the current event to it
								// PROCEED is equivalent to an epsilon transition
								states.push(stateTransition.getTargetState());
								break;
							case IGNORE:
							case TAKE:
								outgoingEdges.add(stateTransition);
								break;
						}
					}
				} catch (Exception e) {
					throw new RuntimeException("Failure happened in filter function.", e);
				}
			}
		}
		return outgoingEdges;
	}

	private boolean checkFilterCondition(ComputationState<T> computationState, IterativeCondition<T> condition, T event) throws Exception {
		return condition == null || condition.filter(event, computationState.getConditionContext());
	}

	/**
	 * Extracts all the sequences of events from the start to the given computation state. An event
	 * sequence is returned as a map which contains the events and the names of the states to which
	 * the events were mapped.
	 *
	 * @param computationState The end computation state of the extracted event sequences
	 * @return Collection of event sequences which end in the given computation state
	 */
	Map<String, List<T>> extractCurrentMatches(final ComputationState<T> computationState) {
		if (computationState.getPreviousState() == null) {
			return new HashMap<>();
		}

		List<Map<String, List<T>>> paths = eventSharedBuffer.extractPatterns(
			NFAStateNameHandler.getOriginalNameFromInternal(computationState.getPreviousState().getName()),
			computationState.getEvent(),
			computationState.getTimestamp(),
			computationState.getCounter(),
			computationState.getVersion());

		if (paths.isEmpty()) {
			return new HashMap<>();
		}
		// for a given computation state, we cannot have more than one matching patterns.
		Preconditions.checkState(paths.size() == 1);

		Map<String, List<T>> result = new HashMap<>();
		Map<String, List<T>> path = paths.get(0);
		for (String key : path.keySet()) {
			List<T> events = path.get(key);

			List<T> values = result.get(key);
			if (values == null) {
				values = new ArrayList<>(events.size());
				result.put(key, values);
			}

			values.addAll(events);
		}
		return result;
	}

	//////////////////////			New Serialization			//////////////////////

	/**
	 * The {@link TypeSerializerConfigSnapshot} serializer configuration to be stored with the managed state.
	 */
	public static final class NFASerializerConfigSnapshot<T> extends CompositeTypeSerializerConfigSnapshot {

		private static final int VERSION = 2;

		/** This empty constructor is required for deserializing the configuration. */
		public NFASerializerConfigSnapshot() {}

		public NFASerializerConfigSnapshot(
				TypeSerializer<T> eventSerializer,
				TypeSerializer<SharedBuffer<String, T>> sharedBufferSerializer) {

			super(eventSerializer, sharedBufferSerializer);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}

		@Override
		public int[] getCompatibleVersions() {
			return new int[] {VERSION, 1};
		}
	}

	/**
	 * Base class for {@link TypeSerializer}s for {@link NFA} that uses Flink custom serialization.
	 * @param <T> Type of events
	 */
	public abstract static class BaseNFASerializer<T> extends TypeSerializer<NFA<T>> {
		protected abstract TypeSerializer<SharedBuffer<String, T>> getSharedBufferSerializer();

		protected abstract TypeSerializer<T> getEventSerializer();

		protected abstract void readFactoryIfNecessary(DataInputView source) throws IOException;

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<NFA<T>> duplicate() {
			return this;
		}

		@Override
		public NFA<T> createInstance() {
			return null;
		}

		@Override
		public NFA<T> copy(NFA<T> from) {
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos);

				serialize(from, new DataOutputViewStreamWrapper(oos));

				oos.close();
				baos.close();

				byte[] data = baos.toByteArray();

				ByteArrayInputStream bais = new ByteArrayInputStream(data);
				ObjectInputStream ois = new ObjectInputStream(bais);

				@SuppressWarnings("unchecked")
				NFA<T> copy = deserialize(new DataInputViewStreamWrapper(ois));
				ois.close();
				bais.close();
				return copy;
			} catch (IOException e) {
				throw new RuntimeException("Could not copy NFA.", e);
			}
		}

		@Override
		public NFA<T> copy(NFA<T> from, NFA<T> reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(NFA<T> record, DataOutputView target) throws IOException {
			getSharedBufferSerializer().serialize(record.eventSharedBuffer, target);

			target.writeInt(record.computationStates.size());

			StringSerializer stateNameSerializer = StringSerializer.INSTANCE;
			LongSerializer timestampSerializer = LongSerializer.INSTANCE;
			DeweyNumber.DeweyNumberSerializer versionSerializer = new DeweyNumber.DeweyNumberSerializer();

			for (ComputationState<T> computationState: record.computationStates) {
				stateNameSerializer.serialize(computationState.getState().getName(), target);
				stateNameSerializer.serialize(computationState.getPreviousState() == null
					? null : computationState.getPreviousState().getName(), target);

				timestampSerializer.serialize(computationState.getTimestamp(), target);
				versionSerializer.serialize(computationState.getVersion(), target);
				timestampSerializer.serialize(computationState.getStartTimestamp(), target);
				target.writeInt(computationState.getCounter());

				if (computationState.getEvent() == null) {
					target.writeBoolean(false);
				} else {
					target.writeBoolean(true);
					getEventSerializer().serialize(computationState.getEvent(), target);
				}
			}
		}

		@Override
		public NFA<T> deserialize(DataInputView source) throws IOException {
			readFactoryIfNecessary(source);

			NFA<T> nfa = new NFA<>();
			nfa.eventSharedBuffer = getSharedBufferSerializer().deserialize(source);

			Queue<ComputationState<T>> computationStates = new LinkedList<>();
			StringSerializer stateNameSerializer = StringSerializer.INSTANCE;
			LongSerializer timestampSerializer = LongSerializer.INSTANCE;
			DeweyNumber.DeweyNumberSerializer versionSerializer = new DeweyNumber.DeweyNumberSerializer();

			int computationStateNo = source.readInt();
			for (int i = 0; i < computationStateNo; i++) {
				String state = stateNameSerializer.deserialize(source);
				String prevState = stateNameSerializer.deserialize(source);
				long timestamp = timestampSerializer.deserialize(source);
				DeweyNumber version = versionSerializer.deserialize(source);
				long startTimestamp = timestampSerializer.deserialize(source);
				int counter = source.readInt();

				T event = null;
				if (source.readBoolean()) {
					event = getEventSerializer().deserialize(source);
				}

				computationStates.add(ComputationState.createState(
					nfa, state, prevState, event, counter, timestamp, version, startTimestamp));
			}

			nfa.computationStates = computationStates;
			return nfa;
		}

		@Override
		public NFA<T> deserialize(NFA<T> reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			SharedBuffer<String, T> sharedBuffer = getSharedBufferSerializer().deserialize(source);
			getSharedBufferSerializer().serialize(sharedBuffer, target);

			StringSerializer stateNameSerializer = StringSerializer.INSTANCE;
			LongSerializer timestampSerializer = LongSerializer.INSTANCE;
			DeweyNumber.DeweyNumberSerializer versionSerializer = new DeweyNumber.DeweyNumberSerializer();

			int computationStateNo = source.readInt();
			target.writeInt(computationStateNo);

			for (int i = 0; i < computationStateNo; i++) {
				String stateName = stateNameSerializer.deserialize(source);
				stateNameSerializer.serialize(stateName, target);

				String prevStateName = stateNameSerializer.deserialize(source);
				stateNameSerializer.serialize(prevStateName, target);

				long timestamp = timestampSerializer.deserialize(source);
				timestampSerializer.serialize(timestamp, target);

				DeweyNumber version = versionSerializer.deserialize(source);
				versionSerializer.serialize(version, target);

				long startTimestamp = timestampSerializer.deserialize(source);
				timestampSerializer.serialize(startTimestamp, target);

				int counter = source.readInt();
				target.writeInt(counter);

				boolean hasEvent = source.readBoolean();
				target.writeBoolean(hasEvent);
				if (hasEvent) {
					T event = getEventSerializer().deserialize(source);
					getEventSerializer().serialize(event, target);
				}
			}
		}

		@Override
		public boolean equals(Object obj) {
			return obj == this ||
					(obj != null && obj.getClass().equals(getClass()) &&
					getSharedBufferSerializer().equals(((BaseNFASerializer) obj).getSharedBufferSerializer()) &&
					getEventSerializer().equals(((BaseNFASerializer) obj).getEventSerializer()));
		}

		@Override
		public boolean canEqual(Object obj) {
			return true;
		}

		@Override
		public int hashCode() {
			return Objects.hash(getSharedBufferSerializer(), getEventSerializer());
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			return new NFASerializerConfigSnapshot<>(getEventSerializer(), getSharedBufferSerializer());
		}

	}

	/**
	 * A {@link TypeSerializer} for {@link NFA} that uses Flink custom serialization that expects static info to be
	 * serialized within NFA.
	 */
	public static class NFASerializer<T> extends BaseNFASerializer<T> {

		private static final long serialVersionUID = 2098282423980597010L;

		private TypeSerializer<SharedBuffer<String, T>> sharedBufferSerializer;

		private TypeSerializer<T> eventSerializer;

		@Override
		protected TypeSerializer<SharedBuffer<String, T>> getSharedBufferSerializer() {
			return sharedBufferSerializer;
		}

		@Override
		protected TypeSerializer<T> getEventSerializer() {
			return eventSerializer;
		}

		@Override
		protected void readFactoryIfNecessary(DataInputView source) throws IOException {
			new NFAFactory.NFAFactorySerializer<T>().deserialize(source);
		}

		@Override
		public void serialize(NFA<T> record, DataOutputView target) throws IOException {
			throw new UnsupportedOperationException("Old serializer should not be used for serializing state.");
		}

		private NFASerializer(
			TypeSerializer<T> typeSerializer,
			TypeSerializer<SharedBuffer<String, T>> sharedBufferSerializer) {
			this.eventSerializer = typeSerializer;
			this.sharedBufferSerializer = sharedBufferSerializer;
		}

		public NFASerializer(TypeSerializer<T> typeSerializer) {
			this.eventSerializer = typeSerializer;
			this.sharedBufferSerializer = new SharedBuffer.SharedBufferSerializer<>(
				StringSerializer.INSTANCE,
				typeSerializer);
		}

		@Override
		public CompatibilityResult<NFA<T>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			if (configSnapshot instanceof NFASerializerConfigSnapshot) {
				List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> serializersAndConfigs =
					((NFASerializerConfigSnapshot) configSnapshot).getNestedSerializersAndConfigs();

				CompatibilityResult<T> eventCompatResult = CompatibilityUtil.resolveCompatibilityResult(
					serializersAndConfigs.get(0).f0,
					UnloadableDummyTypeSerializer.class,
					serializersAndConfigs.get(0).f1,
					eventSerializer);

				CompatibilityResult<SharedBuffer<String, T>> sharedBufCompatResult =
					CompatibilityUtil.resolveCompatibilityResult(
						serializersAndConfigs.get(1).f0,
						UnloadableDummyTypeSerializer.class,
						serializersAndConfigs.get(1).f1,
						sharedBufferSerializer);

				if (!sharedBufCompatResult.isRequiresMigration() && !eventCompatResult.isRequiresMigration()) {
					return CompatibilityResult.compatible();
				} else {
					if (eventCompatResult.getConvertDeserializer() != null &&
						sharedBufCompatResult.getConvertDeserializer() != null) {
						return CompatibilityResult.requiresMigration(
							new NFASerializer<>(
								new TypeDeserializerAdapter<>(eventCompatResult.getConvertDeserializer()),
								new TypeDeserializerAdapter<>(sharedBufCompatResult.getConvertDeserializer())));
					}
				}
			}

			return CompatibilityResult.requiresMigration();
		}
	}

	/**
	 * A {@link TypeSerializer} for {@link NFA} that uses Custom serialization. This version handles formats with
	 * and without serialized static info.
	 */
	public static class NFAWithoutStatesSerializer<T> extends BaseNFASerializer<T> {

		private static final long serialVersionUID = 6739159374183883274L;

		private TypeSerializer<SharedBuffer<String, T>> sharedBufferSerializer;

		private TypeSerializer<T> eventSerializer;

		@Override
		protected TypeSerializer<SharedBuffer<String, T>> getSharedBufferSerializer() {
			return sharedBufferSerializer;
		}

		@Override
		protected TypeSerializer<T> getEventSerializer() {
			return eventSerializer;
		}

		@Override
		protected void readFactoryIfNecessary(DataInputView source) throws IOException {
		}

		public NFAWithoutStatesSerializer(TypeSerializer<T> typeSerializer) {
			this(typeSerializer, new SharedBuffer.SharedBufferSerializer<>(StringSerializer.INSTANCE, typeSerializer));
		}

		private NFAWithoutStatesSerializer(
			TypeSerializer<T> typeSerializer,
			TypeSerializer<SharedBuffer<String, T>> sharedBufferSerializer) {
			this.eventSerializer = typeSerializer;
			this.sharedBufferSerializer = sharedBufferSerializer;
		}

		@Override
		public CompatibilityResult<NFA<T>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			if (configSnapshot instanceof NFASerializerConfigSnapshot) {
				List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> serializersAndConfigs =
					((NFASerializerConfigSnapshot) configSnapshot).getNestedSerializersAndConfigs();

				CompatibilityResult<T> eventCompatResult = CompatibilityUtil.resolveCompatibilityResult(
					serializersAndConfigs.get(0).f0,
					UnloadableDummyTypeSerializer.class,
					serializersAndConfigs.get(0).f1,
					eventSerializer);

				CompatibilityResult<SharedBuffer<String, T>> sharedBufCompatResult =
					CompatibilityUtil.resolveCompatibilityResult(
						serializersAndConfigs.get(1).f0,
						UnloadableDummyTypeSerializer.class,
						serializersAndConfigs.get(1).f1,
						sharedBufferSerializer);

				if (!sharedBufCompatResult.isRequiresMigration() && !eventCompatResult.isRequiresMigration()) {
					return CompatibilityResult.compatible();
				} else {
					if (eventCompatResult.getConvertDeserializer() != null &&
						sharedBufCompatResult.getConvertDeserializer() != null) {
						return CompatibilityResult.requiresMigration(
							new NFAWithoutStatesSerializer<>(
								new TypeDeserializerAdapter<>(eventCompatResult.getConvertDeserializer()),
								new TypeDeserializerAdapter<>(sharedBufCompatResult.getConvertDeserializer())));
					}
				}
			}

			return CompatibilityResult.requiresMigration();
		}

	}

	//////////////////			Old Serialization			//////////////////////

	/**
	 * A {@link TypeSerializer} for {@link NFA} that uses Java Serialization.
	 */
	public static class Serializer<T> extends TypeSerializerSingleton<NFA<T>> {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public NFA<T> createInstance() {
			return null;
		}

		@Override
		public NFA<T> copy(NFA<T> from) {
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos);

				oos.writeObject(from);

				oos.close();
				baos.close();

				byte[] data = baos.toByteArray();

				ByteArrayInputStream bais = new ByteArrayInputStream(data);
				ObjectInputStream ois = new ObjectInputStream(bais);

				@SuppressWarnings("unchecked")
				NFA<T> copy = (NFA<T>) ois.readObject();
				ois.close();
				bais.close();
				return copy;
			} catch (IOException | ClassNotFoundException e) {
				throw new RuntimeException("Could not copy NFA.", e);
			}
		}

		@Override
		public NFA<T> copy(NFA<T> from, NFA<T> reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return 0;
		}

		@Override
		public void serialize(NFA<T> record, DataOutputView target) throws IOException {
			throw new UnsupportedOperationException("This is the deprecated serialization strategy.");
		}

		@Override
		public NFA<T> deserialize(DataInputView source) throws IOException {
			try (ObjectInputStream ois = new ObjectInputStream(new DataInputViewStream(source))) {
				return (NFA<T>) ois.readObject();
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Could not deserialize NFA.", e);
			}
		}

		@Override
		public NFA<T> deserialize(NFA<T> reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			int size = source.readInt();
			target.writeInt(size);
			target.write(source, size);
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof Serializer;
		}
	}
}
