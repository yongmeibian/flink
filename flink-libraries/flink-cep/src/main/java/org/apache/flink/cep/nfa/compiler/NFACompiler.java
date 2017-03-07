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

package org.apache.flink.cep.nfa.compiler;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.State;
import org.apache.flink.cep.pattern.FilterFunctions;
import org.apache.flink.cep.pattern.FollowedByPattern;
import org.apache.flink.cep.pattern.NotFilterFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Compiler class containing methods to compile a {@link Pattern} into a {@link NFA} or a
 * {@link NFAFactory}.
 */
public class NFACompiler {

	protected static final String ENDING_STATE_NAME = "$endState$";

	/**
	 * Compiles the given pattern into a {@link NFA}.
	 *
	 * @param pattern Definition of sequence pattern
	 * @param inputTypeSerializer Serializer for the input type
	 * @param timeoutHandling True if the NFA shall return timed out event patterns
	 * @param <T> Type of the input events
	 * @return Non-deterministic finite automaton representing the given pattern
	 */
	public static <T> NFA<T> compile(
		Pattern<T, ?> pattern,
		TypeSerializer<T> inputTypeSerializer,
		boolean timeoutHandling) {
		NFAFactory<T> factory = compileFactory(pattern, inputTypeSerializer, timeoutHandling);

		return factory.createNFA();
	}

	/**
	 * Compiles the given pattern into a {@link NFAFactory}. The NFA factory can be used to create
	 * multiple NFAs.
	 *
	 * @param pattern Definition of sequence pattern
	 * @param inputTypeSerializer Serializer for the input type
	 * @param timeoutHandling True if the NFA shall return timed out event patterns
	 * @param <T> Type of the input events
	 * @return Factory for NFAs corresponding to the given pattern
	 */
	@SuppressWarnings("unchecked")
	public static <T> NFAFactory<T> compileFactory(
		final Pattern<T, ?> pattern,
		final TypeSerializer<T> inputTypeSerializer,
		boolean timeoutHandling) {
		if (pattern == null) {
			// return a factory for empty NFAs
			return new NFAFactoryImpl<>(inputTypeSerializer, 0, Collections.<State<T>>emptyList(), timeoutHandling);
		} else {
			final NFAFactoryCompiler<T> nfaFactoryCompiler = new NFAFactoryCompiler<>(pattern);
			nfaFactoryCompiler.compileFactory();
			return new NFAFactoryImpl<>(inputTypeSerializer, nfaFactoryCompiler.getWindowTime(), nfaFactoryCompiler.getStates(), timeoutHandling);
		}
	}

	private static class NFAFactoryCompiler<T> {

		private final Set<String> usedNames = new HashSet<>();
		private final List<State<T>> states = new ArrayList<>();

		private long windowTime = 0;
		private Pattern<T, ?> currentPattern;

		NFAFactoryCompiler(final Pattern<T, ?> pattern) {
			this.currentPattern = pattern;
		}

		/**
		 * Compiles the given pattern into a {@link NFAFactory}. The NFA factory can be used to create
		 * multiple NFAs.
		 */
		void compileFactory() {
			// we're traversing the pattern from the end to the beginning --> the first state is the final state
			State<T> sinkState = createEndingState();
			// add all the normal states
			sinkState = createMiddleStates(sinkState);
			// add the beginning state
			createStartState(sinkState);
		}

		List<State<T>> getStates() {
			return states;
		}

		long getWindowTime() {
			return windowTime;
		}

		private State<T> createEndingState() {
			State<T> sinkState = new State<>(ENDING_STATE_NAME, State.StateType.Final);
			states.add(sinkState);
			usedNames.add(ENDING_STATE_NAME);

			windowTime = currentPattern.getWindowTime() != null ? currentPattern.getWindowTime().toMilliseconds() : 0L;
			return sinkState;
		}

		private State<T> createMiddleStates(final State<T> sinkState) {

			State<T> lastSink = sinkState;
			while (currentPattern.getPrevious() != null) {
				State<T> sourceState;

				checkPatternNameUniqueness();

				sourceState = new State<>(currentPattern.getName(), State.StateType.Normal);
				states.add(sourceState);
				usedNames.add(sourceState.getName());

				if (isLooping()) {
					convertToLooping(lastSink, sourceState);

					if (isAtLeastOne()) {
						sourceState = createFirstMandatoryStateOfLoop(
							sourceState,
							State.StateType.Normal
						);
						states.add(sourceState);
						usedNames.add(sourceState.getName());
					}
				} else if (isTimes()) {
					sourceState = convertToTimesState(lastSink, sourceState, currentPattern.getTimes());
				} else {
					convertToSingletonState(
						lastSink,
						sourceState);
				}

				currentPattern = currentPattern.getPrevious();
				lastSink = sourceState;

				final Time currentWindowTime = currentPattern.getWindowTime();
				if (currentWindowTime != null && currentWindowTime.toMilliseconds() < windowTime) {
					// the window time is the global minimum of all window times of each state
					windowTime = currentWindowTime.toMilliseconds();
				}
			}

			return lastSink;
		}

		private void checkPatternNameUniqueness() {
			if (usedNames.contains(currentPattern.getName())) {
				throw new MalformedPatternException(
					"Duplicate pattern name: " + currentPattern.getName() + ". " +
					"Pattern names must be unique.");
			}
		}

		@SuppressWarnings("unchecked")
		private State<T> createStartState(State<T> sinkState) {
			final State<T> beginningState;

			checkPatternNameUniqueness();

			if (isLooping()) {
				final State<T> loopingState;
				if (isAtLeastOne()) {
					loopingState = new State<>(currentPattern.getName(), State.StateType.Normal);
					beginningState = createFirstMandatoryStateOfLoop(loopingState, State.StateType.Start);
					states.add(loopingState);
				} else {
					loopingState = new State<>(currentPattern.getName(), State.StateType.Start);
					beginningState = loopingState;
				}
				convertToLooping(sinkState, loopingState);
			} else if (isTimes()) {
				if (currentPattern.getTimes() > 1) {
					final State<T> timesState = new State<>(currentPattern.getName(), State.StateType.Normal);
					states.add(timesState);
					sinkState = convertToTimesState(sinkState, timesState, currentPattern.getTimes() - 1);
				}
				beginningState = new State<>(currentPattern.getName(), State.StateType.Start);
				beginningState.addTake(sinkState, (FilterFunction<T>) currentPattern.getFilterFunction());
			} else {
				beginningState = new State<>(currentPattern.getName(), State.StateType.Start);
				beginningState.addTake(sinkState, (FilterFunction<T>) currentPattern.getFilterFunction());
			}

			states.add(beginningState);
			usedNames.add(beginningState.getName());

			return beginningState;
		}

		private boolean isTimes() {
			return currentPattern.getQuantifier() == Quantifier.TIMES;
		}

		private boolean isAtLeastOne() {
			return currentPattern.getQuantifier() == Quantifier.ONE_OR_MORE_COMBINATIONS ||
				currentPattern.getQuantifier() == Quantifier.ONE_OR_MORE_EAGER;
		}

		private boolean isLooping() {
			return currentPattern.getQuantifier() == Quantifier.ZERO_OR_MORE_EAGER ||
				currentPattern.getQuantifier() == Quantifier.ZERO_OR_MORE_COMBINATIONS ||
				currentPattern.getQuantifier() == Quantifier.ONE_OR_MORE_COMBINATIONS ||
				currentPattern.getQuantifier() == Quantifier.ONE_OR_MORE_EAGER;
		}

		private State<T> convertToTimesState(State<T> sinkState, State<T> sourceState, int times) {
			convertToSingletonState(sinkState, sourceState);
			for (int i = 0; i < times - 1; i++) {
				sinkState = sourceState;
				sourceState = new State<>(currentPattern.getName(), State.StateType.Normal);
				states.add(sourceState);
				convertToSingletonState(sinkState, sourceState);
			}
			return sourceState;
		}

		@SuppressWarnings("unchecked")
		private void convertToSingletonState(
			final State<T> sinkState,
			final State<T> sourceState) {

			final FilterFunction<T> currentFilterFunction = (FilterFunction<T>) currentPattern.getFilterFunction();
			final FilterFunction<T> trueFunction = FilterFunctions.trueFunction();
			sourceState.addTake(sinkState, currentFilterFunction);

			final State<T> ignoreState;
			if (currentPattern.getQuantifier() == Quantifier.OPTIONAL) {
				sourceState.addProceed(sinkState, trueFunction);
				ignoreState = new State<>(currentPattern.getName(), State.StateType.Normal);

				ignoreState.addTake(sinkState, currentFilterFunction);
				states.add(ignoreState);
			} else {
				ignoreState = sourceState;
			}

			if (currentPattern instanceof FollowedByPattern) {
				sourceState.addIgnore(ignoreState, trueFunction);
			}
		}

		@SuppressWarnings("unchecked")
		private State<T> createFirstMandatoryStateOfLoop(
			final State<T> sinkState,
			final State.StateType stateType) {

			final FilterFunction<T> currentFilterFunction = (FilterFunction<T>) currentPattern.getFilterFunction();
			final State<T> firstState = new State<>(currentPattern.getName(), stateType);

			firstState.addTake(sinkState, currentFilterFunction);
			if (currentPattern instanceof FollowedByPattern) {
				if (currentPattern.getQuantifier() == Quantifier.ONE_OR_MORE_COMBINATIONS) {
					firstState.addIgnore(FilterFunctions.<T>trueFunction());
				} else {
					firstState.addIgnore(new NotFilterFunction<>(currentFilterFunction));
				}
			}
			return firstState;
		}

		@SuppressWarnings("unchecked")
		private void convertToLooping(
			final State<T> sinkState,
			final State<T> sourceState) {

			final FilterFunction<T> filterFunction = (FilterFunction<T>) currentPattern.getFilterFunction();
			final FilterFunction<T> trueFunction = FilterFunctions.<T>trueFunction();

			sourceState.addProceed(sinkState, trueFunction);
			sourceState.addTake(filterFunction);
			if (currentPattern instanceof FollowedByPattern) {
				final State<T> ignoreState = new State<>(
					currentPattern.getName(),
					State.StateType.Normal);


				final FilterFunction<T> ignoreCondition;
				if (currentPattern.getQuantifier() == Quantifier.ZERO_OR_MORE_COMBINATIONS ||
					currentPattern.getQuantifier() == Quantifier.ONE_OR_MORE_COMBINATIONS) {
					ignoreCondition = trueFunction;
				} else {
					ignoreCondition = new NotFilterFunction<>(filterFunction);
				}

				sourceState.addIgnore(ignoreState, ignoreCondition);
				ignoreState.addTake(sourceState, filterFunction);
				ignoreState.addIgnore(ignoreState, ignoreCondition);
				states.add(ignoreState);
			}
		}
	}

	/**
	 * Factory interface for {@link NFA}.
	 *
	 * @param <T> Type of the input events which are processed by the NFA
	 */
	public interface NFAFactory<T> extends Serializable {
		NFA<T> createNFA();
	}

	/**
	 * Implementation of the {@link NFAFactory} interface.
	 * <p>
	 * The implementation takes the input type serializer, the window time and the set of
	 * states and their transitions to be able to create an NFA from them.
	 *
	 * @param <T> Type of the input events which are processed by the NFA
	 */
	private static class NFAFactoryImpl<T> implements NFAFactory<T> {

		private static final long serialVersionUID = 8939783698296714379L;

		private final TypeSerializer<T> inputTypeSerializer;
		private final long windowTime;
		private final Collection<State<T>> states;
		private final boolean timeoutHandling;

		private NFAFactoryImpl(
			TypeSerializer<T> inputTypeSerializer,
			long windowTime,
			Collection<State<T>> states,
			boolean timeoutHandling) {

			this.inputTypeSerializer = inputTypeSerializer;
			this.windowTime = windowTime;
			this.states = states;
			this.timeoutHandling = timeoutHandling;
		}

		@Override
		public NFA<T> createNFA() {
			NFA<T> result =  new NFA<>(inputTypeSerializer.duplicate(), windowTime, timeoutHandling);

			result.addStates(states);

			return result;
		}
	}
}
