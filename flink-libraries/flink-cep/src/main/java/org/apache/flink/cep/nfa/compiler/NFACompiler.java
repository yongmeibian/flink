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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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
		Pattern<T, ?> pattern,
		TypeSerializer<T> inputTypeSerializer,
		boolean timeoutHandling) {
		if (pattern == null) {
			// return a factory for empty NFAs
			return new NFAFactoryImpl<T>(inputTypeSerializer, 0, Collections.<State<T>>emptyList(), timeoutHandling);
		} else {
			// set of all generated states
			Map<String, State<T>> states = new HashMap<>();
			long windowTime;

			Pattern<T, ?> currentPattern = pattern;

			// we're traversing the pattern from the end to the beginning --> the first state is the final state
			State<T> sinkState = new State<>(ENDING_STATE_NAME, State.StateType.Final);
			states.put(ENDING_STATE_NAME, sinkState);
			windowTime = currentPattern.getWindowTime() != null ? currentPattern.getWindowTime().toMilliseconds() : 0L;
			while (currentPattern.getPrevious() != null) {
				State<T> sourceState;

				if (states.containsKey(currentPattern.getName())) {
					throw new MalformedPatternException("Duplicate pattern name: " + currentPattern.getName() + ". " +
						"Pattern names must be unique.");
				}

				sourceState = new State<>(currentPattern.getName(), State.StateType.Normal);
				states.put(sourceState.getName(), sourceState);

				if (isLooping(currentPattern)) {
					convertToLooping(currentPattern, sinkState, sourceState);
				} else {
					convertToSingletonState(
						currentPattern,
						sinkState,
						sourceState);
				}

				if (isAtLeastOne(currentPattern)) {
					sourceState = createFirstMandatoryStateOfLoop(
						currentPattern,
						sourceState,
						State.StateType.Normal
					);
				}

				currentPattern = currentPattern.getPrevious();
				sinkState = sourceState;

				final Time currentWindowTime = currentPattern.getWindowTime();
				if (currentWindowTime != null && currentWindowTime.toMilliseconds() < windowTime) {
					// the window time is the global minimum of all window times of each state
					windowTime = currentWindowTime.toMilliseconds();
				}
			}

			if (states.containsKey(currentPattern.getName())) {
				throw new MalformedPatternException("Duplicate pattern name: " + currentPattern.getName() + ". " +
					"Pattern names must be unique.");
			}

			// add the beginning state
			final State<T> beginningState;
			if (isAtLeastOne(currentPattern)) {
				beginningState = new State<>(currentPattern.getName(), State.StateType.Normal);
				states.put(currentPattern.getName(), beginningState);

				final State<T> mandatoryState = createFirstMandatoryStateOfLoop(currentPattern, beginningState, State.StateType.Start);
				states.put(currentPattern.getName(), mandatoryState);
			} else {
				beginningState = new State<>(currentPattern.getName(), State.StateType.Start);
				states.put(currentPattern.getName(), beginningState);
			}

			if (isLooping(currentPattern)) {
				convertToLooping(currentPattern, sinkState, beginningState);
			} else {
				beginningState.addTake(sinkState, (FilterFunction<T>) currentPattern.getFilterFunction());
			}


			return new NFAFactoryImpl<T>(inputTypeSerializer, windowTime, new HashSet<>(states.values()), timeoutHandling);
		}
	}

	private static <T> void convertToSingletonState(
		final Pattern<T, ?> currentPattern,
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
		} else {
			ignoreState = sourceState;
		}

		if (currentPattern instanceof FollowedByPattern) {
			sourceState.addIgnore(ignoreState, trueFunction);
		}
	}

	private static <T> State<T> createFirstMandatoryStateOfLoop(
		final Pattern<T, ?> currentPattern,
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

	private static <T> boolean isAtLeastOne(Pattern<T, ?> currentPattern) {
		return currentPattern.getQuantifier() == Quantifier.ONE_OR_MORE_COMBINATIONS ||
			currentPattern.getQuantifier() == Quantifier.ONE_OR_MORE_EAGER;
	}

	private static <T> boolean isLooping(Pattern<T, ?> currentPattern) {
		return currentPattern.getQuantifier() == Quantifier.ZERO_OR_MORE_EAGER ||
		    currentPattern.getQuantifier() == Quantifier.ZERO_OR_MORE_COMBINATIONS ||
		    currentPattern.getQuantifier() == Quantifier.ONE_OR_MORE_COMBINATIONS ||
		    currentPattern.getQuantifier() == Quantifier.ONE_OR_MORE_EAGER;
	}

	private static <T> void convertToLooping(
		final Pattern<T, ?> currentPattern,
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
