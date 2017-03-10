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
package org.apache.flink.cep.pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

public class QuantifiedPattern<T, F extends T> implements IPattern<T,F> {

	private Pattern<T, F> wrappedPattern;


	public QuantifiedPattern(
		final Pattern<T, F> wrappedPattern) {
		this.wrappedPattern = wrappedPattern;
	}

	@Override
	public String getName() {
		return wrappedPattern.getName();
	}

	@Override
	public Pattern<T, ? extends T> getPrevious() {
		return wrappedPattern.getPrevious();
	}

	@Override
	public FilterFunction<F> getFilterFunction() {
		return wrappedPattern.getFilterFunction();
	}

	@Override
	public Time getWindowTime() {
		return wrappedPattern.getWindowTime();
	}

	@Override
	public Quantifier getQuantifier() {
		return wrappedPattern.getQuantifier();
	}

	@Override
	public int getTimes() {
		return wrappedPattern.getTimes();
	}

	@Override
	public boolean isFollowedBy() {
		return wrappedPattern.isFollowedBy();
	}


	/**
	 * Specifies a filter condition which has to be fulfilled by an event in order to be matched.
	 *
	 * @param newFilterFunction Filter condition
	 * @return The same pattern operator where the new filter condition is set
	 */
	public QuantifiedPattern<T, F> where(FilterFunction<F> newFilterFunction) {
		wrappedPattern = wrappedPattern.where(newFilterFunction);

		return this;
	}

	/**
	 * Specifies a filter condition which is OR'ed with an existing filter function.
	 *
	 * @param orFilterFunction OR filter condition
	 * @return The same pattern operator where the new filter condition is set
	 */
	public QuantifiedPattern<T, F> or(FilterFunction<F> orFilterFunction) {
		wrappedPattern = wrappedPattern.where(orFilterFunction);

		return this;
	}

	/**
	 * Applies a subtype constraint on the current pattern operator. This means that an event has
	 * to be of the given subtype in order to be matched.
	 *
	 * @param subtypeClass Class of the subtype
	 * @param <S> Type of the subtype
	 * @return The same pattern operator with the new subtype constraint
	 */
	public <S extends F> QuantifiedPattern<T, S> subtype(final Class<S> subtypeClass) {
		return new QuantifiedPattern<>(wrappedPattern.subtype(subtypeClass));
	}

	/**
	 * Defines the maximum time interval for a matching pattern. This means that the time gap
	 * between first and the last event must not be longer than the window time.
	 *
	 * @param windowTime Time of the matching window
	 * @return The same pattenr operator with the new window length
	 */
	public QuantifiedPattern<T, F> within(Time windowTime) {
		wrappedPattern = wrappedPattern.within(windowTime);

		return this;
	}

	/**
	 * Appends a new pattern operator to the existing one. The new pattern operator enforces strict
	 * temporal contiguity. This means that the whole pattern only matches if an event which matches
	 * this operator directly follows the preceding matching event. Thus, there cannot be any
	 * events in between two matching events.
	 *
	 * @param name Name of the new pattern operator
	 * @return A new pattern operator which is appended to this pattern operator
	 */
	public Pattern<T, T> next(final String name) {
		return new Pattern<T, T>(name, wrappedPattern);
	}

	/**
	 * Appends a new pattern operator to the existing one. The new pattern operator enforces
	 * non-strict temporal contiguity. This means that a matching event of this operator and the
	 * preceding matching event might be interleaved with other events which are ignored.
	 *
	 * @param name Name of the new pattern operator
	 * @return A new pattern operator which is appended to this pattern operator
	 */
	public FollowedByPattern<T, T> followedBy(final String name) {
		return new FollowedByPattern<T, T>(name, wrappedPattern);
	}
}
