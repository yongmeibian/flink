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

package org.apache.flink.table.api;

import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ValueLiteralExpression;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.time.Period;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for Java Expressions DSL.
 */
@RunWith(Parameterized.class)
public class ExpressionsTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Parameterized.Parameter
	public TestSpec testSpec;

	@Parameterized.Parameters(name = "{0}")
	public static Collection<TestSpec> parameters() {
		return Arrays.asList(
			TestSpec.expression(
				"testIntervalDurationDoesNotFitIntoLong",
				() -> Expressions.interval(Duration.ofSeconds(Long.MAX_VALUE).plusMillis(1))
			).expectExceptionMessage(""),

			TestSpec.expression(
				"testIntervalDurationOfDifferentResolution",
				() -> Expressions.interval(Duration.ofDays(3).plusSeconds(23).plusMillis(300))
			).expect(new ValueLiteralExpression(
				259223300L, // 3 days + 23 seconds + 300 millis
				DataTypes.INTERVAL(DataTypes.SECOND(3)).bridgedTo(Long.class)
			)),

			TestSpec.expression(
				"testIntervalDurationOfMillis",
				() -> Expressions.interval(Duration.ofMillis(100))
			).expect(new ValueLiteralExpression(
				100L,
				DataTypes.INTERVAL(DataTypes.SECOND(3)).bridgedTo(Long.class)
			)),

			TestSpec.expression(
				"testIntervalPeriodDoesNotFitIntoInt",
				() -> Expressions.interval(Period.ofYears(2).plusMonths(Integer.MAX_VALUE))
			).expectExceptionMessage(""),

			TestSpec.expression(
				"testIntervalPeriodOfDifferentResolution",
				() -> Expressions.interval(Period.ofYears(1).plusMonths(3))
			).expect(new ValueLiteralExpression(
				15,
				DataTypes.INTERVAL(DataTypes.MONTH()).bridgedTo(Integer.class)
			)),

			TestSpec.expression(
				"testIntervalPeriodOfMonths",
				() -> Expressions.interval(Period.ofMonths(4))
			).expect(new ValueLiteralExpression(
				4,
				DataTypes.INTERVAL(DataTypes.MONTH()).bridgedTo(Integer.class)
			))
		);
	}

	@Test
	public void testExpression() {
		testSpec.getExceptionMessage().ifPresent(message -> {
				thrown.expect(ValidationException.class);
				thrown.expectMessage(message);
			}
		);

		assertThat(testSpec.getTestedExpression(), equalTo(testSpec.getExpectedExpression()));
	}

	private static class TestSpec {
		private final Supplier<ApiExpression> testedExpression;
		private Expression expectedExpression = null;
		private String exceptionMessage = null;
		private final String description;

		public TestSpec(Supplier<ApiExpression> testedExpression, String description) {
			this.testedExpression = testedExpression;
			this.description = description;
		}

		public static TestSpec expression(String description, Supplier<ApiExpression> expression) {
			return new TestSpec(expression, description);
		}

		public TestSpec expect(Expression expected) {
			this.expectedExpression = expected;
			return this;
		}

		public TestSpec expectExceptionMessage(String message) {
			this.exceptionMessage = message;
			return this;
		}

		public Expression getTestedExpression() {
			return testedExpression.get().toExpr();
		}

		public Expression getExpectedExpression() {
			return expectedExpression;
		}

		public Optional<String> getExceptionMessage() {
			return Optional.ofNullable(exceptionMessage);
		}

		@Override
		public String toString() {
			return description;
		}
	}
}
