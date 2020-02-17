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

import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Duration;
import java.time.Period;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for Java Expressions DSL.
 */
public class ExpressionsTest extends TestLogger {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testIntervalDurationDoesNotFitIntoLong() {
		thrown.expect(ValidationException.class);
		Expressions.interval(Duration.ofSeconds(Long.MAX_VALUE).plusMillis(1));
	}

	@Test
	public void testIntervalDurationOfDifferentResolution() {
		ApiExpression literal = Expressions.interval(Duration.ofDays(3).plusSeconds(23).plusMillis(300));

		assertThat(literal.toExpr(), equalTo(new ValueLiteralExpression(
			259223300L, // 3 days + 23 seconds + 300 millis
			DataTypes.INTERVAL(DataTypes.SECOND(3)).bridgedTo(Long.class)
		)));
	}

	@Test
	public void testIntervalDurationOfMillis() {
		ApiExpression literal = Expressions.interval(Duration.ofMillis(100));

		assertThat(literal.toExpr(), equalTo(new ValueLiteralExpression(
			100L,
			DataTypes.INTERVAL(DataTypes.SECOND(3)).bridgedTo(Long.class)
		)));
	}

	@Test
	public void testIntervalPeriodDoesNotFitIntoInt() {
		thrown.expect(ValidationException.class);
		Expressions.interval(Period.ofYears(2).plusMonths(Integer.MAX_VALUE));
	}

	@Test
	public void testIntervalPeriodOfDifferentResolution() {
		ApiExpression literal = Expressions.interval(Period.ofYears(1).plusMonths(3));

		assertThat(literal.toExpr(), equalTo(new ValueLiteralExpression(
			15,
			DataTypes.INTERVAL(DataTypes.MONTH()).bridgedTo(Integer.class)
		)));
	}

	@Test
	public void testIntervalPeriodOfMonths() {
		ApiExpression literal = Expressions.interval(Period.ofMonths(4));

		assertThat(literal.toExpr(), equalTo(new ValueLiteralExpression(
			4,
			DataTypes.INTERVAL(DataTypes.MONTH()).bridgedTo(Integer.class)
		)));
	}
}
