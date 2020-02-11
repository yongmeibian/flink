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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.internal.BaseExpressions;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.TimePointUnit;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.utils.ApiExpressionUtils;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.time.Duration;
import java.time.Period;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.objectToExpression;
import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.valueLiteral;

/**
 * Entry point for a Java Table Expression DSL.
 */
@PublicEvolving
public final class Expressions {

	/**
	 * Creates an unresolved reference to a table's column.
	 */
	//CHECKSTYLE.OFF: MethodName
	public static ApiExpression $(String name) {
		return new ApiExpression(ApiExpressionUtils.unresolvedRef(name));
	}
	//CHECKSTYLE.ON: MethodName

	/**
	 * Creates a SQL literal. The data type is derived from the Java class.
	 */
	public static ApiExpression lit(Object v) {
		return new ApiExpression(valueLiteral(v));
	}

	/**
	 * Creates a SQL literal of a given {@link DataType}.
	 */
	public static ApiExpression lit(Object v, DataType dataType) {
		return new ApiExpression(valueLiteral(v, dataType));
	}

	/**
	 * Indicates a range from 'start' to 'end', which can be used in columns
	 * selection.
	 *
	 * @see #withColumns(Object, Object...)
	 * @see #withoutColumns(Object, Object...)
	 */
	public static ApiExpression range(String start, String end) {
		return apiCall(BuiltInFunctionDefinitions.RANGE_TO, valueLiteral(start), valueLiteral(end));
	}

	/**
	 * Indicates an index based range, which can be used in columns selection.
	 *
	 * @see #withColumns(Object, Object...)
	 * @see #withoutColumns(Object, Object...)
	 */
	public static ApiExpression range(int start, int end) {
		return apiCall(BuiltInFunctionDefinitions.RANGE_TO, valueLiteral(start), valueLiteral(end));
	}

	/**
	 * Boolean AND in three-valued logic.
	 */
	public static ApiExpression and(Object predicate0, Object predicate1, Object... predicates) {
		return apiCall(BuiltInFunctionDefinitions.AND, Stream.concat(
			Stream.of(predicate0, predicate1),
			Stream.of(predicates)
		).map(ApiExpressionUtils::objectToExpression)
			.toArray(Expression[]::new));
	}

	/**
	 * Boolean OR in three-valued logic.
	 */
	public static ApiExpression or(Object predicate0, Object predicate1, Object... predicates) {
		return apiCall(BuiltInFunctionDefinitions.OR, Stream.concat(
			Stream.of(predicate0, predicate1),
			Stream.of(predicates)
		).map(ApiExpressionUtils::objectToExpression)
			.toArray(Expression[]::new));
	}

	/**
	 * Offset constant to be used in the {@code preceding} clause of unbounded {@code Over} windows. Use this
	 * constant for a time interval. Unbounded over windows start with the first row of a partition.
	 */
	public static final ApiExpression UNBOUNDED_ROW = apiCall(BuiltInFunctionDefinitions.UNBOUNDED_ROW);

	/**
	 * Offset constant to be used in the {@code preceding} clause of unbounded {@link Over} windows. Use this
	 * constant for a row-count interval. Unbounded over windows start with the first row of a
	 * partition.
	 */
	public static final ApiExpression UNBOUNDED_RANGE = apiCall(BuiltInFunctionDefinitions.UNBOUNDED_RANGE);

	/**
	 * Offset constant to be used in the {@code following} clause of {@link Over} windows. Use this for setting
	 * the upper bound of the window to the current row.
	 */
	public static final ApiExpression CURRENT_ROW = apiCall(BuiltInFunctionDefinitions.CURRENT_ROW);

	/**
	 * Offset constant to be used in the {@code following} clause of {@link Over} windows. Use this for setting
	 * the upper bound of the window to the sort key of the current row, i.e., all rows with the same
	 * sort key as the current row are included in the window.
	 */
	public static final ApiExpression CURRENT_RANGE = apiCall(BuiltInFunctionDefinitions.CURRENT_RANGE);

	/**
	 * Returns the current SQL date in UTC time zone.
	 */
	public static ApiExpression currentDate() {
		return apiCall(BuiltInFunctionDefinitions.CURRENT_DATE);
	}

	/**
	 * Returns the current SQL time in UTC time zone.
	 */
	public static ApiExpression currentTime() {
		return apiCall(BuiltInFunctionDefinitions.CURRENT_TIME);
	}

	/**
	 * Returns the current SQL timestamp in UTC time zone.
	 */
	public static ApiExpression currentTimestamp() {
		return apiCall(BuiltInFunctionDefinitions.CURRENT_TIMESTAMP);
	}

	/**
	 * Returns the current SQL time in local time zone.
	 */
	public static ApiExpression localTime() {
		return apiCall(BuiltInFunctionDefinitions.LOCAL_TIME);
	}

	/**
	 * Returns the current SQL timestamp in local time zone.
	 */
	public static ApiExpression localTimestamp() {
		return apiCall(BuiltInFunctionDefinitions.LOCAL_TIMESTAMP);
	}

	/**
	 * Determines whether two anchored time intervals overlap. Time point and temporal are
	 * transformed into a range defined by two time points (start, end). The function
	 * evaluates <code>leftEnd >= rightStart && rightEnd >= leftStart</code>.
	 *
	 * <p>It evaluates: leftEnd >= rightStart && rightEnd >= leftStart
	 *
	 * <p>e.g. temporalOverlaps("2:55:00".toTime, 1.hour, "3:30:00".toTime, 2.hour) leads to true
	 */
	public static ApiExpression temporalOverlaps(
			Object leftTimePoint,
			Object leftTemporal,
			Object rightTimePoint,
			Object rightTemporal) {
		return apiCall(
			BuiltInFunctionDefinitions.TEMPORAL_OVERLAPS,
			objectToExpression(leftTimePoint),
			objectToExpression(leftTemporal),
			objectToExpression(rightTimePoint),
			objectToExpression(rightTemporal));
	}

	/**
	 * Formats a timestamp as a string using a specified format.
	 * The format must be compatible with MySQL's date formatting syntax as used by the
	 * date_parse function.
	 *
	 * <p>For example dataFormat('time, "%Y, %d %M") results in strings formatted as "2017, 05 May".
	 *
	 * @param timestamp The timestamp to format as string.
	 * @param format The format of the string.
	 * @return The formatted timestamp as string.
	 */
	public static ApiExpression dateFormat(
			Object timestamp,
			Object format) {
		return apiCall(
			BuiltInFunctionDefinitions.DATE_FORMAT,
			objectToExpression(timestamp),
			objectToExpression(format));
	}

	/**
	 * Returns the (signed) number of {@link TimePointUnit} between timePoint1 and timePoint2.
	 *
	 * <p>For example, {@code timestampDiff(TimePointUnit.DAY, '2016-06-15'.toDate, '2016-06-18'.toDate} leads
	 * to 3.
	 *
	 * @param timePointUnit The unit to compute diff.
	 * @param timePoint1 The first point in time.
	 * @param timePoint2 The second point in time.
	 * @return The number of intervals as integer value.
	 */
	public static ApiExpression timestampDiff(
			TimePointUnit timePointUnit,
			Object timePoint1,
			Object timePoint2) {
		return apiCall(
			BuiltInFunctionDefinitions.TIMESTAMP_DIFF,
			valueLiteral(timePointUnit),
			objectToExpression(timePoint1),
			objectToExpression(timePoint2));
	}

	/**
	 * Creates an array of literals.
	 */
	public static ApiExpression array(Object head, Object... tail) {
		return apiCall(
			BuiltInFunctionDefinitions.ARRAY,
			Stream.concat(
				Stream.of(head),
				Stream.of(tail)
			).map(ApiExpressionUtils::objectToExpression)
				.toArray(Expression[]::new));
	}

	/**
	 * Creates a row of expressions.
	 */
	public static ApiExpression row(Object head, Object... tail) {
		return apiCall(BuiltInFunctionDefinitions.ROW, Stream.concat(
			Stream.of(head),
			Stream.of(tail)
		).map(ApiExpressionUtils::objectToExpression)
			.toArray(Expression[]::new));
	}

	/**
	 * Creates a map of expressions.
	 */
	public static ApiExpression map(Object key, Object value, Object... tail) {
		return apiCall(BuiltInFunctionDefinitions.MAP, Stream.concat(
			Stream.of(key, value),
			Stream.of(tail)
		).map(ApiExpressionUtils::objectToExpression)
			.toArray(Expression[]::new));
	}

	/**
	 * Creates an interval of rows.
	 *
	 * @see Table#window(GroupWindow)
	 * @see Table#window(OverWindow...)
	 */
	public static ApiExpression rowInterval(Long rows) {
		return new ApiExpression(valueLiteral(rows));
	}

	/**
	 * Creates a SQL INTERVAL literal corresponding to the given {@link Duration}.
	 * Equivalent to {@code lit(Duration)}.
	 */
	public static ApiExpression interval(Duration duration) {
		if (duration.getNano() > 999) {
			throw new ValidationException("For now only intervals of millisecond precision are supported.");
		}
		long totalMillis = duration.toMillis();
		return new ApiExpression(valueLiteral(
			totalMillis,
			DataTypes.INTERVAL(DataTypes.SECOND(3)).bridgedTo(Long.class)));
	}

	/**
	 * Creates a SQL INTERVAL literal corresponding to the given {@link Period}.
	 * Equivalent to {@code lit(Duration)}.
	 */
	public static ApiExpression interval(Period period) {
		long totalMonths = period.toTotalMonths();
		if (totalMonths > Integer.MAX_VALUE) {
			throw new ValidationException("For now only intervals of up to INTEGER.MAX_VALUE months are supported.");
		}
		return new ApiExpression(valueLiteral(
			(int) totalMonths,
			DataTypes.INTERVAL(DataTypes.MONTH()).bridgedTo(Integer.class)));
	}

	/**
	 * Parses given string and creates a SQL INTERVAL literal. For the expected formats see javadocs for
	 * corresponding {@code DataTypes}.
	 *
	 * @see org.apache.flink.table.types.logical.YearMonthIntervalType
	 * @see org.apache.flink.table.types.logical.DayTimeIntervalType
	 */
	public static ApiExpression interval(
			String interval,
			DataTypes.Resolution lowerResolution,
			DataTypes.Resolution upperResolution) {
		DataType intervalType = DataTypes.INTERVAL(lowerResolution, upperResolution);
		Function<String, Object> intervalParser = IntervalParsingUtils.intervalParser(intervalType);
		return new ApiExpression(valueLiteral(intervalParser.apply(interval), intervalType));
	}

	/**
	 * Returns a value that is closer than any other value to pi.
	 */
	public static ApiExpression pi() {
		return apiCall(BuiltInFunctionDefinitions.PI);
	}

	/**
	 * Returns a value that is closer than any other value to e.
	 */
	public static ApiExpression e() {
		return apiCall(BuiltInFunctionDefinitions.E);
	}

	/**
	 * Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive).
	 */
	public static ApiExpression rand() {
		return apiCall(BuiltInFunctionDefinitions.RAND);
	}

	/**
	 * Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive) with a
	 * initial seed. Two rand() functions will return identical sequences of numbers if they
	 * have same initial seed.
	 */
	public static ApiExpression rand(Object seed) {
		return apiCall(BuiltInFunctionDefinitions.RAND, objectToExpression(seed));
	}

	/**
	 * Returns a pseudorandom integer value between 0.0 (inclusive) and the specified
	 * value (exclusive).
	 */
	public static ApiExpression randInteger(Object bound) {
		return apiCall(BuiltInFunctionDefinitions.RAND_INTEGER, objectToExpression(bound));
	}

	/**
	 * Returns a pseudorandom integer value between 0.0 (inclusive) and the specified value
	 * (exclusive) with a initial seed. Two randInteger() functions will return identical sequences
	 * of numbers if they have same initial seed and same bound.
	 */
	public static ApiExpression randInteger(Object seed, Object bound) {
		return apiCall(BuiltInFunctionDefinitions.RAND_INTEGER, objectToExpression(seed), objectToExpression(bound));
	}

	/**
	 * Returns the string that results from concatenating the arguments.
	 * Returns NULL if any argument is NULL.
	 */
	public static ApiExpression concat(Object string, Object... strings) {
		return apiCall(
			BuiltInFunctionDefinitions.CONCAT,
			Stream.concat(
				Stream.of(string),
				Stream.of(strings)
			).map(ApiExpressionUtils::objectToExpression)
				.toArray(Expression[]::new));
	}

	/**
	 * Calculates the arc tangent of a given coordinate.
	 */
	public static ApiExpression atan2(Object y, Object x) {
		return apiCall(BuiltInFunctionDefinitions.ATAN2, objectToExpression(y), objectToExpression(x));
	}

	/**
	 * Returns the string that results from concatenating the arguments and separator.
	 * Returns NULL If the separator is NULL.
	 *
	 * <p>Note: this user-public static ApiExpressionined function does not skip empty strings. However, it does skip any NULL
	 * values after the separator argument.
	 **/
	public static ApiExpression concatWs(Object separator, Object string, Object... strings) {
		return apiCall(BuiltInFunctionDefinitions.CONCAT_WS, Stream.concat(
			Stream.of(separator, string),
			Stream.of(strings)
		).map(ApiExpressionUtils::objectToExpression)
			.toArray(Expression[]::new));
	}

	/**
	 * Returns an UUID (Universally Unique Identifier) string (e.g.,
	 * "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e") according to RFC 4122 type 4 (pseudo randomly
	 * generated) UUID. The UUID is generated using a cryptographically strong pseudo random number
	 * generator.
	 */
	public static ApiExpression uuid() {
		return apiCall(BuiltInFunctionDefinitions.UUID);
	}

	/**
	 * Returns a null literal value of a given data type.
	 *
	 * <p>e.g. {@code nullOf(DataTypes.INT())}
	 */
	public static ApiExpression nullOf(DataType dataType) {
		return new ApiExpression(valueLiteral(null, dataType));
	}

	/**
	 * @deprecated This method will be removed in future versions as it uses the old type system.
	 * It is recommended to use {@link #nullOf(DataType)} instead which uses the new type
	 * system based on {@link DataTypes}. Please make sure to use either the old or the new
	 * type system consistently to avoid unintended behavior. See the website
	 * documentation for more information.
	 */
	public static ApiExpression nullOf(TypeInformation<?> typeInfo) {
		return nullOf(TypeConversions.fromLegacyInfoToDataType(typeInfo));
	}

	/**
	 * Calculates the logarithm of the given value.
	 */
	public static ApiExpression log(Object value) {
		return apiCall(BuiltInFunctionDefinitions.LOG, objectToExpression(value));
	}

	/**
	 * Calculates the logarithm of the given value to the given base.
	 */
	public static ApiExpression log(Object base, Object value) {
		return apiCall(BuiltInFunctionDefinitions.LOG, objectToExpression(base), objectToExpression(value));
	}

	/**
	 * Ternary conditional operator that decides which of two other expressions should be evaluated
	 * based on a evaluated boolean condition.
	 *
	 * <p>e.g. ifThenElse(42 > 5, "A", "B") leads to "A"
	 *
	 * @param condition boolean condition
	 * @param ifTrue expression to be evaluated if condition holds
	 * @param ifFalse expression to be evaluated if condition does not hold
	 */
	public static ApiExpression ifThenElse(Object condition, Object ifTrue, Object ifFalse) {
		return apiCall(
			BuiltInFunctionDefinitions.IF,
			objectToExpression(condition),
			objectToExpression(ifTrue),
			objectToExpression(ifFalse));
	}

	/**
	 * Creates an expression that selects a range of columns. It can be used wherever an array of
	 * expression is accepted such as function calls, projections, or groupings.
	 *
	 * <p>A range can either be index-based or name-based. Indices start at 1 and boundaries are
	 * inclusive.
	 *
	 * <p>e.g. withColumns('b to 'c) or withColumns('*)
	 */
	public static ApiExpression withColumns(Object head, Object... tail) {
		return apiCall(
			BuiltInFunctionDefinitions.WITH_COLUMNS,
			Stream.concat(
				Stream.of(head),
				Stream.of(tail)
			).map(ApiExpressionUtils::objectToExpression)
				.toArray(Expression[]::new));
	}

	/**
	 * Creates an expression that selects all columns except for the given range of columns. It can
	 * be used wherever an array of expression is accepted such as function calls, projections, or
	 * groupings.
	 *
	 * <p>A range can either be index-based or name-based. Indices start at 1 and boundaries are
	 * inclusive.
	 *
	 * <p>e.g. withoutColumns('b to 'c) or withoutColumns('c)
	 */
	public static ApiExpression withoutColumns(Object head, Object... tail) {
		return apiCall(
			BuiltInFunctionDefinitions.WITHOUT_COLUMNS,
			Stream.concat(
				Stream.of(head),
				Stream.of(tail)
			).map(ApiExpressionUtils::objectToExpression)
				.toArray(Expression[]::new));
	}

	/**
	 * A call to a function that will be looked up in a catalog.
	 */
	public static ApiExpression call(String functionName, Object... params) {
		return new ApiExpression(ApiExpressionUtils.lookupCall(
			functionName,
			Arrays.stream(params).map(ApiExpressionUtils::objectToExpression).toArray(Expression[]::new)));
	}

	/**
	 * A call to an inline function. For functions registered in a catalog use {@link #call(String, Object...)}.
	 */
	public static ApiExpression call(FunctionDefinition scalarFunction, Object... params) {
		return apiCall(
			scalarFunction,
			Arrays.stream(params).map(ApiExpressionUtils::objectToExpression).toArray(Expression[]::new));
	}

	private static ApiExpression apiCall(FunctionDefinition functionDefinition, Expression... args) {
		return new ApiExpression(new UnresolvedCallExpression(functionDefinition, Arrays.asList(args)));
	}

	/**
	 * Java API class that gives access to expressions operations.
	 */
	public static final class ApiExpression extends BaseExpressions<Object, ApiExpression> implements Expression {
		private final Expression wrappedExpression;

		@Override
		public String asSummaryString() {
			return wrappedExpression.asSummaryString();
		}

		private ApiExpression(Expression wrappedExpression) {
			if (wrappedExpression instanceof ApiExpression) {
				throw new UnsupportedOperationException("This is a bug. Please file a JIRA.");
			}
			this.wrappedExpression = wrappedExpression;
		}

		@Override
		public Expression toExpr() {
			return wrappedExpression;
		}

		@Override
		protected ApiExpression toApiSpecificExpression(Expression expression) {
			return new ApiExpression(expression);
		}

		@Override
		public List<Expression> getChildren() {
			return wrappedExpression.getChildren();
		}

		@Override
		public <R> R accept(ExpressionVisitor<R> visitor) {
			return wrappedExpression.accept(visitor);
		}
	}
}
