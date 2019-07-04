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

package org.apache.flink.table.expressions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OrdinalReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.CHAR;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.SYMBOL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasLength;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasScale;
import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;

public class ExpressionConverter extends ResolvedExpressionVisitor<RexNode> {

	private final FlinkRelBuilder relBuilder;
	private final PlannerExpressionConverter toPlannerExpression = PlannerExpressionConverter$.MODULE$.INSTANCE();
	private final int inputCount;

	public ExpressionConverter(FlinkRelBuilder relBuilder, int inputCount) {
		this.relBuilder = relBuilder;
		this.inputCount = inputCount;
	}

	@Override
	public RexNode visit(TableReferenceExpression tableReference) {
		throw new TableException("Should be translated from IN clause");
	}

	@Override
	public RexNode visit(LocalReferenceExpression localReference) {
		throw new TableException(
			"Local reference should be handled individually by a call: " + localReference);
	}

	@Override
	public RexNode visit(CallExpression call) {

		if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.AS) {
			RexNode expression = call.getChildren().get(0).accept(this);
			String alias = ExpressionUtils.extractValue(call.getChildren().get(1), String.class).get();
			return relBuilder.alias(expression, alias);
		} else if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.CAST) {
			RexNode expression = call.getChildren().get(0).accept(this);
			DataType outputDataType = ((TypeLiteralExpression) (call.getChildren().get(1))).getOutputDataType();
			RelDataType relDataType = relBuilder.getTypeFactory()
				.createTypeFromTypeInfo(
					fromDataTypeToLegacyInfo(outputDataType),
					outputDataType.getLogicalType().isNullable()
				);
			return relBuilder.getRexBuilder().makeCast(relDataType, expression);
		} else if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.IN) {
			Expression subQuery = call.getChildren().get(1);
			if (subQuery instanceof TableReferenceExpression) {
				return RexSubQuery.in(
					relBuilder.tableOperation(((TableReferenceExpression) subQuery).getQueryOperation()).build(),
					ImmutableList.of(call.getChildren().get(0).accept(this))
				);
			} else {
				List<RexNode> elements = call.getChildren()
					.stream()
					.map(c -> c.accept(this))
					.collect(Collectors.toList());
				return relBuilder.call(SqlStdOperatorTable.IN, elements);
			}
		} else if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.GET) {
			RexNode expression = call.getChildren().get(0).accept(this);
			Optional<String> fieldName = ExpressionUtils.extractValue(call.getChildren().get(1), String.class);
			if (fieldName.isPresent()) {
				return relBuilder.getRexBuilder().makeFieldAccess(expression, fieldName.get(), true);
			} else {
				Optional<Integer> fieldIndex = ExpressionUtils.extractValue(call.getChildren().get(1), Integer.class);
				return relBuilder.getRexBuilder().makeFieldAccess(expression, fieldIndex.get());
			}
		} else if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.OVER) {
			return convertOver(call);
		} else  if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.TRIM) {
			List<Expression> children = call.getChildren();
			boolean removeLeading = ExpressionUtils.extractValue(children.get(0), Boolean.class).get();
			boolean removeTrailing = ExpressionUtils.extractValue(children.get(1), Boolean.class).get();

			final RexLiteral trimFlag;
			if (removeLeading && removeTrailing) {
				trimFlag = relBuilder.getRexBuilder().makeFlag(SqlTrimFunction.Flag.BOTH);
			} else if (removeLeading) {
				trimFlag = relBuilder.getRexBuilder().makeFlag(SqlTrimFunction.Flag.LEADING);
			} else if (removeTrailing) {
				trimFlag = relBuilder.getRexBuilder().makeFlag(SqlTrimFunction.Flag.TRAILING);
			} else {
				throw new TableException("Unsupported trim mode.");
			}

			List<RexNode> trimChildren = call.getChildren()
				.subList(2, call.getChildren().size())
				.stream()
				.map(expr -> expr.accept(this))
				.collect(Collectors.toCollection(ArrayList::new));
			trimChildren.add(0, trimFlag);
			return relBuilder.call(SqlStdOperatorTable.TRIM, trimChildren);
		}

		List<RexNode> childrenRexNodes = call.getChildren()
			.stream()
			.map(c -> c.accept(this))
			.collect(Collectors.toList());

		if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.AND) {
			return relBuilder.and(childrenRexNodes);
		} else {
			List<ResolvedExpression> childrenExpressions = childrenRexNodes.stream()
				.map(RexNodeExpression::new)
				.collect(Collectors.toList());
			return new CallExpression(call.getFunctionDefinition(),
				childrenExpressions, call.getOutputDataType())
				.accept(toPlannerExpression).toRexNode(relBuilder);
		}
	}

	private RexNode convertOver(CallExpression call) {
		RexBuilder rexBuilder = relBuilder.getRexBuilder();

		// assemble aggregation
		SqlAggFunction operator = ((Aggregation) (call.getChildren()
			.get(0)
			.accept(toPlannerExpression))).getSqlAggFunction(relBuilder);
		DataType dataType = call.getOutputDataType();
		RelDataType aggResultType = relBuilder
			.getTypeFactory()
			.createTypeFromTypeInfo(fromDataTypeToLegacyInfo(dataType), dataType.getLogicalType().isNullable());

		// assemble exprs by agg children
		List<RexNode> aggExprs = call.getChildren().get(0).getChildren()
			.stream()
			.map(expr -> expr.accept(this))
			.collect(Collectors.toList());

		// assemble order by key
		Expression orderBy = call.getChildren().get(1);
		RexFieldCollation orderKey = new RexFieldCollation(orderBy.accept(this), Collections.emptySet());
		ImmutableList<RexFieldCollation> orderKeys = ImmutableList.of(orderKey);

		// assemble partition by keys
		List<RexNode> partitionKeys = call.getChildren()
			.subList(4, call.getChildren().size())
			.stream()
			.map(expr -> expr.accept(this))
			.collect(Collectors.toList());

		// assemble bounds
		ResolvedExpression preceding = call.getResolvedChildren().get(2);
		boolean isPhysical = hasRoot(
			preceding.getOutputDataType().getLogicalType(),
			LogicalTypeRoot.BIGINT);

		ResolvedExpression following = call.getResolvedChildren().get(3);
		RexWindowBound lowerBound = createBound(preceding, SqlKind.PRECEDING);
		RexWindowBound upperBound = createBound(following, SqlKind.FOLLOWING);

		// build RexOver
		return rexBuilder.makeOver(
			aggResultType,
			operator,
			aggExprs,
			partitionKeys,
			orderKeys,
			lowerBound,
			upperBound,
			isPhysical,
			true,
			false,
			false);
	}

	private RexWindowBound createBound(ResolvedExpression bound, SqlKind sqlKind){
		if (isFunction(bound, BuiltInFunctionDefinitions.UNBOUNDED_ROW) ||
			isFunction(bound, BuiltInFunctionDefinitions.UNBOUNDED_RANGE)) {
			SqlNode unboundedPreceding = SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO);
			return RexWindowBound.create(unboundedPreceding, null);
		} else if (isFunction(bound, BuiltInFunctionDefinitions.CURRENT_RANGE) ||
			isFunction(bound, BuiltInFunctionDefinitions.CURRENT_ROW)) {
			SqlNode currentRow = SqlWindow.createCurrentRow(SqlParserPos.ZERO);
			return RexWindowBound.create(currentRow, null);
		} else if (bound instanceof ValueLiteralExpression) {
			RelDataType relDataType = relBuilder.getTypeFactory()
				.createTypeFromTypeInfo(Types.BIG_DEC, bound.getOutputDataType().getLogicalType().isNullable());
			SqlPostfixOperator sqlOperator = new SqlPostfixOperator(
				sqlKind.name(),
				sqlKind,
				2,
				new OrdinalReturnTypeInference(0),
				null,
				null);
			SqlNode[] operands = new SqlNode[]{SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)};
			SqlBasicCall node = new SqlBasicCall(sqlOperator, operands, SqlParserPos.ZERO);

			RexNode rexNode = relBuilder.getRexBuilder()
				.makeCall(relDataType, sqlOperator, Arrays.asList(bound.accept(this)));
			return RexWindowBound.create(node, rexNode);
		}
		return null;
	}

	private boolean isFunction(ResolvedExpression expression, BuiltInFunctionDefinition functionDefinition) {
		return expression instanceof CallExpression &&
			((CallExpression) expression).getFunctionDefinition() == functionDefinition;
	}

	@Override
	public RexNode visit(ValueLiteralExpression valueLiteral) {
		DataType dataType = valueLiteral.getOutputDataType();

		if (hasRoot(dataType.getLogicalType(), SYMBOL)) {
			return relBuilder.getRexBuilder()
				.makeFlag(convertTableSymbol(valueLiteral.getValueAs(TableSymbol.class).get()));
		}

		RelDataType literalType = relBuilder.getTypeFactory().createTypeFromTypeInfo(
			getLiteralTypeInfo(valueLiteral),
			dataType.getLogicalType().isNullable());

		if (valueLiteral.isNull()) {
			return relBuilder.getRexBuilder().makeNullLiteral(literalType);
		}

		switch (dataType.getLogicalType().getTypeRoot()) {

			case DECIMAL:
				DecimalType logicalType = (DecimalType) dataType.getLogicalType();
				BigDecimal bigDecimal = valueLiteral.getValueAs(BigDecimal.class).orElse(null);
				RelDataType relType = relBuilder.getTypeFactory().createSqlType(
					SqlTypeName.DECIMAL,
					logicalType.getPrecision(),
					logicalType.getScale());
				return relBuilder.getRexBuilder().makeLiteral(bigDecimal, relType, false);
			case BIGINT:
				BigDecimal value = valueLiteral.getValueAs(BigDecimal.class).orElse(null);
				return relBuilder.getRexBuilder().makeBigintLiteral(value);
			case DATE:
				DateString dateString = valueLiteral.getValueAs(LocalDate.class)
					.map(ld -> new DateString(ld.getYear(), ld.getMonthValue(), ld.getDayOfMonth()))
					.orElse(null);
				return relBuilder.getRexBuilder().makeDateLiteral(dateString);
			case TIME_WITHOUT_TIME_ZONE:
				int precision = ((TimeType) dataType.getLogicalType()).getPrecision();
				TimeString timeString = valueLiteral.getValueAs(LocalTime.class)
					.map(lt -> new TimeString(lt.getHour(), lt.getMinute(), lt.getSecond()).withNanos(lt.getNano()))
					.orElse(null);
				return relBuilder.getRexBuilder().makeTimeLiteral(timeString, precision);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				int timestampPrecision = ((TimestampType) dataType.getLogicalType()).getPrecision();
				TimestampString timestampString = valueLiteral.getValueAs(Timestamp.class)
					.map(t -> {
						Calendar calendar = Calendar.getInstance();
						calendar.setTime(t);
						return calendar;
					})
					.map(TimestampString::fromCalendarFields)
					.orElse(null);
				return relBuilder.getRexBuilder().makeTimestampLiteral(timestampString, timestampPrecision);
			case INTERVAL_YEAR_MONTH:
				BigDecimal intervalYear = valueLiteral.getValueAs(Period.class)
					.map(p -> BigDecimal.valueOf(p.toTotalMonths()))
					.orElse(null);
				// This could work with new type system and handle resolution properly. It would break backwards
				// compatibility though of multiple builtin functions.
				SqlIntervalQualifier sqlYearQualifier = new SqlIntervalQualifier(
					TimeUnit.YEAR,
					TimeUnit.MONTH,
					SqlParserPos.ZERO
				);
				return relBuilder.getRexBuilder().makeIntervalLiteral(intervalYear, sqlYearQualifier);
			case INTERVAL_DAY_TIME:
				BigDecimal interval = valueLiteral.getValueAs(Duration.class)
					.map(p -> BigDecimal.valueOf(p.toMillis()))
					.orElse(null);
				// This could work with new type system and handle resolution properly. It would break backwards
				// compatibility though of multiple builtin functions.
				SqlIntervalQualifier sqlDayQualifier = new SqlIntervalQualifier(
					TimeUnit.DAY,
					TimeUnit.SECOND,
					SqlParserPos.ZERO
				);
				return relBuilder.getRexBuilder().makeIntervalLiteral(interval, sqlDayQualifier);
			case DOUBLE:
				BigDecimal doubleValue = valueLiteral.getValueAs(Double.class).map(BigDecimal::valueOf).get();
				RelDataType doubleType = relBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
				return relBuilder.getRexBuilder().makeApproxLiteral(doubleValue, doubleType);
		}

		Class<?> conversionClass = dataType.getConversionClass();
		return relBuilder.getRexBuilder()
			.makeLiteral(valueLiteral.getValueAs(conversionClass).orElse(null), literalType, false);
	}

	private static Enum<?> convertTableSymbol(TableSymbol symbol) {
		if (symbol instanceof TimeIntervalUnit) {
			switch ((TimeIntervalUnit) symbol) {
				case YEAR:
					return TimeUnitRange.YEAR;
				case YEAR_TO_MONTH:
					return TimeUnitRange.YEAR_TO_MONTH;
				case QUARTER:
					return TimeUnitRange.QUARTER;
				case MONTH:
					return TimeUnitRange.MONTH;
				case WEEK:
					return TimeUnitRange.WEEK;
				case DAY:
					return TimeUnitRange.DAY;
				case DAY_TO_HOUR:
					return TimeUnitRange.DAY_TO_HOUR;
				case DAY_TO_MINUTE:
					return TimeUnitRange.DAY_TO_MINUTE;
				case DAY_TO_SECOND:
					return TimeUnitRange.DAY_TO_SECOND;
				case HOUR:
					return TimeUnitRange.HOUR;
				case SECOND:
					return TimeUnitRange.SECOND;
				case HOUR_TO_MINUTE:
					return TimeUnitRange.HOUR_TO_MINUTE;
				case HOUR_TO_SECOND:
					return TimeUnitRange.HOUR_TO_SECOND;
				case MINUTE:
					return TimeUnitRange.MINUTE;
				case MINUTE_TO_SECOND:
					return TimeUnitRange.MINUTE_TO_SECOND;
			}
		} else if (symbol instanceof TimePointUnit) {
			switch ((TimePointUnit) symbol) {
				case YEAR:
					return TimeUnit.YEAR;
				case MONTH:
					return TimeUnit.MONTH;
				case DAY:
					return TimeUnit.DAY;
				case HOUR:
					return TimeUnit.HOUR;
				case MINUTE:
					return TimeUnit.MINUTE;
				case SECOND:
					return TimeUnit.SECOND;
				case QUARTER:
					return TimeUnit.QUARTER;
				case WEEK:
					return TimeUnit.WEEK;
				case MILLISECOND:
					return TimeUnit.MILLISECOND;
				case MICROSECOND:
					return TimeUnit.MICROSECOND;
			}
		}

		throw new TableException("Unknown symbol: " + symbol);
	}

	private SqlIntervalQualifier getSqlYearIntervalQualifier(YearMonthIntervalType intervalYearType) {
		SqlIntervalQualifier sqlYearQualifier = null;
		switch (intervalYearType.getResolution()) {
			case YEAR:
				sqlYearQualifier = new SqlIntervalQualifier(
					TimeUnit.YEAR,
					intervalYearType.getYearPrecision(),
					TimeUnit.YEAR,
					0,
					SqlParserPos.ZERO
				);
				break;
			case MONTH:
				sqlYearQualifier = new SqlIntervalQualifier(
					TimeUnit.MONTH,
					0,
					TimeUnit.MONTH,
					0,
					SqlParserPos.ZERO
				);
				break;
			case YEAR_TO_MONTH:
				sqlYearQualifier = new SqlIntervalQualifier(
					TimeUnit.YEAR,
					intervalYearType.getYearPrecision(),
					TimeUnit.MONTH,
					0,
					SqlParserPos.ZERO
				);
				break;
		}
		return sqlYearQualifier;
	}

	private SqlIntervalQualifier getSqlDayIntervalQualifier(DayTimeIntervalType intervalType) {
		SqlIntervalQualifier sqlDayQualifier = null;
		switch (intervalType.getResolution()) {
			case DAY:
				sqlDayQualifier = new SqlIntervalQualifier(
					TimeUnit.DAY,
					intervalType.getDayPrecision(),
					TimeUnit.DAY,
					0,
					SqlParserPos.ZERO
				);
				break;
			case DAY_TO_HOUR:
				sqlDayQualifier = new SqlIntervalQualifier(
					TimeUnit.DAY,
					intervalType.getDayPrecision(),
					TimeUnit.HOUR,
					0,
					SqlParserPos.ZERO
				);
				break;
			case DAY_TO_MINUTE:
				sqlDayQualifier = new SqlIntervalQualifier(
					TimeUnit.DAY,
					intervalType.getDayPrecision(),
					TimeUnit.MINUTE,
					0,
					SqlParserPos.ZERO
				);
				break;
			case DAY_TO_SECOND:
				sqlDayQualifier = new SqlIntervalQualifier(
					TimeUnit.DAY,
					intervalType.getDayPrecision(),
					TimeUnit.SECOND,
					intervalType.getFractionalPrecision(),
					SqlParserPos.ZERO
				);
				break;
			case HOUR:
				sqlDayQualifier = new SqlIntervalQualifier(
					TimeUnit.HOUR,
					0,
					TimeUnit.HOUR,
					0,
					SqlParserPos.ZERO
				);
				break;
			case HOUR_TO_MINUTE:
				sqlDayQualifier = new SqlIntervalQualifier(
					TimeUnit.HOUR,
					0,
					TimeUnit.MINUTE,
					0,
					SqlParserPos.ZERO
				);
				break;
			case HOUR_TO_SECOND:
				sqlDayQualifier = new SqlIntervalQualifier(
					TimeUnit.HOUR,
					0,
					TimeUnit.SECOND,
					intervalType.getFractionalPrecision(),
					SqlParserPos.ZERO
				);
				break;
			case MINUTE:
				sqlDayQualifier = new SqlIntervalQualifier(
					TimeUnit.MINUTE,
					0,
					TimeUnit.MINUTE,
					0,
					SqlParserPos.ZERO
				);
				break;
			case MINUTE_TO_SECOND:
				sqlDayQualifier = new SqlIntervalQualifier(
					TimeUnit.MINUTE,
					0,
					TimeUnit.SECOND,
					intervalType.getFractionalPrecision(),
					SqlParserPos.ZERO
				);
				break;
			case SECOND:
				sqlDayQualifier = new SqlIntervalQualifier(
					TimeUnit.SECOND,
					intervalType.getFractionalPrecision(),
					TimeUnit.SECOND,
					intervalType.getFractionalPrecision(),
					SqlParserPos.ZERO
				);
				break;
		}
		return sqlDayQualifier;
	}

	@Override
	public RexNode visit(FieldReferenceExpression fieldReference) {
		return relBuilder.field(inputCount, fieldReference.getInputIndex(), fieldReference.getFieldIndex());
	}

	@Override
	public RexNode visit(TypeLiteralExpression typeLiteral) {
		throw new TableException("Unsupported type literal expression: " + typeLiteral);
	}

	/**
	 * This method makes the planner more lenient for new data types defined for literals.
	 */
	private TypeInformation<?> getLiteralTypeInfo(ValueLiteralExpression literal) {
		LogicalType logicalType = literal.getOutputDataType().getLogicalType();

		if (hasRoot(logicalType, DECIMAL)) {
			if (literal.isNull()) {
				return Types.BIG_DEC;
			}
			BigDecimal value = literal.getValueAs(BigDecimal.class).get();
			if (hasPrecision(logicalType, value.precision()) && hasScale(logicalType, value.scale())) {
				return Types.BIG_DEC;
			}
		} else if (hasRoot(logicalType, CHAR)) {
			if (literal.isNull()) {
				return Types.STRING;
			}
			String value = literal.getValueAs(String.class).get();
			if (hasLength(logicalType, value.length())) {
				return Types.STRING;
			}
		} else if (hasRoot(logicalType, TIMESTAMP_WITHOUT_TIME_ZONE)) {
			if (getPrecision(logicalType) <= 3) {
				return Types.SQL_TIMESTAMP;
			}
		}

		return fromDataTypeToLegacyInfo(literal.getOutputDataType());
	}

}
