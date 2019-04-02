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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.AggregateFunctionDefinition;
import org.apache.flink.table.expressions.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.BuiltInFunctionDefinitions;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.FunctionDefinition;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.plan.logical.LogicalWindow;
import org.apache.flink.table.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.plan.logical.WindowAggregate;
import org.apache.flink.table.typeutils.RowIntervalTypeInfo;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.flink.table.expressions.ApiExpressionUtils.extractName;
import static org.apache.flink.table.expressions.FunctionDefinition.Type.AGGREGATE_FUNCTION;

/**
 * Utility class for creating a valid {@link AggregationTableOperation} or {@link WindowAggregate}.
 */
@Internal
public class AggregateOperationFactory {

	private final boolean isStreaming;
	private final ExpressionBridge<PlannerExpression> expressionBridge;
	private final GroupingExpressionValidator groupingExpressionValidator = new GroupingExpressionValidator();
	private final NoChainedAggregates noChainedAggregates = new NoChainedAggregates();
	private final ValidateDistinct validateDistinct = new ValidateDistinct();
	private AggregationExpressionValidator aggregationsValidator = new AggregationExpressionValidator();

	public AggregateOperationFactory(ExpressionBridge<PlannerExpression> expressionBridge, boolean isStreaming) {
		this.expressionBridge = expressionBridge;
		this.isStreaming = isStreaming;
	}

	/**
	 * Creates a valid {@link AggregationTableOperation} operation.
	 *
	 * @param groupings expressions describing grouping key of aggregates
	 * @param aggregates expressions describing aggregation functions
	 * @param child relational operation on top of which to apply the aggregation
	 * @return valid aggregate operation
	 */
	public TableOperation createAggregate(
			List<Expression> groupings,
			List<Expression> aggregates,
			TableOperation child) {

		validateGroupings(groupings);
		validateAggregates(aggregates);

		List<PlannerExpression> convertedGroupings = bridge(groupings);
		List<PlannerExpression> convertedAggregates = bridge(aggregates);

		TypeInformation[] fieldTypes = Stream.concat(
			convertedGroupings.stream(),
			convertedAggregates.stream()
		).map(PlannerExpression::resultType)
			.toArray(TypeInformation[]::new);

		String[] fieldNames = Stream.concat(
			groupings.stream(),
			aggregates.stream()
		).map(expr -> extractName(expr).orElseGet(expr::toString))
			.toArray(String[]::new);

		TableSchema tableSchema = new TableSchema(fieldNames, fieldTypes);

		return new AggregationTableOperation(groupings, aggregates, child, tableSchema);
	}

	/**
	 * Creates a valid {@link WindowAggregate} operation.
	 *
	 * @param groupings expressions describing grouping key of aggregates
	 * @param aggregates expressions describing aggregation functions
	 * @param windowProperties expressions describing window properties
	 * @param window grouping window of this aggregation
	 * @param child relational operation on top of which to apply the aggregation
	 * @return valid window aggregate operation
	 */
	public WindowAggregate createWindowAggregate(
			List<Expression> groupings,
			List<Expression> aggregates,
			List<Expression> windowProperties,
			LogicalWindow window,
			TableOperation child) {

		validateGroupings(groupings);
		validateAggregates(aggregates);

		List<PlannerExpression> convertedGroupings = bridge(groupings);
		List<PlannerExpression> convertedAggregates = bridge(aggregates);
		List<PlannerExpression> convertedWindowProperties = bridge(windowProperties);

		validateWindow(windowProperties, window);

		return new WindowAggregate(convertedGroupings, window, convertedWindowProperties, convertedAggregates, child);
	}

	private void validateWindow(List<Expression> windowProperties, LogicalWindow window) {
		window.validate(isStreaming);

		if (!windowProperties.isEmpty()) {
			if (window instanceof TumblingGroupWindow) {
				PlannerExpression windowSize = ((TumblingGroupWindow) window).size();
				if (isRowCountLiteral(windowSize)) {
					throw new ValidationException("Window start and Window end cannot be selected " +
						"for a row-count Tumbling window.");
				}
			} else if (window instanceof SlidingGroupWindow) {
				PlannerExpression windowSize = ((SlidingGroupWindow) window).size();
				if (isRowCountLiteral(windowSize)) {
					throw new ValidationException("Window start and Window end cannot be selected " +
						"for a row-count Sliding window.");
				}
			}
		}
	}

	private boolean isRowCountLiteral(PlannerExpression expr) {
		return expr.resultType() == RowIntervalTypeInfo.INTERVAL_ROWS;
	}

	private List<PlannerExpression> bridge(List<Expression> aggregates) {
		return aggregates.stream()
			.map(expressionBridge::bridge)
			.collect(Collectors.toList());
	}

	private void validateGroupings(List<Expression> groupings) {
		groupings.forEach(expr -> expr.accept(groupingExpressionValidator));
	}

	private void validateAggregates(List<Expression> aggregates) {
		aggregates.forEach(agg -> agg.accept(aggregationsValidator));
	}

	private class AggregationExpressionValidator extends ApiExpressionDefaultVisitor<Void> {

		@Override
		public Void visitCall(CallExpression call) {
			FunctionDefinition functionDefinition = call.getFunctionDefinition();
			if (functionDefinition.getType() == AGGREGATE_FUNCTION) {
				if (functionDefinition == BuiltInFunctionDefinitions.DISTINCT) {
					call.getChildren().forEach(expr -> expr.accept(validateDistinct));
				} else {
					if (functionDefinition instanceof AggregateFunctionDefinition) {
						if (requiresOver(functionDefinition)) {
							throw new ValidationException(format(
								"OVER clause is necessary for window functions: [%s].",
								call));
						}
					}

					call.getChildren().forEach(child -> child.accept(noChainedAggregates));
				}
			} else if (functionDefinition == BuiltInFunctionDefinitions.AS) {
				// skip alias
				call.getChildren().get(0).accept(this);
			} else {
				failExpression(call);
			}
			return null;
		}

		private boolean requiresOver(FunctionDefinition functionDefinition) {
			return ((AggregateFunctionDefinition) functionDefinition).getAggregateFunction().requiresOver();
		}

		@Override
		protected Void defaultMethod(Expression expression) {
			failExpression(expression);
			return null;
		}

		private void failExpression(Expression expression) {
			throw new ValidationException(format("expression '%s' is invalid because it is neither" +
				" present in group by nor an aggregate function", expression));
		}
	}

	private class ValidateDistinct extends ApiExpressionDefaultVisitor<Void> {

		@Override
		public Void visitCall(CallExpression call) {
			if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.DISTINCT) {
				throw new ValidationException("It's not allowed to use an aggregate function as " +
					"input of another aggregate function");
			} else if (call.getFunctionDefinition().getType() != AGGREGATE_FUNCTION) {
				throw new ValidationException("Distinct operator can only be applied to aggregation expressions!");
			} else {
				call.getChildren().forEach(child -> child.accept(noChainedAggregates));
			}
			return null;
		}

		@Override
		protected Void defaultMethod(Expression expression) {
			return null;
		}
	}

	private class NoChainedAggregates extends ApiExpressionDefaultVisitor<Void> {

		@Override
		public Void visitCall(CallExpression call) {
			if (call.getFunctionDefinition().getType() == AGGREGATE_FUNCTION) {
				throw new ValidationException("It's not allowed to use an aggregate function as " +
					"input of another aggregate function");
			}
			return null;
		}

		@Override
		protected Void defaultMethod(Expression expression) {
			return null;
		}
	}

	private class GroupingExpressionValidator extends ApiExpressionDefaultVisitor<Void> {
		@Override
		protected Void defaultMethod(Expression expression) {
			TypeInformation<?> groupingType = expressionBridge.bridge(expression).resultType();

			if (!groupingType.isKeyType()) {
				throw new ValidationException(format("expression %s cannot be used as a grouping expression " +
					"because it's not a valid key type which must be hashable and comparable", expression));
			}
			return null;
		}
	}
}
