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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.expressions.Aggregation;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.ExpressionDefaultVisitor;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.utils.TableSqlFunction;
import org.apache.flink.table.plan.logical.LogicalNode;
import org.apache.flink.table.plan.schema.FlinkTableFunctionImpl;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.tools.RelBuilder.GroupKey;

import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.flink.table.expressions.ApiExpressionUtils.extractValue;
import static org.apache.flink.table.expressions.ApiExpressionUtils.isFunctionOfType;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.expressions.FunctionDefinition.Type.AGGREGATE_FUNCTION;

/**
 * Converter from Flink's specific relational representation: {@link TableOperation} to Calcite's specific relational
 * representation: {@link RelNode}.
 */
@Internal
public class TableOperationToRelNodeConverter extends TableOperationDefaultVisitor<RelNode> {

	/**
	 * Supplier for {@link TableOperationToRelNodeConverter} that can wrap given {@link RelBuilder}.
	 */
	@Internal
	public static class ToRelConverterSupplier {
		private final ExpressionBridge<PlannerExpression> expressionBridge;

		public ToRelConverterSupplier(ExpressionBridge<PlannerExpression> expressionBridge) {
			this.expressionBridge = expressionBridge;
		}

		public TableOperationToRelNodeConverter get(RelBuilder relBuilder) {
			return new TableOperationToRelNodeConverter(relBuilder, expressionBridge);
		}
	}

	private final RelBuilder relBuilder;
	private final SingleRelVisitor singleRelVisitor = new SingleRelVisitor();
	private final ExpressionBridge<PlannerExpression> expressionBridge;
	private final AggregateVisitor aggregateVisitor = new AggregateVisitor();

	public TableOperationToRelNodeConverter(
			RelBuilder relBuilder,
			ExpressionBridge<PlannerExpression> expressionBridge) {
		this.relBuilder = relBuilder;
		this.expressionBridge = expressionBridge;
	}

	@Override
	public RelNode defaultMethod(TableOperation other) {
		other.getChildren().forEach(child -> relBuilder.push(child.accept(this)));
		return other.accept(singleRelVisitor);
	}

	private class SingleRelVisitor implements TableOperationVisitor<RelNode> {

		@Override
		public RelNode visitProject(ProjectTableOperation projection) {
			List<RexNode> rexNodes = convertToRexNodes(projection.getProjectList());

			return relBuilder.project(
				rexNodes,
				asList(projection.getTableSchema().getFieldNames()),
				true)
				.build();
		}

		@Override
		public RelNode visitAggregate(AggregationTableOperation aggregate) {
			List<AggCall> aggregations = aggregate.getAggregateExpressions()
				.stream()
				.map(expr -> expr.accept(aggregateVisitor))
				.collect(toList());

			List<RexNode> groupings = convertToRexNodes(aggregate.getGroupingExpressions());
			GroupKey groupKey = relBuilder.groupKey(groupings);

			return relBuilder.aggregate(groupKey, aggregations).build();
		}

		@Override
		public RelNode visitAlgebraicOperation(AlgebraicTableOperation algebraicOperation) {
			switch (algebraicOperation.getType()) {
				case INTERSECT:
					relBuilder.intersect(algebraicOperation.isAll());
					break;
				case MINUS:
					relBuilder.minus(algebraicOperation.isAll());
					break;
				case UNION:
					relBuilder.union(algebraicOperation.isAll());
					break;
			}
			return relBuilder.build();
		}

		@Override
		public RelNode visitFilter(FilterTableOperation filter) {
			RexNode rexNode = convertToRexNode(filter.getCondition());
			return relBuilder.filter(rexNode).build();
		}

		@Override
		public RelNode visitDistinct(DistinctTableOperation distinct) {
			return relBuilder.distinct().build();
		}

		@Override
		public RelNode visitSort(SortTableOperation sort) {
			List<RexNode> rexNodes = convertToRexNodes(sort.getOrder());
			return relBuilder.sortLimit(sort.getOffset(), sort.getFetch(), rexNodes)
				.build();
		}

		@Override
		public RelNode visitOther(TableOperation other) {
			if (other instanceof LogicalNode) {
				return ((LogicalNode) other).toRelNode(relBuilder);
			}

			throw new TableException("Unknown table operation: " + other);
		}

		@Override
		public RelNode visitCalculatedTable(CalculatedTableOperation calculatedTable) {
			String[] fieldNames = calculatedTable.getTableSchema().getFieldNames();
			int[] fieldIndices = IntStream.range(0, fieldNames.length).toArray();
			TypeInformation<?> resultType = calculatedTable.getResultType();

			@SuppressWarnings("unchecked")
			FlinkTableFunctionImpl function = new FlinkTableFunctionImpl(
				resultType,
				fieldIndices,
				fieldNames);
			TableFunction<?> tableFunction = calculatedTable.getTableFunction();

			FlinkTypeFactory typeFactory = (FlinkTypeFactory) relBuilder.getTypeFactory();
			TableSqlFunction sqlFunction = new TableSqlFunction(
				tableFunction.functionIdentifier(),
				tableFunction.toString(),
				tableFunction,
				resultType,
				typeFactory,
				function);

			List<RexNode> parameters = convertToRexNodes(calculatedTable.getParameters());

			return LogicalTableFunctionScan.create(
				relBuilder.peek().getCluster(),
				Collections.emptyList(),
				relBuilder.call(sqlFunction, parameters),
				function.getElementType(null),
				function.getRowType(typeFactory, null),
				null);
		}

		private RexNode convertToRexNode(Expression expression) {
			return expressionBridge.bridge(expression).toRexNode(relBuilder);
		}

		private List<RexNode> convertToRexNodes(List<Expression> expressions) {
			return expressions
				.stream()
				.map(expressionBridge::bridge)
				.map(expr -> expr.toRexNode(relBuilder))
				.collect(toList());
		}
	}

	private class AggregateVisitor extends ExpressionDefaultVisitor<AggCall> {

		@Override
		public AggCall visitCall(CallExpression call) {
			if (call.getFunctionDefinition() == AS) {
				String aggregateName = extractValue(call.getChildren().get(1),
					Types.STRING).orElseThrow(() -> new TableException(
					"Unexpected name"));

				Expression aggregate = call.getChildren().get(0);
				if (isFunctionOfType(aggregate, AGGREGATE_FUNCTION)) {
					return ((Aggregation) expressionBridge.bridge(aggregate))
						.toAggCall(aggregateName, false, relBuilder);
				}
			}
			throw new TableException("Expected named aggregate. Got: " + call);
		}

		@Override
		protected AggCall defaultMethod(Expression expression) {
			throw new TableException("Unexpected expression: " + expression);
		}
	}
}
