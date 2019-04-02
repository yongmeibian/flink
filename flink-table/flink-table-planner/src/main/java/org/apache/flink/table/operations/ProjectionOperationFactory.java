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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.FunctionDefinition;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.CAST;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.GET;

/**
 * Utility class for creating valid {@link Project} operation.
 */
@Internal
public class ProjectionOperationFactory {

	private final TransitiveExtractNameVisitor extractTransitiveNameVisitor = new TransitiveExtractNameVisitor();
	private final NamingVisitor namingVisitor = new NamingVisitor();
	private final StripAliases stripAliases = new StripAliases();
	private int currentFieldIndex = 0;

	private final ExpressionBridge<PlannerExpression> expressionBridge;

	public ProjectionOperationFactory(ExpressionBridge<PlannerExpression> expressionBridge) {
		this.expressionBridge = expressionBridge;
	}

	public TableOperation create(
			List<Expression> projectList,
			TableOperation childNode,
			boolean explicitAlias) {

		final List<Expression> namedExpressions = nameExpressions(projectList);
		String[] fieldNames = validateAndGetUniqueNames(namedExpressions);

		final List<Expression> finalExpression;
		if (explicitAlias) {
			finalExpression = namedExpressions;
		} else {
			finalExpression = namedExpressions.stream()
				.map(expr -> expr.accept(stripAliases))
				.collect(Collectors.toList());
		}

		TypeInformation[] fieldTypes = namedExpressions.stream()
			.map(expressionBridge::bridge)
			.map(PlannerExpression::resultType)
			.toArray(TypeInformation[]::new);

		TableSchema tableSchema = new TableSchema(fieldNames, fieldTypes);

		return new ProjectTableOperation(finalExpression, childNode, tableSchema);
	}

	private String[] validateAndGetUniqueNames(List<Expression> namedExpressions) {
		// we need to maintain field names order to match with types
		final Set<String> names = new LinkedHashSet<>();

		ApiExpressionUtils.extractNames(namedExpressions).stream()
			.map(name -> name.orElseThrow(() -> new TableException("Could not name a field in a projection.")))
			.forEach(name -> {
				if (!names.add(name)) {
					throw new ValidationException("Ambiguous column name: " + name);
				}
			});

		return names.toArray(new String[0]);
	}

	private List<Expression> nameExpressions(List<Expression> expression) {
		return IntStream.range(0, expression.size())
			.mapToObj(idx -> {
				currentFieldIndex = idx;
				return expression.get(idx).accept(namingVisitor);
			})
			.collect(Collectors.toList());
	}

	private class NamingVisitor extends ApiExpressionDefaultVisitor<Expression> {

		@Override
		public Expression visitCall(CallExpression call) {
			FunctionDefinition functionDefinition = call.getFunctionDefinition();
			final Optional<String> rename;
			if (functionDefinition.equals(CAST)) {
				rename = nameForCast(call);
			} else if (functionDefinition.equals(GET)) {
				rename = nameForGet(call);
			} else if (functionDefinition.equals(AS)) {
				rename = Optional.empty();
			} else {
				rename = Optional.of(getUniqueName());
			}

			return rename.map(name -> new CallExpression(AS, Arrays.asList(call, valueLiteral(name)))).orElse(call);
		}

		private Optional<String> nameForGet(CallExpression call) {
			return Optional.of(call.accept(extractTransitiveNameVisitor)
				.orElseGet(ProjectionOperationFactory.this::getUniqueName));
		}

		private Optional<String> nameForCast(CallExpression call) {
			Optional<String> innerName = call.getChildren().get(0).accept(extractTransitiveNameVisitor);
			Expression type = call.getChildren().get(1);
			return Optional.of(innerName.map(n -> String.format("%s-%s", n, type))
				.orElseGet(ProjectionOperationFactory.this::getUniqueName));
		}

		@Override
		public Expression visitValueLiteral(ValueLiteralExpression valueLiteralExpression) {
			return new CallExpression(AS, Arrays.asList(valueLiteralExpression, valueLiteral(getUniqueName())));
		}

		@Override
		protected Expression defaultMethod(Expression expression) {
			return expression;
		}
	}

	private class StripAliases extends ApiExpressionDefaultVisitor<Expression> {

		@Override
		public Expression visitCall(CallExpression call) {
			if (call.getFunctionDefinition().equals(AS)) {
				return call.getChildren().get(0).accept(this);
			} else {
				return call;
			}
		}

		@Override
		protected Expression defaultMethod(Expression expression) {
			return expression;
		}
	}

	private class TransitiveExtractNameVisitor extends ApiExpressionDefaultVisitor<Optional<String>> {

		@Override
		public Optional<String> visitCall(CallExpression call) {
			if (call.getFunctionDefinition().equals(GET)) {
				return extractNameFromGet(call);
			} else {
				return defaultMethod(call);
			}
		}

		@Override
		protected Optional<String> defaultMethod(Expression expression) {
			return ApiExpressionUtils.extractName(expression);
		}

		private Optional<String> extractNameFromGet(CallExpression call) {
			Expression child = call.getChildren().get(0);
			ValueLiteralExpression key = (ValueLiteralExpression) call.getChildren().get(1);
			final String keySuffix;
			if (key.getType().equals(Types.INT)) {
				keySuffix = "$_" + key.getValue();
			} else {
				keySuffix = "$" + key.getValue();
			}
			return child.accept(this).map(p -> p + keySuffix);
		}
	}

	private String getUniqueName() {
		return "_c" + currentFieldIndex++;
	}
}
