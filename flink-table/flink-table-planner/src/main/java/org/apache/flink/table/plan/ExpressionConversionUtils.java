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

package org.apache.flink.table.plan;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.FunctionDefinition;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedFieldReferenceExpression;
import org.apache.flink.table.validate.FunctionCatalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedFieldRef;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.PROCTIME;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.ROWTIME;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.WINDOW_END;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.WINDOW_START;
import static org.apache.flink.table.expressions.FunctionDefinition.Type.AGGREGATE_FUNCTION;

public class ExpressionConversionUtils {

	/**
	 * Extracts and deduplicates all aggregation and window property expressions (zero, one, or more)
	 * from the given expressions.
	 *
	 * @param exprs a list of expressions to extract
	 * @param uniqueAttributeGenerator a supplier that every time returns a unique attribute
	 * @return a Tuple2, the first field contains the extracted and deduplicated aggregations,
	 * and the second field contains the extracted and deduplicated window properties.
	 */
	public static Tuple2<Map<Expression, String>, Map<Expression, String>> extractAggregationsAndProperties(
			Iterable<Expression> exprs,
			Supplier<String> uniqueAttributeGenerator) {
		AggregationAndPropertiesSplitter splitter = new AggregationAndPropertiesSplitter(uniqueAttributeGenerator);
		for (Expression expression : exprs) {
			expression.accept(splitter);
		}

		return Tuple2.of(splitter.aggregates, splitter.properties);
	}

	private static final Set<FunctionDefinition> WINDOW_PROPERTIES = new HashSet<>(Arrays.asList(
		WINDOW_START, WINDOW_END, PROCTIME, ROWTIME
	));

	private static class AggregationAndPropertiesSplitter extends DefaultExpressionVisitor<Void> {

		private final Map<Expression, String> aggregates = new LinkedHashMap<>();
		private final Map<Expression, String> properties = new LinkedHashMap<>();
		private final Supplier<String> uniqueAttributeGenerator;

		private AggregationAndPropertiesSplitter(Supplier<String> uniqueAttributeGenerator) {
			this.uniqueAttributeGenerator = uniqueAttributeGenerator;
		}

		@Override
		public Void visitUnresolvedCall(UnresolvedCallExpression unresolvedCall) {
			throw new IllegalStateException("All calls should be resolved by now. Got: " + unresolvedCall);
		}

		@Override
		public Void visitCall(CallExpression call) {
			FunctionDefinition functionDefinition = call.getFunctionDefinition();
			if (functionDefinition.getType() == AGGREGATE_FUNCTION) {
				aggregates.put(call, uniqueAttributeGenerator.get());
			} else if (WINDOW_PROPERTIES.contains(functionDefinition)) {
				properties.put(call, uniqueAttributeGenerator.get());
			} else {
				call.getChildren().forEach(c -> c.accept(this));
			}
			return null;
		}

		@Override
		protected Void defaultMethod(Expression expression) {
			return null;
		}
	}

	/**
	 * Replaces expressions with deduplicated aggregations and properties.
	 *
	 * @param exprs     a list of expressions to replace
	 * @param aggNames  the deduplicated aggregations
	 * @param propNames the deduplicated properties
	 * @return a list of replaced expressions
	 */
	public static List<Expression> replaceAggregationsAndProperties(
			Iterable<Expression> exprs,
			Map<Expression, String> aggNames,
			Map<Expression, String> propNames) {
		AggregationAndPropertiesReplacer replacer = new AggregationAndPropertiesReplacer(aggNames, propNames);
		return StreamSupport.stream(exprs.spliterator(), false)
			.map(expr -> expr.accept(replacer))
			.collect(Collectors.toList());
	}

	private static class AggregationAndPropertiesReplacer extends DefaultExpressionVisitor<Expression> {

		private final Map<Expression, String> aggregates;
		private final Map<Expression, String> properties;

		private AggregationAndPropertiesReplacer(
				Map<Expression, String> aggregates,
				Map<Expression, String> properties) {
			this.aggregates = aggregates;
			this.properties = properties;
		}

		@Override
		public Expression visitUnresolvedCall(UnresolvedCallExpression unresolvedCall) {
			throw new IllegalStateException("All calls should be resolved by now. Got: " + unresolvedCall);
		}

		@Override
		public Expression visitCall(CallExpression call) {
			if (aggregates.get(call) != null) {
				return unresolvedFieldRef(aggregates.get(call));
			} else if (properties.get(call) != null) {
				return unresolvedFieldRef(properties.get(call));
			}

			List<Expression> args = ((Expression) call).getChildren()
				.stream()
				.map(c -> c.accept(this))
				.collect(Collectors.toList());
			return new CallExpression(call.getFunctionDefinition(), args);
		}

		@Override
		protected Expression defaultMethod(Expression expression) {
			return expression;
		}
	}

	/**
	 * Extract all field references from the given expressions.
	 *
	 * @param exprs a list of expressions to extract
	 * @return a list of field references extracted from the given expressions
	 */
	public static Set<Expression> extractFieldReferences(Iterable<Expression> exprs) {
		FieldReferenceExtractor referenceExtractor = new FieldReferenceExtractor();
		return StreamSupport.stream(exprs.spliterator(), false)
			.flatMap(expr -> expr.accept(referenceExtractor).stream())
			.collect(Collectors.toSet());
	}

	private static class FieldReferenceExtractor extends DefaultExpressionVisitor<Set<Expression>> {

		@Override
		public Set<Expression> visitCall(CallExpression call) {
			FunctionDefinition functionDefinition = call.getFunctionDefinition();
			if (WINDOW_PROPERTIES.contains(functionDefinition)) {
				return Collections.emptySet();
			} else {
				return ((Expression) call).getChildren()
					.stream()
					.flatMap(c -> c.accept(this).stream())
					.collect(Collectors.toSet());
			}
		}

		@Override
		public Set<Expression> visitUnresolvedCall(UnresolvedCallExpression unresolvedCall) {
			throw new IllegalStateException("All calls should be resolved by now. Got: " + unresolvedCall);
		}

		@Override
		public Set<Expression> visitFieldReference(FieldReferenceExpression fieldReference) {
			return Collections.singleton(fieldReference);
		}

		@Override
		public Set<Expression> visitUnresolvedFieldReference(UnresolvedFieldReferenceExpression fieldReference) {
			return Collections.singleton(fieldReference);
		}

		@Override
		protected Set<Expression> defaultMethod(Expression expression) {
			return Collections.emptySet();
		}
	}

	public static List<Expression> resolveCalls(List<Expression> expressions, FunctionCatalog functionCatalog) {
		UnresolvedCallResolver callResolver = new UnresolvedCallResolver(functionCatalog);
		return expressions.stream().map(expr -> expr.accept(callResolver)).collect(Collectors.toList());
	}

	/**
	 * Resolves calls with function names to calls with actual function definitions.
	 */
	private static class UnresolvedCallResolver extends DefaultExpressionVisitor<Expression> {

		private final FunctionCatalog functionCatalog;

		private UnresolvedCallResolver(FunctionCatalog functionCatalog) {
			this.functionCatalog = functionCatalog;
		}

		public Expression visitUnresolvedCall(UnresolvedCallExpression unresolvedCall) {
			FunctionDefinition functionDefinition = functionCatalog.lookupFunction(unresolvedCall.getUnresolvedName());
			return new CallExpression(
				functionDefinition,
				unresolvedCall.getChildren().stream().map(expr -> expr.accept(this)).collect(Collectors.toList()));
		}

		public Expression visitCall(CallExpression call) {
			boolean exprResolved = false;
			List<Expression> resolvedArgs = new ArrayList<>();
			for (Expression child : call.getChildren()) {
				Expression resolved = child.accept(this);
				if (resolved != child) {
					exprResolved = true;
				}
				resolvedArgs.add(resolved);
			}

			if (exprResolved) {
				return new CallExpression(call.getFunctionDefinition(), resolvedArgs);
			} else {
				return call;
			}
		}

		@Override
		protected Expression defaultMethod(Expression expression) {
			return expression;
		}
	}

}
