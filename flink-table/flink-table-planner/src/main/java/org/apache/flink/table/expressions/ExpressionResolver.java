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
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.plan.DefaultExpressionVisitor;
import org.apache.flink.table.plan.logical.LogicalNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;

/**
 * Tries to resolve all unresolved expressions such as {@link UnresolvedFieldReferenceExpression},
 * {@link UnresolvedCallExpression} or calls such as {@link BuiltInFunctionDefinitions#OVER}.
 */
public class ExpressionResolver {

	private final Map<String, FieldReferenceExpression> fieldReferences;

	private final ExpressionResolverVisitor resolverVisitor = new ExpressionResolverVisitor();
	private final FieldFlatteningVisitor flatteningVisitor = new FieldFlatteningVisitor();
	private final FlatteningCallVisitor flatteningCallVisitor = new FlatteningCallVisitor();

	private ExpressionResolver(LogicalNode[] inputs) {
		this.fieldReferences = IntStream.range(0, inputs.length)
			.mapToObj(i -> Tuple2.of(i, inputs[i].tableSchema()))
			.flatMap(p -> IntStream.range(0, p.f1.getFieldCount())
				.mapToObj(i -> new FieldReferenceExpression(
					p.f1.getFieldName(i).get(),
					p.f1.getFieldType(i).get(),
					p.f0,
					i)))
			.collect(Collectors.toMap(
				FieldReferenceExpression::getName,
				Function.identity(),
				(fieldRef1, fieldRef2) -> {
					throw new ValidationException("Ambigous column");
				}
			));
	}

	public static ExpressionResolver resolverFor(LogicalNode... inputs) {
		return new ExpressionResolver(inputs);
	}

	public List<Expression> resolve(Expression expression) {
		return expression.accept(flatteningVisitor)
			.stream()
			.map(expr -> expr.accept(resolverVisitor))
			.flatMap(expr -> expr.accept(flatteningCallVisitor).stream())
			.collect(Collectors.toList());
	}

	private class FieldFlatteningVisitor extends DefaultExpressionVisitor<List<Expression>> {

		@Override
		public List<Expression> visitUnresolvedFieldReference(UnresolvedFieldReferenceExpression fieldReference) {
			if (fieldReference.getName().equals("*")) {
				return new ArrayList<>(fieldReferences.values());
			} else {
				return singletonList(fieldReference);
			}
		}

		@Override
		protected List<Expression> defaultMethod(Expression expression) {
			return singletonList(expression);
		}
	}

	private class FlatteningCallVisitor extends DefaultExpressionVisitor<List<Expression>> {

		private final PlannerExpressionConverter expressionConverter = PlannerExpressionConverter.INSTANCE();

		@Override
		public List<Expression> visitCall(CallExpression call) {
			if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.FLATTEN) {
				Expression arg = call.getChildren().get(0);
				TypeInformation<?> resultType = arg.accept(expressionConverter).resultType();
				if (resultType instanceof CompositeType) {
					CompositeType<?> compositeType = (CompositeType<?>) resultType;
					return IntStream.range(0, compositeType.getArity())
						.mapToObj(idx -> new CallExpression(
								BuiltInFunctionDefinitions.GET,
								asList(arg, valueLiteral(idx))
							)
						)
						.collect(Collectors.toList());
				} else {
					return singletonList(arg);
				}
			}

			return singletonList(call);
		}

		@Override
		protected List<Expression> defaultMethod(Expression expression) {
			return singletonList(expression);
		}
	}

	private class ExpressionResolverVisitor extends DefaultExpressionVisitor<Expression> {

		@Override
		public Expression visitUnresolvedCall(UnresolvedCallExpression unresolvedCall) {
			throw new IllegalStateException("All calls should be resolved by now. Got: " + unresolvedCall);
		}

		@Override
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
		public Expression visitUnresolvedFieldReference(UnresolvedFieldReferenceExpression fieldReference) {
			FieldReferenceExpression resolvedReference = fieldReferences.get(fieldReference.getName());

			if (resolvedReference != null) {
				return resolvedReference;
			} else {
				throw new ValidationException("Could not resolve field reference: " + fieldReference.getName());
			}
		}

		@Override
		protected Expression defaultMethod(Expression expression) {
			return expression;
		}
	}
}
