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
import org.apache.flink.table.plan.logical.LogicalOverWindow;
import org.apache.flink.table.typeutils.RowIntervalTypeInfo;
import org.apache.flink.table.typeutils.TypeCoercion;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.flink.table.expressions.ApiExpressionUtils.typeLiteral;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.GET;
import static org.apache.flink.table.util.JavaScalaConversionUtil.toJava;

/**
 * Tries to resolve all unresolved expressions such as {@link UnresolvedFieldReferenceExpression},
 * {@link UnresolvedCallExpression} or calls such as {@link BuiltInFunctionDefinitions#OVER}.
 */
public class ExpressionResolver {

	private final Map<String, FieldReferenceExpression> fieldReferences;
	private final Map<Expression, LogicalOverWindow> overWindows;

	private final ExpressionResolverVisitor resolverVisitor = new ExpressionResolverVisitor();
	private final FieldFlatteningVisitor flatteningStarVisitor = new FieldFlatteningVisitor();
	private final FlatteningCallVisitor flatteningCallVisitor = new FlatteningCallVisitor();
	private final CallArgumentsCastingVisitor callArgumentsCastingVisitor = new CallArgumentsCastingVisitor();

	private final PlannerExpressionConverter bridgeConverter = PlannerExpressionConverter.INSTANCE();

	private ExpressionResolver(LogicalNode[] inputs, List<LogicalOverWindow> overWindows) {
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

		this.overWindows = overWindows.stream()
			.map(window -> new LogicalOverWindow(
				window.alias(),
				window.partitionBy().stream().map(expr -> expr.accept(resolverVisitor)).collect(Collectors.toList()),
				window.orderBy().accept(resolverVisitor),
				window.preceding().accept(resolverVisitor),
				window.following().map(expr -> expr.accept(resolverVisitor))
			))
			.collect(Collectors.toMap(
				LogicalOverWindow::alias,
				Function.identity()
			));
	}

	public static ExpressionResolverBuilder resolverFor(LogicalNode... inputs) {
		return new ExpressionResolverBuilder(inputs);
	}

	public static class ExpressionResolverBuilder {
		private final LogicalNode[] logicalNodes;
		private List<LogicalOverWindow> logicalOverWindows = new ArrayList<>();


		private ExpressionResolverBuilder(LogicalNode[] logicalNodes) {
			this.logicalNodes = logicalNodes;
		}

		public ExpressionResolverBuilder withOverWindows(List<LogicalOverWindow> windows) {
			this.logicalOverWindows = windows;
			return this;
		}

		public ExpressionResolver build() {
			return new ExpressionResolver(logicalNodes, logicalOverWindows);
		}
	}

	public List<Expression> resolve(Expression expression) {
		return expression.accept(flatteningStarVisitor)
			.stream()
			.map(expr -> expr.accept(resolverVisitor))
			.flatMap(expr -> expr.accept(flatteningCallVisitor).stream())
			.map(expr -> expr.accept(callArgumentsCastingVisitor))
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

		@Override
		public List<Expression> visitCall(CallExpression call) {
			if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.FLATTEN) {
				return executeFlatten(call);
			}

			return singletonList(call);
		}

		private List<Expression> executeFlatten(CallExpression call) {
			Expression arg = call.getChildren().get(0);
			PlannerExpression plannerExpression = arg.accept(bridgeConverter);
			plannerExpression.validateInput();
			TypeInformation<?> resultType = plannerExpression.resultType();
			if (resultType instanceof CompositeType) {
				return flattenCompositeType(arg, (CompositeType<?>) resultType);
			} else {
				return singletonList(arg);
			}
		}

		private List<Expression> flattenCompositeType(Expression arg, CompositeType<?> resultType) {
			return IntStream.range(0, resultType.getArity())
				.mapToObj(idx -> new CallExpression(GET, asList(arg, valueLiteral(idx))))
				.collect(Collectors.toList());
		}

		@Override
		protected List<Expression> defaultMethod(Expression expression) {
			return singletonList(expression);
		}
	}

	private class ExpressionResolverVisitor extends DefaultExpressionVisitor<Expression> {

		@Override
		public Expression visitCall(CallExpression call) {

			boolean argsChanged = false;
			List<Expression> resolvedArgs = new ArrayList<>();
			List<Expression> callArgs = getCallArgs(call);

			for (Expression child : callArgs) {
				Expression resolved = child.accept(this);
				if (resolved != child) {
					argsChanged = true;
				}
				resolvedArgs.add(resolved);
			}

			if (argsChanged) {
				return new CallExpression(call.getFunctionDefinition(), resolvedArgs);
			} else {
				return call;
			}
		}

		private List<Expression> getCallArgs(CallExpression call) {
			List<Expression> callArgs = call.getChildren();

			if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.OVER) {
				Expression alias = callArgs.get(1);
				LogicalOverWindow referenceWindow = overWindows.get(alias);
				if (referenceWindow == null) {
					throw new ValidationException("Could not resolve over call.");
				}

				Expression following = calculateOverWindowFollowing(referenceWindow);
				List<Expression> newArgs = new ArrayList<>(asList(
					callArgs.get(0),
					referenceWindow.orderBy(),
					referenceWindow.preceding(),
					following));

				newArgs.addAll(referenceWindow.partitionBy());
				return newArgs;
			} else {
				return callArgs;
			}
		}

		private Expression calculateOverWindowFollowing(LogicalOverWindow referenceWindow) {
			return referenceWindow.following().orElseGet(() -> {
					PlannerExpression preceding = referenceWindow.preceding().accept(bridgeConverter);
					if (preceding.resultType() instanceof RowIntervalTypeInfo) {
						return new CurrentRow();
					} else {
						return new CurrentRange();
					}
				}
			);
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

	private class CallArgumentsCastingVisitor extends DefaultExpressionVisitor<Expression> {

		@Override
		public Expression visitCall(CallExpression call) {
			PlannerExpression plannerCall = call.accept(bridgeConverter);
			if (plannerCall instanceof InputTypeSpec) {
				List<TypeInformation<?>> expectedTypes = toJava(((InputTypeSpec) plannerCall).expectedTypes());
				List<PlannerExpression> args = call.getChildren()
					.stream()
					.map(expr -> expr.accept(bridgeConverter)).collect(Collectors.toList());

				List<Expression> newArgs = new ArrayList<>();

				for (int i = 0; i < args.size(); i++) {
					PlannerExpression childExpression = args.get(i);
					TypeInformation<?> expectedType = expectedTypes.get(i);
					newArgs.add(castIfNeeded(childExpression, expectedType));
				}

				return new CallExpression(call.getFunctionDefinition(), newArgs);
			} else {
				return call;
			}
		}

		private Expression castIfNeeded(
				PlannerExpression childExpression,
				TypeInformation<?> expectedType) {
			TypeInformation<?> actualType = childExpression.resultType();
			if (!actualType.equals(expectedType) && TypeCoercion.canSafelyCast(actualType, expectedType)) {
				return new CallExpression(
					BuiltInFunctionDefinitions.CAST,
					asList(childExpression, typeLiteral(expectedType))
				);
			} else {
				return childExpression;
			}
		}

		@Override
		protected Expression defaultMethod(Expression expression) {
			return expression;
		}
	}
}
