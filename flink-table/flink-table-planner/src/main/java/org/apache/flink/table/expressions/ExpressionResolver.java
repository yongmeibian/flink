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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.plan.logical.LogicalNode;
import org.apache.flink.table.validate.FunctionCatalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tries to resolve all unresolved expressions such as {@link UnresolvedFieldReferenceExpression},
 * {@link UnresolvedCallExpression} or calls such as {@link BuiltInFunctionDefinitions#OVER}.
 */
public class ExpressionResolver {

	private final FunctionCatalog functionCatalog;

	public ExpressionResolver(FunctionCatalog functionCatalog) {
		this.functionCatalog = functionCatalog;
	}

	public Expression resolveExpression(Expression unresolved, LogicalNode... inputs) {
		ExpressionResolverVisitor resolverVisitor = new ExpressionResolverVisitor(inputs);
		return unresolved.accept(resolverVisitor);
	}

	class ExpressionResolverVisitor extends ApiExpressionVisitor<Expression> {

		private final Map<String, FieldReferenceExpression> fieldReferences;

		ExpressionResolverVisitor(LogicalNode[] inputs) {
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

		@Override
		public Expression visitUnresolvedCall(UnresolvedCallExpression unresolvedCall) {
			FunctionDefinition resolvedDefinition = functionCatalog.lookupFunction(unresolvedCall.getUnresolvedName());
			return new CallExpression(
				resolvedDefinition,
				unresolvedCall.getChildren().stream().map(expr -> expr.accept(this)).collect(Collectors.toList()));
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
		public Expression visitSymbol(SymbolExpression symbolExpression) {
			return symbolExpression;
		}

		@Override
		public Expression visitValueLiteral(ValueLiteralExpression valueLiteralExpression) {
			return valueLiteralExpression;
		}

		@Override
		public Expression visitFieldReference(FieldReferenceExpression fieldReference) {
			return fieldReference;
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
		public Expression visitTypeLiteral(TypeLiteralExpression typeLiteral) {
			return typeLiteral;
		}

		@Override
		public Expression visitTableReference(TableReferenceExpression tableReference) {
			return tableReference;
		}

		@Override
		public Expression visitNonApiExpression(Expression other) {
			return other;
		}
	}
}
