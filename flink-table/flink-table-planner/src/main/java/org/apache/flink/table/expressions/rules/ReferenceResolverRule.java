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

package org.apache.flink.table.expressions.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Resolves {@link UnresolvedReferenceExpression} to either
 * {@link org.apache.flink.table.expressions.FieldReferenceExpression} or
 * {@link org.apache.flink.table.expressions.TableReferenceExpression} in this order.
 */
@Internal
final class ReferenceResolverRule implements ResolverRule {

	@Override
	public List<Expression> apply(List<Expression> expression, ResolutionContext context) {
		return expression.stream()
			.map(expr -> expr.accept(new ExpressionResolverVisitor(context)))
			.collect(Collectors.toList());
	}

	private class ExpressionResolverVisitor extends ApiExpressionDefaultVisitor<Expression> {

		private final ResolutionContext resolutionContext;

		ExpressionResolverVisitor(ResolutionContext resolutionContext) {
			this.resolutionContext = resolutionContext;
		}

		@Override
		public Expression visitCall(CallExpression call) {
			boolean argsChanged = false;
			List<Expression> resolvedArgs = new ArrayList<>();

			List<Expression> callArgs = call.getChildren();

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

		@Override
		public Expression visitUnresolvedReference(UnresolvedReferenceExpression fieldReference) {
			return resolutionContext.referenceLookup().lookupField(fieldReference.getName())
				.map(expr -> (Expression) expr)
				.orElseGet(() ->
					resolutionContext.tableLookup().lookupTable(fieldReference.getName())
						.map(expr -> (Expression) expr)
						.orElseGet(() -> resolutionContext.getLocalReference(fieldReference.getName())
							.orElseThrow(() -> failForField(fieldReference)
							)));
		}

		private ValidationException failForField(UnresolvedReferenceExpression fieldReference) {
			return new ValidationException(format("Cannot resolve field [%s]", fieldReference.getName()));
		}

		@Override
		protected Expression defaultMethod(Expression expression) {
			return expression;
		}
	}
}
