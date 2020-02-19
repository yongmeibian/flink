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

package org.apache.flink.table.expressions.resolver;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.FunctionLookup;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.LookupCallExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.utils.ApiExpressionDefaultVisitor;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Resolves calls with function names to calls with actual function definitions.
 */
@Internal
public class LookupCallResolver extends ApiExpressionDefaultVisitor<Expression> {

	private final FunctionLookup functionLookup;
	private final Function<String, UnresolvedIdentifier> parser;

	public LookupCallResolver(FunctionLookup functionLookup, Function<String, UnresolvedIdentifier> parser) {
		this.functionLookup = functionLookup;
		this.parser = parser;
	}

	public Expression visit(LookupCallExpression lookupCall) {
		UnresolvedIdentifier unresolvedIdentifier = parser.apply(lookupCall.getUnresolvedName());
		final FunctionLookup.Result result = functionLookup.lookupFunction(unresolvedIdentifier)
			.orElseThrow(() -> new ValidationException("Undefined function: " + lookupCall.getUnresolvedName()));

		return new UnresolvedCallExpression(
			result.getFunctionIdentifier(),
			result.getFunctionDefinition(),
			resolveChildren(lookupCall.getChildren()));
	}

	public Expression visit(UnresolvedCallExpression unresolvedCall) {
		return unresolvedCall.replaceArgs(resolveChildren(unresolvedCall.getChildren()));
	}

	private List<Expression> resolveChildren(List<Expression> lookupChildren) {
		return lookupChildren
			.stream()
			.map(child -> child.accept(this))
			.collect(Collectors.toList());
	}

	@Override
	protected Expression defaultMethod(Expression expression) {
		return expression;
	}
}
