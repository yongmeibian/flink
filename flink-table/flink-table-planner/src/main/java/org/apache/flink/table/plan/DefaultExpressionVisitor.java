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

import org.apache.flink.table.expressions.ApiExpressionVisitor;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.SymbolExpression;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedFieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;

public abstract class DefaultExpressionVisitor<ResultT> extends ApiExpressionVisitor<ResultT> {
	@Override
	public ResultT visitCall(CallExpression call) {
		return defaultMethod(call);
	}

	@Override
	public ResultT visitSymbol(SymbolExpression symbolExpression) {
		return defaultMethod(symbolExpression);
	}

	@Override
	public ResultT visitValueLiteral(ValueLiteralExpression valueLiteralExpression) {
		return defaultMethod(valueLiteralExpression);
	}

	@Override
	public ResultT visitFieldReference(FieldReferenceExpression fieldReference) {
		return defaultMethod(fieldReference);
	}

	@Override
	public ResultT visitUnresolvedFieldReference(UnresolvedFieldReferenceExpression fieldReference) {
		return defaultMethod(fieldReference);
	}

	@Override
	public ResultT visitTypeLiteral(TypeLiteralExpression typeLiteral) {
		return defaultMethod(typeLiteral);
	}

	@Override
	public ResultT visitTableReference(TableReferenceExpression tableReference) {
		return defaultMethod(tableReference);
	}

	@Override
	public ResultT visitUnresolvedCall(UnresolvedCallExpression unresolvedCall) {
		return defaultMethod(unresolvedCall);
	}

	@Override
	public ResultT visitNonApiExpression(Expression other) {
		return defaultMethod(other);
	}

	protected abstract ResultT defaultMethod(Expression expression);
}
