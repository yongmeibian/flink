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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.Expression;

import javax.annotation.Nullable;

import java.lang.reflect.Array;
import java.util.Optional;

public final class JoinOperation extends BinaryOperation {

	private final JoinType joinType;

	private final Expression condition;

	private final boolean correlated;

	private final TableSchema joinSchema;

	public JoinOperation(
			Operation left,
			Operation right,
			JoinType joinType,
			@Nullable Expression condition,
			boolean correlated) {
		super(left, right);
		this.joinType = joinType;
		this.condition = condition;
		this.correlated = correlated;
		this.joinSchema = calculateJoinSchema(left, right);
	}

	private TableSchema calculateJoinSchema(Operation left, Operation right) {
		TableSchema leftSchema = left.getSchema();
		TableSchema rightSchema = right.getSchema();

		String[] mergedNames = concatenate(leftSchema.getFieldNames(), rightSchema.getFieldNames());
		TypeInformation<?>[] mergedTypes = concatenate(leftSchema.getFieldTypes(), rightSchema.getFieldTypes());

		return new TableSchema(mergedNames, mergedTypes);
	}

	private <T> T[] concatenate(T[] a, T[] b) {
		int aLen = a.length;
		int bLen = b.length;

		@SuppressWarnings("unchecked")
		T[] c = (T[]) Array.newInstance(a.getClass().getComponentType(), aLen + bLen);
		System.arraycopy(a, 0, c, 0, aLen);
		System.arraycopy(b, 0, c, aLen, bLen);

		return c;
	}

	public JoinType getJoinType() {
		return joinType;
	}

	public Optional<Expression> getCondition() {
		return Optional.ofNullable(condition);
	}

	public boolean isCorrelated() {
		return correlated;
	}

	@Override
	public TableSchema getSchema() {
		return joinSchema;
	}

	@Override
	public <R> R accept(OperationTreeVisitor<R> visitor) {
		return visitor.visitJoinOperation(this);
	}
}
