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

import org.apache.flink.table.api.TableSchema;

public final class AlgebraicOperation extends BinaryOperation {

	public enum AlgebraicOperationType {
		UNION,
		MINUS,
		INTERSECT
	}

	private final boolean all;
	private final AlgebraicOperationType algebraicOperationType;

	public AlgebraicOperation(
			Operation left,
			Operation right,
			boolean all,
			AlgebraicOperationType algebraicOperationType) {
		super(left, right);
		this.all = all;
		this.algebraicOperationType = algebraicOperationType;
	}

	public boolean isAll() {
		return all;
	}

	public AlgebraicOperationType getAlgebraicOperationType() {
		return algebraicOperationType;
	}

	@Override
	public TableSchema getSchema() {
		return getLeft().getSchema();
	}

	@Override
	public <R> R accept(OperationTreeVisitor<R> visitor) {
		return visitor.visitAlgebraicOperation(this);
	}
}
