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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.plan.logical.LogicalNode;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;

/**
 * Converter from Flink's specific relational representation: {@link TableOperation} to Calcite's specific relational
 * representation: {@link RelNode}.
 */
@Internal
public class TableOperationToRelNodeConverter extends TableOperationDefaultVisitor<RelNode> {

	/**
	 * Supplier for {@link TableOperationToRelNodeConverter} that can wrap given {@link RelBuilder}.
	 */
	@Internal
	public static class ToRelConverterSupplier {
		public TableOperationToRelNodeConverter get(RelBuilder relBuilder) {
			return new TableOperationToRelNodeConverter(relBuilder);
		}
	}

	private final RelBuilder relBuilder;
	private final SingleRelVisitor singleRelVisitor = new SingleRelVisitor();

	public TableOperationToRelNodeConverter(RelBuilder relBuilder) {
		this.relBuilder = relBuilder;
	}

	@Override
	public RelNode defaultMethod(TableOperation other) {
		other.getChildren().forEach(child -> relBuilder.push(child.accept(this)));
		return other.accept(singleRelVisitor);
	}

	private class SingleRelVisitor implements TableOperationVisitor<RelNode> {

		@Override
		public RelNode visitAlgebraicOperation(AlgebraicTableOperation algebraicOperation) {
			switch (algebraicOperation.getType()) {
				case INTERSECT:
					relBuilder.intersect(algebraicOperation.isAll());
					break;
				case MINUS:
					relBuilder.minus(algebraicOperation.isAll());
					break;
				case UNION:
					relBuilder.union(algebraicOperation.isAll());
					break;
			}
			return relBuilder.build();
		}

		@Override
		public RelNode visitOther(TableOperation other) {
			if (other instanceof LogicalNode) {
				return ((LogicalNode) other).toRelNode(relBuilder);
			}

			throw new TableException("Unknown table operation: " + other);
		}

	}
}
