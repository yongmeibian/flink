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

package org.apache.flink.table.plan.schema;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.operations.TableOperation;
import org.apache.flink.table.plan.TableOperationConverter;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

public class TableOperationCalciteTable extends AbstractTable implements TranslatableTable {

	private final TableOperation tableOperation;

	public TableOperationCalciteTable(TableOperation tableOperation) {
		this.tableOperation = tableOperation;
	}

	@Override
	public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
		Context clusterContext = context.getCluster()
			.getPlanner()
			.getContext();

		TableOperationConverter tableOperationConverter = clusterContext
			.unwrap(TableOperationConverter.ToRelConverterSupplier.class)
			.get(bridge -> new FlinkRelBuilder(
				clusterContext,
				context.getCluster(),
				relOptTable.getRelOptSchema(),
				bridge));

		return tableOperation.accept(tableOperationConverter);
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		TableSchema tableSchema = tableOperation.getTableSchema();
		return ((FlinkTypeFactory) typeFactory).buildLogicalRowType(
			tableSchema.getFieldNames(),
			tableSchema.getFieldTypes());
	}
}
