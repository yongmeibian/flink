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

package org.apache.flink.table.api;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.UnregisteredSinkModifyOperation;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.utils.ConnectorDescriptorMock;
import org.apache.flink.table.utils.FormatDescriptorMock;
import org.apache.flink.table.utils.PlannerMock;
import org.apache.flink.table.utils.TableEnvironmentMock;
import org.apache.flink.table.utils.TableSourceSinkFactoryMock;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link TableEnvironment}.
 */
public class TableEnvironmentTest {

	@Test
	public void testConnect() throws Exception {
		final TableEnvironmentMock tableEnv = TableEnvironmentMock.getStreamingInstance();

		tableEnv
			.connect(new ConnectorDescriptorMock(TableSourceSinkFactoryMock.CONNECTOR_TYPE_VALUE, 1, true))
			.withFormat(new FormatDescriptorMock("my_format", 1))
			.withSchema(new Schema()
				.field("my_field_0", "INT")
				.field("my_field_1", "BOOLEAN"))
			.inAppendMode()
			.registerTableSource("my_table");

		CatalogManager.TableLookupResult lookupResult = tableEnv.catalogManager.getTable(ObjectIdentifier.of(
			EnvironmentSettings.DEFAULT_BUILTIN_CATALOG,
			EnvironmentSettings.DEFAULT_BUILTIN_DATABASE,
			"my_table"))
			.orElseThrow(AssertionError::new);

		assertThat(lookupResult.isTemporary(), equalTo(true));

		CatalogBaseTable table = lookupResult.getTable();
		assertThat(
			table.getSchema(),
			equalTo(
				TableSchema.builder()
					.field("my_field_0", DataTypes.INT())
					.field("my_field_1", DataTypes.BOOLEAN())
					.build()));

		final ConnectorCatalogTable<?, ?> connectorCatalogTable = (ConnectorCatalogTable<?, ?>) table;

		assertThat(
			connectorCatalogTable.getTableSource().isPresent(),
			equalTo(true));
	}

	@Test
	public void testConnectWithInlineTables() throws Exception {
		TableEnvironmentMock tableEnv = TableEnvironmentMock.mock()
			.setPlanner(new PlannerMock() {
				@Override
				public List<Transformation<?>> translate(List<ModifyOperation> modifyOperations) {
					ModifyOperation modifyOperation = modifyOperations.get(0);
					UnregisteredSinkModifyOperation sinkModifyOperation = (UnregisteredSinkModifyOperation) modifyOperation;

					TableSink sink = sinkModifyOperation.getSink();
					assertThat(sink.getTableSchema(), equalTo(
						TableSchema.builder()
							.field("my_field_1", DataTypes.BOOLEAN())
							.build()));

					QueryOperation query = sinkModifyOperation.getChild();
					assertThat(query.asSummaryString(),
						equalTo(
							"Project: (projections: [my_field_1])\\n    TableSource: (fields: [my_field_0, my_field_1])"));

					return Collections.emptyList();
				}
			}).createStreamingInstance();

		tableEnv
			.connect(new ConnectorDescriptorMock(TableSourceSinkFactoryMock.CONNECTOR_TYPE_VALUE, 1, true))
			.withFormat(new FormatDescriptorMock("my_format", 1))
			.withSchema(new Schema()
				.field("my_field_0", "INT")
				.field("my_field_1", "BOOLEAN"))
			.inAppendMode()
			.read()
			.select(new UnresolvedReferenceExpression("my_field_1"))
			.insertInto(new ConnectorDescriptorMock(TableSourceSinkFactoryMock.CONNECTOR_TYPE_VALUE, 1, true))
			.withFormat(new FormatDescriptorMock("my_format", 1))
			.inAppendMode()
			.write();

		tableEnv.execute("");
	}
}
