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

package org.apache.flink.table.catalog;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ConnectorCatalogTable<T1, T2> implements CatalogTable {
	private final TableSource<T1> tableSource;
	private final TableSink<T2> tableSink;
	private final TableSchema tableSchema;

	public static <T1> ConnectorCatalogTable source(TableSource<T1> source) {
		TableSchema tableSchema = calculateSourceSchema(source);
		return new ConnectorCatalogTable<>(source, null, tableSchema);
	}

	public static <T2> ConnectorCatalogTable sink(TableSink<T2> sink) {
		TableSchema tableSchema = new TableSchema(sink.getFieldNames(), sink.getFieldTypes());
		return new ConnectorCatalogTable<>(null, sink, tableSchema);
	}

	public static <T1, T2> ConnectorCatalogTable sourceAndSink(TableSource<T1> source, TableSink<T2> sink) {
		TableSchema tableSchema = calculateSourceSchema(source);
		return new ConnectorCatalogTable<>(source, sink, tableSchema);
	}

	private ConnectorCatalogTable(
			TableSource<T1> tableSource,
			TableSink<T2> tableSink,
			TableSchema tableSchema) {
		this.tableSource = tableSource;
		this.tableSink = tableSink;
		this.tableSchema = tableSchema;
	}

	public Optional<TableSource<T1>> getTableSource() {
		return Optional.ofNullable(tableSource);
	}

	public Optional<TableSink<T2>> getTableSink() {
		return Optional.ofNullable(tableSink);
	}

	@Override
	public TableStats getStatistics() {
		return null;
	}

	@Override
	public boolean isPartitioned() {
		return false;
	}

	@Override
	public List<String> getPartitionKeys() {
		return Collections.emptyList();
	}

	@Override
	public Map<String, String> getProperties() {
		return Collections.emptyMap();
	}

	@Override
	public TableSchema getSchema() {
		return tableSchema;
	}

	@Override
	public CatalogBaseTable copy() {
		return this;
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.empty();
	}

	@Override
	public Optional<String> getDetailedDescription() {
		return Optional.empty();
	}

	private static <T1> TableSchema calculateSourceSchema(TableSource<T1> source) {
		TableSchema tableSchema = source.getTableSchema();
		TypeInformation[] types = Arrays.copyOf(tableSchema.getFieldTypes(), tableSchema.getFieldCount());
		String[] fieldNames = tableSchema.getFieldNames();
		if (source instanceof DefinedRowtimeAttributes) {
			List<String> rowtimeAttributes = ((DefinedRowtimeAttributes) source).getRowtimeAttributeDescriptors()
				.stream()
				.map(RowtimeAttributeDescriptor::getAttributeName)
				.collect(Collectors.toList());

			for (int i = 0; i < fieldNames.length; i++) {
				if (rowtimeAttributes.contains(fieldNames[i])) {
					types[i] = TimeIndicatorTypeInfo.ROWTIME_INDICATOR;
				}
			}
		}
		if (source instanceof DefinedProctimeAttribute) {
			String proctimeAttribute = ((DefinedProctimeAttribute) source).getProctimeAttribute();

			for (int i = 0; i < fieldNames.length; i++) {
				if (fieldNames[i].equals(proctimeAttribute)) {
					types[i] = TimeIndicatorTypeInfo.PROCTIME_INDICATOR;
					break;
				}
			}
		}
		return new TableSchema(fieldNames, types);
	}
}
