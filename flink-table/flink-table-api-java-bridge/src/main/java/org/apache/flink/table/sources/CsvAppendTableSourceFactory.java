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

package org.apache.flink.table.sources;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.FileSystemValidator;
import org.apache.flink.table.factories.SchemaDerivation;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.utils.LogicalTypeGeneralization;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;

/**
 * Factory for creating configured instances of {@link CsvTableSource} in a stream environment.
 */
@PublicEvolving
public class CsvAppendTableSourceFactory extends CsvTableSourceFactoryBase implements StreamTableSourceFactory<Row> {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>(super.requiredContext());
		context.put(UPDATE_MODE, UPDATE_MODE_VALUE_APPEND);
		return context;
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		return createTableSource(true, properties);
	}

	@Override
	public Optional<SchemaDerivation> deriveSchema(Map<String, String> properties) {
		String path = properties.get(FileSystemValidator.CONNECTOR_PATH);
		return Optional.of(
			new SchemaDerivation() {
				@Override
				public TableSource<?> getTableSource() {
					return new InputFormatTableSource<String>() {
						@Override
						public InputFormat<String, ?> getInputFormat() {
							return new TextInputFormat(new Path(path));
						}

						@Override
						public DataType getProducedDataType() {
							return DataTypes.STRING();
						}

						@Override
						public TableSchema getTableSchema() {
							return TableSchema.builder()
								.field("f0", DataTypes.STRING())
								.build();
						}
					};
				}

				@Override
				public AggregateFunction<TableSchema, ?> getAccumulator() {
					return new MergeCsvSchema();
				}
			}
		);
	}

	public static class MergeCsvSchema extends AggregateFunction<TableSchema, Tuple2<TableSchema, Boolean>> {

		@Override
		public TableSchema getValue(Tuple2<TableSchema, Boolean> accumulator) {
			return accumulator.f0;
		}

		@Override
		public Tuple2<TableSchema, Boolean> createAccumulator() {
			return Tuple2.of(
				TableSchema.builder().build(),
				false
			);
		}

		public void accumulate(Tuple2<TableSchema, Boolean> accumulator, String row) {
			TableSchema inferredSchema = inferSchema(row);
			int commonLength = Math.min(accumulator.f0.getFieldCount(), inferredSchema.getFieldCount());
			TableSchema.Builder builder = TableSchema.builder();
			for (int i = 0; i < commonLength; i++) {
				DataType dataType = LogicalTypeGeneralization.findCommonType(Arrays.asList(
					accumulator.f0.getFieldDataType(i).get().getLogicalType(),
					inferredSchema.getFieldDataType(i).get().getLogicalType()
				)).map(TypeConversions::fromLogicalToDataType)
					.orElse(DataTypes.STRING());
				builder.field("f" + i, dataType);
			}

			for (int i = commonLength; i < accumulator.f0.getFieldCount(); i++) {
				builder.field("f" + i, accumulator.f0.getFieldDataType(i).get());
			}

			for (int i = commonLength; i < inferredSchema.getFieldCount(); i++) {
				builder.field("f" + i, inferredSchema.getFieldDataType(i).get());
			}

			accumulator.f0 = builder.build();
		}

		private TableSchema inferSchema(String row) {
			String[] fields = row.split("\t");
			int length = fields.length;
			TableSchema.Builder builder = TableSchema.builder();
			for (int i = 0; i < length; i++) {
				builder.field("f" + i, inferField(fields[i]));
			}

			return builder.build();
		}

		@Override
		public TypeInformation<TableSchema> getResultType() {
			return new GenericTypeInfo<>(TableSchema.class);
		}

		@Override
		public TypeInformation<Tuple2<TableSchema, Boolean>> getAccumulatorType() {
			return new TupleTypeInfo(
				Tuple2.class,
				new GenericTypeInfo<>(TableSchema.class),
				BasicTypeInfo.BOOLEAN_TYPE_INFO
			);
		}

		@Override
		public boolean isDeterministic() {
			return true;
		}

		public DataType inferField(String field) {
			if (isLong(field)) {
				return DataTypes.BIGINT();
			}

			return DataTypes.STRING();
		}

		private boolean isLong(String field) {
			try {
				Long.parseLong(field);
				return true;
			} catch (Exception ex) {
				return false;
			}
		}
	}
}
