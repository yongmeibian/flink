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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.TABLE_SCHEMA_EXPR;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_DATA_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

public final class CollectTableSinkFactory implements TableSinkFactory<Tuple2<Boolean, Row>> {

	public static final String CONNECTOR_TYPE_VALUE = "collect";
	public static final String CONNECTOR_ID_KEY = "connector.accumulator-id";

	@Override
	public TableSink<Tuple2<Boolean, Row>> createTableSink(Map<String, String> properties) {
		DescriptorProperties params = new DescriptorProperties();
		params.putProperties(properties);

		TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(params.getTableSchema(SCHEMA));
		String accumulatorId = params.getString(CONNECTOR_ID_KEY);
		return new CollectTableSink(
			tableSchema,
			accumulatorId
		);
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE);
		context.put(CONNECTOR_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		return Arrays.asList(
			CONNECTOR_ID_KEY,

			// schema
			SCHEMA + ".#." + SCHEMA_TYPE,
			SCHEMA + ".#." + SCHEMA_DATA_TYPE,
			SCHEMA + ".#." + SCHEMA_NAME,
			SCHEMA + ".#." + SCHEMA_FROM,

			// time attributes
			SCHEMA + ".#." + SCHEMA_PROCTIME,
			SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE,
			SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM,
			SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS,
			SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED,
			SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE,
			SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS,
			SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED,
			SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY,

			// watermark
			SCHEMA + "." + WATERMARK + ".#." + WATERMARK_ROWTIME,
			SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_EXPR,
			SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_DATA_TYPE,

			// computed column
			SCHEMA + ".#." + TABLE_SCHEMA_EXPR
		);
	}

	private static class CollectTableSink implements RetractStreamTableSink<Row> {
		private final TableSchema schema;
		private final String accumulatorId;

		CollectTableSink(
				TableSchema schema,
				String accumulatorId) {
			this.schema = schema;
			this.accumulatorId = accumulatorId;
		}

		@Override
		public TableSchema getTableSchema() {
			return schema;
		}

		@Override
		public TableSink<Tuple2<Boolean, Row>> configure(
				String[] fieldNames,
				TypeInformation<?>[] fieldTypes) {
			throw new UnsupportedOperationException();
		}

		@Override
		public TypeInformation<Row> getRecordType() {
			return schema.toRowType();
		}

		@Override
		public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
			throw new UnsupportedOperationException("");
		}

		@Override
		public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
			TypeSerializer<Row> serializer = schema.toRowType().createSerializer(dataStream.getExecutionConfig());
			Utils.CollectHelper<Row> outputFormat = new Utils.CollectHelper<>(accumulatorId, serializer);
			return dataStream.map(row -> row.f1)
				.writeUsingOutputFormat(outputFormat)
				.setParallelism(1)
				.name("tableResult");
		}
	}

//	public static class CollectHelper<T> extends RichOutputFormat<T> {
//
//		private static final long serialVersionUID = 1L;
//
//		private final String id;
//		private final TypeSerializer<T> serializer;
//
//		private SerializedListAccumulator<T> accumulator;
//
//		public CollectHelper(String id, TypeSerializer<T> serializer) {
//			this.id = id;
//			this.serializer = serializer;
//		}
//
//		@Override
//		public void configure(Configuration parameters) {}
//
//		@Override
//		public void open(int taskNumber, int numTasks)  {
//			this.accumulator = new SerializedListAccumulator<>();
//		}
//
//		@Override
//		public void writeRecord(T record) throws IOException {
//			accumulator.add(record, serializer);
//		}
//
//		@Override
//		public void close() {
//			// Important: should only be added in close method to minimize traffic of accumulators
//			getRuntimeContext().addAccumulator(id, accumulator);
//		}
//	}
}
