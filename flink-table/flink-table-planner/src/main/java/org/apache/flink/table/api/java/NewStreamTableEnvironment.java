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

package org.apache.flink.table.api.java;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.api.NewUnifiedStreamTableEnvironmentImpl;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.operations.DataStreamQueryOperation;
import org.apache.flink.table.operations.OutputConversionModifyOperation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.typeutils.FieldInfoUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Internal
public class NewStreamTableEnvironment extends NewUnifiedStreamTableEnvironmentImpl implements StreamTableEnvironment {
	private NewStreamTableEnvironment(
				CatalogManager catalogManager,
				TableConfig tableConfig,
				StreamExecutionEnvironment executionEnvironment) {
		super(catalogManager, tableConfig, executionEnvironment);
	}

	@Override
	public <T> void registerFunction(String name, TableFunction<T> tableFunction) {
		TypeInformation<T> typeInfo = TypeExtractor.createTypeInfo(
			tableFunction,
			TableFunction.class,
			tableFunction.getClass(),
			0);

		functionCatalog.registerTableFunction(
			name,
			tableFunction,
			typeInfo,
			typeFactory
		);
	}

	@Override
	public <T, ACC> void registerFunction(String name, AggregateFunction<T, ACC> aggregateFunction) {
		TypeInformation<T> typeInfo = TypeExtractor.createTypeInfo(
			aggregateFunction,
			AggregateFunction.class,
			aggregateFunction.getClass(),
			0);

		TypeInformation<T> accTypeInfo = TypeExtractor.createTypeInfo(
			aggregateFunction,
			AggregateFunction.class,
			aggregateFunction.getClass(),
			1);

		functionCatalog.registerAggregateFunction(
			name,
			aggregateFunction,
			typeInfo,
			accTypeInfo,
			typeFactory
		);
	}

	@Override
	public <T, ACC> void registerFunction(String name, TableAggregateFunction<T, ACC> tableAggregateFunction) {

	}

	@Override
	public <T> Table fromDataStream(DataStream<T> dataStream) {
		DataStreamQueryOperation<T> queryOperation = asQueryOperation(dataStream, Optional.empty());

		return createTable(queryOperation);
	}

	@Override
	public <T> Table fromDataStream(DataStream<T> dataStream, String fields) {
		List<Expression> expressions = ExpressionParser.parseExpressionList(fields);
		DataStreamQueryOperation<T> queryOperation = asQueryOperation(dataStream, Optional.of(expressions));

		return createTable(queryOperation);
	}

	@Override
	public <T> void registerDataStream(String name, DataStream<T> dataStream) {
		registerTable(name, fromDataStream(dataStream));
	}

	@Override
	public <T> void registerDataStream(String name, DataStream<T> dataStream, String fields) {
		registerTable(name, fromDataStream(dataStream, fields));
	}

	@Override
	public <T> DataStream<T> toAppendStream(Table table, Class<T> clazz) {
		return toAppendStream(table, clazz, new StreamQueryConfig());
	}

	@Override
	public <T> DataStream<T> toAppendStream(Table table, TypeInformation<T> typeInfo) {
		return toAppendStream(table, typeInfo, new StreamQueryConfig());
	}

	@Override
	public <T> DataStream<T> toAppendStream(
			Table table,
			Class<T> clazz,
			StreamQueryConfig queryConfig) {
		TypeInformation<T> typeInfo = TypeExtractor.createTypeInfo(clazz);
		return toAppendStream(table, typeInfo, queryConfig);
	}

	@Override
	public <T> DataStream<T> toAppendStream(
			Table table,
			TypeInformation<T> typeInfo,
			StreamQueryConfig queryConfig) {
		OutputConversionModifyOperation modifyOperation = new OutputConversionModifyOperation(
			table.getQueryOperation(),
			TypeConversions.fromLegacyInfoToDataType(typeInfo),
			OutputConversionModifyOperation.UpdateMode.APPEND);
		return toDataStream(table, modifyOperation);
	}

	@Override
	public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, Class<T> clazz) {
		return toRetractStream(table, clazz, new StreamQueryConfig());
	}

	@Override
	public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, TypeInformation<T> typeInfo) {
		return toRetractStream(table, typeInfo, new StreamQueryConfig());
	}

	@Override
	public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(
			Table table,
			Class<T> clazz,
			StreamQueryConfig queryConfig) {
		TypeInformation<T> typeInfo = TypeExtractor.createTypeInfo(clazz);
		return toRetractStream(table, typeInfo, queryConfig);
	}

	@Override
	public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(
			Table table,
			TypeInformation<T> typeInfo,
			StreamQueryConfig queryConfig) {
		OutputConversionModifyOperation modifyOperation = new OutputConversionModifyOperation(
			table.getQueryOperation(),
			wrapWithChangeFlag(typeInfo),
			OutputConversionModifyOperation.UpdateMode.RETRACT);
		return toDataStream(table, modifyOperation);
	}

	public StreamTableDescriptor connect(ConnectorDescriptor connectorDescriptor) {
		return (StreamTableDescriptor) super.connect(connectorDescriptor);
	}

	private <T> DataStreamQueryOperation<T> asQueryOperation(
			DataStream<T> dataStream,
			Optional<List<Expression>> fields) {
		TypeInformation<T> streamType = dataStream.getType();

		// get field names and types for all non-replaced fields
		FieldInfoUtils.TypeInfoSchema typeInfoSchema = fields.map(f -> {
			FieldInfoUtils.TypeInfoSchema fieldsInfo = FieldInfoUtils.getFieldsInfo(
				streamType,
				f.toArray(new Expression[0]));

			// check if event-time is enabled
			if (fieldsInfo.isRowtimeDefined() &&
				execEnv.getStreamTimeCharacteristic() != TimeCharacteristic.EventTime) {
				throw new ValidationException(String.format(
					"A rowtime attribute requires an EventTime time characteristic in stream environment. But is: %s",
					execEnv.getStreamTimeCharacteristic()));
			}
			return fieldsInfo;
		}).orElseGet(() -> FieldInfoUtils.getFieldsInfo(streamType));

		return new DataStreamQueryOperation<>(
			dataStream,
			typeInfoSchema.getIndices(),
			typeInfoSchema.toTableSchema());
	}

	private <T> DataStream<T> toDataStream(Table table, OutputConversionModifyOperation modifyOperation) {
		List<StreamTransformation<?>> transformations = planner.translate(Collections.singletonList(modifyOperation));

		StreamTransformation<T> streamTransformation = getStreamTransformation(table, transformations);

		execEnv.addOperator(streamTransformation);
		return new DataStream<>(execEnv, streamTransformation);
	}

	@SuppressWarnings("unchecked")
	private <T> StreamTransformation<T> getStreamTransformation(
		Table table,
		List<StreamTransformation<?>> transformations) {
		if (transformations.size() != 1) {
			throw new TableException(String.format(
				"Expected a single transformation for query: %s\n Got: %s",
				table.getQueryOperation().asSummaryString(),
				transformations));
		}

		return (StreamTransformation<T>) transformations.get(0);
	}

	private <T> DataType wrapWithChangeFlag(TypeInformation<T> outputType) {
		TupleTypeInfo tupleTypeInfo = new TupleTypeInfo<Tuple2<Boolean, T>>(Types.BOOLEAN(), outputType);
		return TypeConversions.fromLegacyInfoToDataType(tupleTypeInfo);
	}
}
