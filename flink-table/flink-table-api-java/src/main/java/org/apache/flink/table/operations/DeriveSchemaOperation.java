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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.AbstractID;

import java.io.IOException;
import java.util.List;

public class DeriveSchemaOperation implements ModifyOperation {

	private final QueryOperation query;
	private final AggregateFunction<TableSchema, ?> mergeFunction;
	private final String id;

	public DeriveSchemaOperation(
			QueryOperation query,
			AggregateFunction<TableSchema, ?> mergeFunction) {
		this.query = query;
		this.mergeFunction = mergeFunction;
		this.id = new AbstractID().toString();
	}

	public QueryOperation getQuery() {
		return query;
	}

	public String getId() {
		return id;
	}

	public AggregateFunction<TableSchema, ?> getMergeFunction() {
		return mergeFunction;
	}

	@Override
	public QueryOperation getChild() {
		return query;
	}

	@Override
	public <T> T accept(ModifyOperationVisitor<T> visitor) {
		return visitor.visit(this);
	}

	@Override
	public String asSummaryString() {
		return null;
	}

	public TableSchema collectResult(JobExecutionResult result) throws IOException {
		List<byte[]> results = result.getAccumulatorResult(id);
		TypeSerializer<Row> serializer = TableSchema.builder()
			.field("f0", new GenericTypeInfo<>(TableSchema.class))
			.build()
			.toRowType()
			.createSerializer(new ExecutionConfig());

		return (TableSchema) serializer
			.deserialize(new DataInputDeserializer(results.get(results.size() - 1)))
			.getField(0);
	}
}
