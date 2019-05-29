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
import org.apache.flink.table.api.QueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;

import java.util.List;

/**
 * Base class for representing the operation structure behind a user-facing {@link Table} API.
 *
 * A tree of {@link TableOperation} represents a runnable query that can be transformed into a graph
 * of {@link org.apache.flink.streaming.api.transformations.StreamTransformation} via
 * {@link org.apache.flink.table.planner.Planner#translate(List, QueryConfig)}
 */
@Internal
public interface TableOperation extends Operation {

	/**
	 * Resolved schema of this operation. In case of {@link DmlTableOperation} it describes the expected schema.
	 */
	TableSchema getTableSchema();

	List<TableOperation> getChildren();

	<T> T accept(TableOperationVisitor<T> visitor);
}
