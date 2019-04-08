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

package org.apache.flink.table.planner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.QueryConfig;
import org.apache.flink.table.operations.TableOperation;
import org.apache.flink.table.sinks.TableSink;

@Internal
public interface Planner {

	/**
	 * Evaluates a SQL query on registered tables and retrieves the result as a {@link TableOperation}.
	 *
	 * <p>All tables referenced by the query must be registered in the TableEnvironment.
	 * A [[TableOperation]] is automatically registered when its [[toString]] method is called, for example
	 * when it is embedded into a String.
	 * Hence, SQL queries can directly reference a [[TableOperation]] as follows:
	 *
	 * <pre>{@code
	 *  Table table = ...
	 *  // the table is not registered to the table environment
	 *  tEnv.sqlQuery(s"SELECT * FROM $table");
	 * }
	 * </pre>
	 *
	 * @param query The SQL query to evaluate.
	 * @return The result of the query as Table
	 */
	TableOperation sqlQuery(String query);

	/**
	 * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
	 * NOTE: Currently only SQL INSERT statements are supported.
	 *
	 * <p>All tables referenced by the query must be registered in the TableEnvironment.
	 * A [[TableOperation]] is automatically registered when its [[toString]] method is called, for example
	 * when it is embedded into a String.
	 * Hence, SQL queries can directly reference a [[TableOperation]] as follows:
	 *
	 * <pre>{@code
	 *   // register the table sink into which the result is inserted.
	 *   tEnv.registerTableSink("sinkTable", fieldNames, fieldsTypes, tableSink)
	 *   val sourceTable: Table = ...
	 *   // sourceTable is not registered to the table environment
	 *   tEnv.sqlUpdate(s"INSERT INTO sinkTable SELECT * FROM $sourceTable")
	 * }</pre>
	 *
	 * @param statement The SQL statement to evaluate.
	 */
	void sqlUpdate(String statement, QueryConfig queryConfig);

	/**
	 * Writes a [[TableOperation]] to a [[TableSink]].
	 *
	 * @param operation The [[TableOperation]] to write.
	 * @param sink The [[TableSink]] to write the [[TableOperation]] to.
	 * @param queryConfig The [[QueryConfig]] to use.
	 * @param <T> The data type that the [[TableSink]] expects.
	 */
	<T> void writeToSink(TableOperation operation, TableSink<T> sink, QueryConfig queryConfig);

	/**
	 * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
	 * the result of the given [[TableOperation]].
	 *
	 * @param tableOperation The table for which the AST and execution plan will be returned.
	 */
	String explain(TableOperation tableOperation, QueryConfig queryConfig);

	/**
	 * Returns completion hints for the given statement at the given cursor position.
	 * The completion happens case insensitively.
	 *
	 * @param statement Partial or slightly incorrect SQL statement
	 * @param position cursor position
	 * @return completion hints that fit at the current cursor position
	 */
	String[] getCompletionHints(String statement, int position);
}
