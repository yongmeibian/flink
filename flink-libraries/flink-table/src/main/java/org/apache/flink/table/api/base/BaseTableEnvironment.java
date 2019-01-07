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

package org.apache.flink.table.api.base;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.QueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.TableDescriptor;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.utils.TableSqlFunction;
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.validate.FunctionCatalog;

import org.apache.calcite.sql.SqlFunction;

import static org.apache.flink.util.InstantiationUtil.checkForInstantiation;

public abstract class BaseTableEnvironment {

	// Table API/SQL function catalog
	private final FunctionCatalog functionCatalog = FunctionCatalog.withBuiltIns();

	/**
	 * Registers a [[ScalarFunction]] under a unique name. Replaces already existing
	 * user-defined functions under this name.
	 */
	public void registerFunction(String name, ScalarFunction function) {
		// check if class could be instantiated
		checkForInstantiation(function.getClass());

		// register in Table API

		functionCatalog.registerFunction(name, function.getClass());

		// register in SQL API
		functionCatalog.registerSqlFunction(
			UserDefinedFunctionUtils.createScalarSqlFunction(name, name, function, typeFactory)
		);
	}

	/**
	 * Registers a [[TableFunction]] under a unique name. Replaces already existing
	 * user-defined functions under this name.
	 */
	protected <T> void registerTableFunctionInternal(
			String name,
			TableFunction<T> function,
			TypeInformation<T> typeInfo) {
		// check if class not Scala object
		UserDefinedFunctionUtils.checkNotSingleton(function.getClass());
		// check if class could be instantiated
		checkForInstantiation(function.getClass());

		final TypeInformation<T> derivedTypeInfo;
		if (function.getResultType() != null) {
			derivedTypeInfo = function.getResultType();
		} else {
			derivedTypeInfo = typeInfo;
		}

		// register in Table API
		functionCatalog.registerFunction(name, function.getClass());

		// register in SQL API
		TableSqlFunction sqlFunction = UserDefinedFunctionUtils.createTableSqlFunction(name,
			name,
			function,
			derivedTypeInfo,
			typeFactory);
		functionCatalog.registerSqlFunction(sqlFunction);
	}

	/**
	 * Registers an [[AggregateFunction]] under a unique name. Replaces already existing
	 * user-defined functions under this name.
	 */
	protected <T, ACC> void registerAggregateFunctionInternal(
			String name,
			AggregateFunction<T, ACC> function,
			TypeInformation<T> typeInfo,
			TypeInformation<ACC> accTypeInfo) {
		// check if class not Scala object
		UserDefinedFunctionUtils.checkNotSingleton(function.getClass());
		// check if class could be instantiated
		checkForInstantiation(function.getClass());

		TypeInformation<?> resultTypeInfo = UserDefinedFunctionUtils.getResultTypeOfAggregateFunction(
			function,
			typeInfo);

		TypeInformation<?> derivedAccTypeInfo = UserDefinedFunctionUtils.getAccumulatorTypeOfAggregateFunction(
			function,
			accTypeInfo);

		// register in Table API
		functionCatalog.registerFunction(name, function.getClass());

		// register in SQL API
		SqlFunction sqlFunctions = UserDefinedFunctionUtils.createAggregateSqlFunction(
			name,
			name,
			function,
			resultTypeInfo,
			derivedAccTypeInfo,
			typeFactory);

		functionCatalog.registerSqlFunction(sqlFunctions);
	}

	/**
	 * Creates a table from a table source.
	 *
	 * @param source table source used as table
	 */
	public abstract Table fromTableSource(TableSource<?> source);

	/**
	 * Registers an [[ExternalCatalog]] under a unique name in the TableEnvironment's schema.
	 * All tables registered in the [[ExternalCatalog]] can be accessed.
	 *
	 * @param name            The name under which the externalCatalog will be registered
	 * @param externalCatalog The externalCatalog to register
	 */
	public abstract void registerExternalCatalog(String name, ExternalCatalog externalCatalog);

	/**
	 * Gets a registered [[ExternalCatalog]] by name.
	 *
	 * @param name The name to look up the [[ExternalCatalog]]
	 * @return The [[ExternalCatalog]]
	 */
	public abstract ExternalCatalog getRegisteredExternalCatalog(String name);

	/**
	 * Registers a [[Table]] under a unique name in the TableEnvironment's catalog.
	 * Registered tables can be referenced in SQL queries.
	 *
	 * @param name The name under which the table will be registered.
	 * @param table The table to register.
	 */
	public abstract void registerTable(String name, Table table);

	/**
	 * Registers an external [[TableSource]] in this [[TableEnvironment]]'s catalog.
	 * Registered tables can be referenced in SQL queries.
	 *
	 * @param name        The name under which the [[TableSource]] is registered.
	 * @param tableSource The [[TableSource]] to register.
	 */
	public abstract void registerTableSource(String name, TableSource<?> tableSource);

	/**
	 * Registers an external [[TableSink]] with given field names and types in this
	 * [[TableEnvironment]]'s catalog.
	 * Registered sink tables can be referenced in SQL DML statements.
	 *
	 * @param name The name under which the [[TableSink]] is registered.
	 * @param fieldNames The field names to register with the [[TableSink]].
	 * @param fieldTypes The field types to register with the [[TableSink]].
	 * @param tableSink The [[TableSink]] to register.
	 */
	public abstract void registerTableSink(
		String name,
		String[] fieldNames,
		TypeInformation<?>[] fieldTypes,
		TableSink<?> tableSink);

	/**
	 * Registers an external [[TableSink]] with already configured field names and field types in
	 * this [[TableEnvironment]]'s catalog.
	 * Registered sink tables can be referenced in SQL DML statements.
	 *
	 * @param name The name under which the [[TableSink]] is registered.
	 * @param configuredSink The configured [[TableSink]] to register.
	 */
	public abstract void registerTableSink(String name, TableSink<?> configuredSink);

	/**
	 * Scans a registered table and returns the resulting [[Table]].
	 *
	 * A table to scan must be registered in the TableEnvironment. It can be either directly
	 * registered as DataStream, DataSet, or Table or as member of an [[ExternalCatalog]].
	 *
	 * Examples:
	 *
	 * - Scanning a directly registered table
	 * {{{
	 *   val tab: Table = tableEnv.scan("tableName")
	 * }}}
	 *
	 * - Scanning a table from a registered catalog
	 * {{{
	 *   val tab: Table = tableEnv.scan("catalogName", "dbName", "tableName")
	 * }}}
	 *
	 * @param tablePath The path of the table to scan.
	 * @throws TableException if no table is found using the given table path.
	 * @return The resulting [[Table]].
	 */
	public abstract Table scan(String... tablePath);

	/**
	 * Creates a table source and/or table sink from a descriptor.
	 *
	 * Descriptors allow for declaring the communication to external systems in an
	 * implementation-agnostic way. The classpath is scanned for suitable table factories that match
	 * the desired configuration.
	 *
	 * The following example shows how to read from a connector using a JSON format and
	 * registering a table source as "MyTable":
	 *
	 * {{{
	 *
	 * tableEnv
	 *   .connect(
	 *     new ExternalSystemXYZ()
	 *       .version("0.11"))
	 *   .withFormat(
	 *     new Json()
	 *       .jsonSchema("{...}")
	 *       .failOnMissingField(false))
	 *   .withSchema(
	 *     new Schema()
	 *       .field("user-name", "VARCHAR").from("u_name")
	 *       .field("count", "DECIMAL")
	 *   .registerSource("MyTable")
	 * }}}
	 *
	 * @param connectorDescriptor connector descriptor describing the external system
	 */
	public abstract TableDescriptor connect(ConnectorDescriptor connectorDescriptor);

	/**
	 * Gets the names of all tables registered in this environment.
	 *
	 * @return A list of the names of all registered tables.
	 */
	public abstract String[] listTables();

	/**
	 * Gets the names of all functions registered in this environment.
	 */
	public abstract String[] listUserDefinedFunctions();

	/**
	 * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
	 * the result of the given [[Table]].
	 *
	 * @param table The table for which the AST and execution plan will be returned.
	 */
	public abstract String explain(Table table);

	/**
	 * Returns completion hints for the given statement at the given cursor position.
	 * The completion happens case insensitively.
	 *
	 * @param statement Partial or slightly incorrect SQL statement
	 * @param position cursor position
	 * @return completion hints that fit at the current cursor position
	 */
	public abstract String[] getCompletionHints(String statement, int position);

	/**
	 * Evaluates a SQL query on registered tables and retrieves the result as a [[Table]].
	 *
	 * All tables referenced by the query must be registered in the TableEnvironment.
	 * A [[Table]] is automatically registered when its [[toString]] method is called, for example
	 * when it is embedded into a String.
	 * Hence, SQL queries can directly reference a [[Table]] as follows:
	 *
	 * {{{
	 *   val table: Table = ...
	 *   // the table is not registered to the table environment
	 *   tEnv.sqlQuery(s"SELECT * FROM $table")
	 * }}}
	 *
	 * @param query The SQL query to evaluate.
	 * @return The result of the query as Table
	 */
	public abstract Table sqlQuery(String query);

	/**
	 * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
	 * NOTE: Currently only SQL INSERT statements are supported.
	 *
	 * All tables referenced by the query must be registered in the TableEnvironment.
	 * A [[Table]] is automatically registered when its [[toString]] method is called, for example
	 * when it is embedded into a String.
	 * Hence, SQL queries can directly reference a [[Table]] as follows:
	 *
	 * {{{
	 *   // register the table sink into which the result is inserted.
	 *   tEnv.registerTableSink("sinkTable", fieldNames, fieldsTypes, tableSink)
	 *   val sourceTable: Table = ...
	 *   // sourceTable is not registered to the table environment
	 *   tEnv.sqlUpdate(s"INSERT INTO sinkTable SELECT * FROM $sourceTable")
	 * }}}
	 *
	 * @param stmt The SQL statement to evaluate.
	 */
	public abstract void sqlUpdate(String stmt);

	/**
	 * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
	 * NOTE: Currently only SQL INSERT statements are supported.
	 *
	 * All tables referenced by the query must be registered in the TableEnvironment.
	 * A [[Table]] is automatically registered when its [[toString]] method is called, for example
	 * when it is embedded into a String.
	 * Hence, SQL queries can directly reference a [[Table]] as follows:
	 *
	 * {{{
	 *   // register the table sink into which the result is inserted.
	 *   tEnv.registerTableSink("sinkTable", fieldNames, fieldsTypes, tableSink)
	 *   val sourceTable: Table = ...
	 *   // sourceTable is not registered to the table environment
	 *   tEnv.sqlUpdate(s"INSERT INTO sinkTable SELECT * FROM $sourceTable")
	 * }}}
	 *
	 * @param stmt The SQL statement to evaluate.
	 * @param config The [[QueryConfig]] to use.
	 */
	public abstract void sqlUpdate(String stmt, QueryConfig config);
}
