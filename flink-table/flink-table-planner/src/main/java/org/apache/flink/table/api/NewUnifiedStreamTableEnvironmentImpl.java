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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.calcite.FlinkTypeSystem;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.descriptors.TableDescriptor;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.PlannerExpressionConverter$;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.operations.CatalogQueryOperation;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationTreeBuilder;
import org.apache.flink.table.operations.OperationTreeBuilderImpl;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.planner.Planner;
import org.apache.flink.table.planner.StreamPlanner;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceUtil;
import org.apache.flink.table.validate.FunctionCatalog;
import org.apache.flink.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Internal
public class NewUnifiedStreamTableEnvironmentImpl implements TableEnvironment {

	private final CatalogManager catalogManager;
	private final TableConfig tableConfig;

	private final String defaultCatalogName;
	private final String defaultDatabaseName;

	private final OperationTreeBuilder operationTreeBuilder;

	protected final FlinkTypeFactory typeFactory = new FlinkTypeFactory(new FlinkTypeSystem());
	protected final StreamExecutionEnvironment execEnv;
	protected final FunctionCatalog functionCatalog;
	protected final Planner planner;

	public NewUnifiedStreamTableEnvironmentImpl(
			CatalogManager catalogManager,
			TableConfig tableConfig,
			StreamExecutionEnvironment executionEnvironment) {
		this.catalogManager = catalogManager;
		this.execEnv = executionEnvironment;
		this.tableConfig = tableConfig;
		this.defaultCatalogName = tableConfig.getBuiltInCatalogName();
		this.defaultDatabaseName = tableConfig.getBuiltInDatabaseName();

		this.functionCatalog = new FunctionCatalog();
		this.planner = new StreamPlanner(execEnv, tableConfig, functionCatalog, catalogManager);
		ExpressionBridge<PlannerExpression> expressionBridge = new ExpressionBridge<>(
			functionCatalog,
			PlannerExpressionConverter$.MODULE$.INSTANCE());
		operationTreeBuilder = new OperationTreeBuilderImpl(
			path -> {
				Optional<CatalogQueryOperation> catalogTableOperation = scanInternal(path);
				return catalogTableOperation.map(tableOperation -> new TableReferenceExpression(path, tableOperation));
			},
			expressionBridge,
			functionCatalog,
			true
		);
	}

	@Override
	public Table fromTableSource(TableSource<?> source) {
		return createTable(new TableSourceQueryOperation(source, false));
	}

	@Override
	public void registerExternalCatalog(String name, ExternalCatalog externalCatalog) {
		catalogManager.registerExternalCatalog(name, externalCatalog);
	}

	@Override
	public ExternalCatalog getRegisteredExternalCatalog(String name) {
		return catalogManager.getExternalCatalog(name).orElseThrow(() -> new ExternalCatalogNotExistException(name));
	}

	@Override
	public void registerCatalog(String catalogName, Catalog catalog) {
		catalogManager.registerCatalog(catalogName, catalog);
	}

	@Override
	public Optional<Catalog> getCatalog(String catalogName) {
		return catalogManager.getCatalog(catalogName);
	}

	@Override
	public void registerFunction(String name, ScalarFunction function) {
		// check if class could be instantiated
		UserDefinedFunctionUtils.checkForInstantiation(function.getClass());

		functionCatalog.registerScalarFunction(
			name,
			function,
			typeFactory);
	}

	@Override
	public void registerTable(String name, Table table) {
		if (((TableImpl) table).getTableEnvironment() != this) {
			throw new TableException(
				"Only tables that belong to this TableEnvironment can be registered.");
		}

		CatalogBaseTable tableTable = new QueryOperationCatalogView(table.getQueryOperation());
		registerTableInternal(name, tableTable);
	}

	@Override
	public void registerTableSource(String name, TableSource<?> tableSource) {
		if (!(tableSource instanceof StreamTableSource<?>)) {
			throw new TableException("Only StreamTableSource can be registered in " +
				"StreamTableEnvironment");
		}

		registerTableSourceInternal(name, tableSource);
	}

	@Override
	public void registerTableSink(
			String name,
			String[] fieldNames,
			TypeInformation<?>[] fieldTypes,
			TableSink<?> tableSink) {
		registerTableSink(name, tableSink.configure(fieldNames, fieldTypes));
	}

	@Override
	public void registerTableSink(String name, TableSink<?> configuredSink) {
		// validate
		if (configuredSink.getTableSchema().getFieldCount() == 0) {
			throw new TableException("Table schema cannot be empty.");
		}

		if (!(configuredSink instanceof StreamTableSink<?>)) {
			throw new TableException(
				"Only AppendStreamTableSink, UpsertStreamTableSink, and RetractStreamTableSink can be " +
					"registered in StreamTableEnvironment.");
		}

		checkValidTableName(name);
		registerTableSinkInternal(name, configuredSink);
	}

	@Override
	public Table scan(String... tablePath) {
		return scanInternal(tablePath).map(this::createTable)
			.orElseThrow(() -> new TableException(String.format(
				"Table '%s' was not found.",
				String.join(".", tablePath))));
	}

	private Optional<CatalogQueryOperation> scanInternal(String... tablePath) {
		return catalogManager.resolveTable(tablePath)
			.map(t -> new CatalogQueryOperation(t.getTablePath(), t.getTableSchema()));
	}

	@Override
	public TableDescriptor connect(ConnectorDescriptor connectorDescriptor) {
		return new StreamTableDescriptor(this, connectorDescriptor);
	}

	@Override
	public String[] listTables() {
		String currentCatalogName = catalogManager.getCurrentCatalog();
		Optional<Catalog> currentCatalog = catalogManager.getCatalog(currentCatalogName);

		return currentCatalog.map(catalog -> {
			try {
				return catalog.listTables(catalogManager.getCurrentDatabase()).toArray(new String[0]);
			} catch (DatabaseNotExistException e) {
				throw new ValidationException("Current database does not exist", e);
			}
		}).orElseThrow(() ->
			new TableException(String.format("The current catalog %s does not exist.", currentCatalogName)));
	}

	@Override
	public String[] listUserDefinedFunctions() {
		return functionCatalog.getUserDefinedFunctions();
	}

	@Override
	public String explain(Table table) {
		return planner.explain(Collections.singletonList(table.getQueryOperation()), false);
	}

	@Override
	public String[] getCompletionHints(String statement, int position) {
		return planner.getCompletionHints(statement, position);
	}

	@Override
	public Table sqlQuery(String query) {
		List<Operation> operations = planner.parse(query);

		if (operations.size() != 1) {
			throw new ValidationException(
				"Unsupported SQL query! sqlQuery() only accepts a single SQL queries of type " +
					"SELECT, UNION, INTERSECT, EXCEPT, VALUES, and ORDER_BY.");
		}

		Operation operation = operations.get(0);

		if (operation instanceof QueryOperation && !(operation instanceof ModifyOperation)) {
			return createTable((QueryOperation) operation);
		} else {
			throw new ValidationException(
				"Unsupported SQL query! sqlQuery() only accepts a single SQL queries of type " +
					"SELECT, UNION, INTERSECT, EXCEPT, VALUES, and ORDER_BY.");
		}
	}

	@Override
	public void sqlUpdate(String stmt) {
		sqlUpdate(stmt, new StreamQueryConfig());
	}

	@Override
	public void insertInto(Table table, QueryConfig queryConfig, String path, String... pathContinued) {
		List<String> fullPath = new ArrayList<>(Arrays.asList(pathContinued));
		fullPath.add(0, path);

		List<StreamTransformation<?>> translate = planner.translate(Collections.singletonList(new CatalogSinkModifyOperation(
			fullPath,
			table.getQueryOperation())));

		translate.forEach(execEnv::addOperator);
	}

	@Override
	public void insertInto(Table table, String path, String... pathContinued) {
		insertInto(table, new StreamQueryConfig(), path, pathContinued);
	}

	@Override
	public void sqlUpdate(String stmt, QueryConfig config) {
		List<Operation> operations = planner.parse(stmt);

		if (operations.size() != 1) {
			throw new TableException(
				"Unsupported SQL query! sqlUpdate() only accepts a single SQL statements of type INSERT.");
		}

		Operation operation = operations.get(0);

		if (operation instanceof ModifyOperation) {
			List<StreamTransformation<?>> transformations =
				planner.translate(Collections.singletonList((ModifyOperation) operation));

			transformations.forEach(execEnv::addOperator);
		} else {
			throw new TableException(
				"Unsupported SQL query! sqlUpdate() only accepts a single SQL statements of type INSERT.");
		}
	}

	@Override
	public String getCurrentCatalog() {
		return catalogManager.getCurrentCatalog();
	}

	@Override
	public void useCatalog(String catalogName) {
		catalogManager.setCurrentCatalog(catalogName);
	}

	@Override
	public String getCurrentDatabase() {
		return catalogManager.getCurrentDatabase();
	}

	@Override
	public void useDatabase(String databaseName) {
		catalogManager.setCurrentDatabase(databaseName);
	}

	@Override
	public TableConfig getConfig() {
		return tableConfig;
	}

	protected void registerTableInternal(String name, CatalogBaseTable table) {
		try {
			checkValidTableName(name);
			ObjectPath path = new ObjectPath(defaultDatabaseName, name);
			Optional<Catalog> catalog = catalogManager.getCatalog(defaultCatalogName);
			if (catalog.isPresent()) {
				catalog.get().createTable(
					path,
					table,
					false);
			}
		} catch (Exception e) {
			throw new TableException("Could not register table", e);
		}
	}

	protected void replaceTableInternal(String name, CatalogBaseTable table) {
		try {
			ObjectPath path = new ObjectPath(defaultDatabaseName, name);
			Optional<Catalog> catalog = catalogManager.getCatalog(defaultCatalogName);
			if (catalog.isPresent()) {
				catalog.get().alterTable(
					path,
					table,
					false);
			}
			;
		} catch (Exception e) {
			throw new TableException("Could not register table", e);
		}
	}

	private void checkValidTableName(String name) {
		if (StringUtils.isNullOrWhitespaceOnly(name)) {
			throw new ValidationException("A table name cannot be null or consist of only whitespaces.");
		}
	}

	private void registerTableSourceInternal(String name, TableSource<?> tableSource) {
		// check that event-time is enabled if table source includes rowtime attributes
		if (TableSourceUtil.hasRowtimeAttribute(tableSource) &&
			execEnv.getStreamTimeCharacteristic() != TimeCharacteristic.EventTime) {
			throw new TableException(String.format(
				"A rowtime attribute requires an EventTime time characteristic in stream " +
					"environment. But is: %s}", execEnv.getStreamTimeCharacteristic()));
		}

		Optional<CatalogBaseTable> table = getCatalogTable(defaultCatalogName, defaultDatabaseName, name);

		if (table.isPresent()) {
			if (table.get() instanceof ConnectorCatalogTable<?, ?>) {
				ConnectorCatalogTable<?, ?> sourceSinkTable = (ConnectorCatalogTable<?, ?>) table.get();
				if (sourceSinkTable.getTableSource().isPresent()) {
					throw new TableException(String.format(
						"Table '%s' already exists. Please choose a different name.", name));
				} else {
					// wrapper contains only sink (not source)
					replaceTableInternal(
						name,
						ConnectorCatalogTable
							.sourceAndSink(tableSource, sourceSinkTable.getTableSink().get(), false));
				}
			}
		} else {
			registerTableInternal(name, ConnectorCatalogTable.source(tableSource, false));
		}
	}

	private void registerTableSinkInternal(String name, TableSink<?> tableSink) {
		Optional<CatalogBaseTable> table = getCatalogTable(defaultCatalogName, defaultDatabaseName, name);

		if (table.isPresent()) {
			if (table.get() instanceof ConnectorCatalogTable<?, ?>) {
				ConnectorCatalogTable<?, ?> sourceSinkTable = (ConnectorCatalogTable<?, ?>) table.get();
				if (sourceSinkTable.getTableSink().isPresent()) {
					throw new TableException(String.format(
						"Table '%s' already exists. Please choose a different name.", name));
				} else {
					// wrapper contains only sink (not source)
					replaceTableInternal(
						name,
						ConnectorCatalogTable
							.sourceAndSink(sourceSinkTable.getTableSource().get(), tableSink, false));
				}
			}
		} else {
			registerTableInternal(name, ConnectorCatalogTable.sink(tableSink, false));
		}

		throw new TableException("Only StreamTableSink can be registered in " +
			"StreamTableEnvironment");
	}

	private Optional<CatalogBaseTable> getCatalogTable(String... name) {
		return catalogManager.resolveTable(name).flatMap(CatalogManager.ResolvedTable::getCatalogTable);
	}

	protected TableImpl createTable(QueryOperation tableOperation) {
		return TableImpl.createTable(
			this,
			tableOperation,
			operationTreeBuilder,
			functionCatalog);
	}
}
