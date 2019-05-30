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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.catalog.CalciteCatalogTable;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.descriptors.TableDescriptor;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpressionConverter$;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.operations.DmlTableOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.TableOperation;
import org.apache.flink.table.plan.schema.RelTable;
import org.apache.flink.table.plan.schema.StreamTableSourceTable;
import org.apache.flink.table.plan.schema.TableSourceSinkTable;
import org.apache.flink.table.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.Planner;
import org.apache.flink.table.planner.PlanningConfigurationBuilder;
import org.apache.flink.table.planner.StreamPlanner;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceUtil;
import org.apache.flink.table.validate.FunctionCatalog;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import scala.Option;
import scala.Some;

public class NewUnifiedStreamTableEnvironmentImpl implements TableEnvironment {

	private final CatalogManager catalogManager;
	private final FunctionCatalog functionCatalog;
	private final CalciteSchema internalSchema;
	private final StreamExecutionEnvironment execEnv;
	private final TableConfig tableConfig;

	private final AtomicInteger nameCntr = new AtomicInteger(0);
	private final String defaultCatalogName;
	private final String defaultDatabaseName;

	private final PlanningConfigurationBuilder planningConfigurationBuilder;

	private final Planner planner;

	public NewUnifiedStreamTableEnvironmentImpl(
			CatalogManager catalogManager,
			TableConfig tableConfig,
			FunctionCatalog functionCatalog,
			StreamExecutionEnvironment executionEnvironment) {
		this.catalogManager = catalogManager;
		this.execEnv = executionEnvironment;
		this.tableConfig = tableConfig;
		this.internalSchema = CalciteSchemaBuilder.asRootSchema(new CatalogManagerCalciteSchema(catalogManager, false));
		this.defaultCatalogName = tableConfig.getBuiltInCatalogName();
		this.defaultDatabaseName = tableConfig.getBuiltInDatabaseName();

		this.functionCatalog = functionCatalog;
		this.planner = new StreamPlanner(execEnv, tableConfig, functionCatalog, catalogManager);
		this.planningConfigurationBuilder = new PlanningConfigurationBuilder(
			tableConfig,
			functionCatalog,
			internalSchema,
			new ExpressionBridge<>(functionCatalog, PlannerExpressionConverter$.MODULE$.INSTANCE()));
	}

	@Override
	public Table fromTableSource(TableSource<?> source) {
		String name = createUniqueTableName();
		registerTableSourceInternal(name, source);
		return scan(name);
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
			planningConfigurationBuilder.getTypeFactory());
	}

	@Override
	public void registerTable(String name, Table table) {
		// TODO check that table belongs to this table environment
//		if (((TableImpl)table).tableEnv != this) {
//			throw new TableException(
//				"Only tables that belong to this TableEnvironment can be registered.")
//		}

		// TODO
//		checkValidTableName(name)
		RelTable tableTable = new RelTable(((TableImpl) table).getRelNode());
		registerTableInternal(name, tableTable);
	}

	@Override
	public void registerTableSource(String name, TableSource<?> tableSource) {
		//TODO
//		checkValidTableName(name)
		registerTableSourceInternal(name, tableSource);
	}

	@Override
	public void registerTableSink(
		String name, String[] fieldNames, TypeInformation<?>[] fieldTypes, TableSink<?> tableSink) {

	}

	@Override
	public void registerTableSink(String name, TableSink<?> configuredSink) {

	}

	@Override
	public Table scan(String... tablePath) {
		return catalogManager.resolveTable(tablePath).map(tableOperation -> new TableImpl(this, tableOperation))
			.orElseThrow(() -> new TableException(String.format(
				"Table '%s' was not found.",
				String.join(".", tablePath))));
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
		return planner.explain(Collections.singletonList(table.getTableOperation()), new StreamQueryConfig());
	}

	@Override
	public String[] getCompletionHints(String statement, int position) {
		return planner.getCompletionHints(statement, position);
	}

	@Override
	public Table sqlQuery(String query) {
		Operation operation = planner.parse(query);

		if (operation instanceof TableOperation && !(operation instanceof DmlTableOperation)) {
			return new TableImpl(this, (TableOperation) operation);
		} else {
			throw new TableException(
				"Unsupported SQL query! sqlQuery() only accepts SQL queries of type " +
					"SELECT, UNION, INTERSECT, EXCEPT, VALUES, and ORDER_BY.");
		}
	}

	@Override
	public void sqlUpdate(String stmt) {
		sqlUpdate(stmt, new StreamQueryConfig());
	}

	@Override
	public void sqlUpdate(String stmt, QueryConfig config) {
		Operation operation = planner.parse(stmt);

		if (operation instanceof DmlTableOperation) {
			List<StreamTransformation<?>> transformations = planner.translate(
				Collections.singletonList((TableOperation) operation),
				(StreamQueryConfig) config);

			transformations.forEach(execEnv::addOperator);
		} else {
			throw new TableException(
				"Unsupported SQL query! sqlUpdate() only accepts SQL statements of type INSERT.");
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

	private String createUniqueTableName() {
		return "_DataStreamTable_" + nameCntr.getAndIncrement();
	}

	/**
	 * Registers a Calcite [[AbstractTable]] in the TableEnvironment's default catalog.
	 *
	 * @param name The name under which the table will be registered.
	 * @param table The table to register in the catalog
	 * @throws TableException if another table is registered under the provided name.
	 */
	private void registerTableInternal(String name, AbstractTable table) {
		try {
			ObjectPath path = new ObjectPath(defaultDatabaseName, name);
			Optional<Catalog> catalog = catalogManager.getCatalog(defaultCatalogName);
			if (catalog.isPresent()) {
				catalog.get().createTable(
					path,
					new CalciteCatalogTable(table, planningConfigurationBuilder.getTypeFactory()),
					false);
			}
		} catch (Exception e) {
			throw new TableException("Could not register table", e);
		}
	}

	/**
	 * Replaces a registered Table with another Table under the same name.
	 * We use this method to replace a [[org.apache.flink.table.plan.schema.DataStreamTable]]
	 * with a [[org.apache.calcite.schema.TranslatableTable]].
	 *
	 * @param name Name of the table to replace.
	 * @param table The table that replaces the previous table.
	 */
	private void replaceRegisteredTable(String name, AbstractTable table) {
		try {
			ObjectPath path = new ObjectPath(defaultDatabaseName, name);
			Optional<Catalog> catalog = catalogManager.getCatalog(defaultCatalogName);
			if (catalog.isPresent()) {
				catalog.get().alterTable(
					path,
					new CalciteCatalogTable(table, planningConfigurationBuilder.getTypeFactory()),
					false);
			}
			;
		} catch (Exception e) {
			throw new TableException("Could not register table", e);
		}
	}

	/**
	 * Registers an internal [[StreamTableSource]] in this [[TableEnvImpl]]'s catalog without
	 * name checking. Registered tables can be referenced in SQL queries.
	 *
	 * @param name The name under which the [[TableSource]] is registered.
	 * @param tableSource The [[TableSource]] to register.
	 */
	@SuppressWarnings("unchecked")
	private void registerTableSourceInternal(String name, TableSource<?> tableSource) {
		if (tableSource instanceof StreamTableSource<?>) {
			// check that event-time is enabled if table source includes rowtime attributes
			if (TableSourceUtil.hasRowtimeAttribute(tableSource) &&
				execEnv.getStreamTimeCharacteristic() != TimeCharacteristic.EventTime) {
				throw new TableException(String.format(
					"A rowtime attribute requires an EventTime time characteristic in stream " +
						"environment. But is: %s}", execEnv.getStreamTimeCharacteristic()));
			}

			Optional<org.apache.calcite.schema.Table> table = getTable(defaultCatalogName, defaultDatabaseName, name);

			if (table.isPresent()) {
				if (table.get() instanceof TableSourceSinkTable) {
					TableSourceSinkTable sourceSinkTable = (TableSourceSinkTable) table.get();
					if (sourceSinkTable.isSourceTable()) {
						throw new TableException(String.format(
							"Table '%s' already exists. Please choose a different name.", name));
					} else {
						TableSourceSinkTable newTable = new TableSourceSinkTable<>(
							new Some<>(new StreamTableSourceTable<>(
								(StreamTableSource<?>) tableSource,
								FlinkStatistic.UNKNOWN())),
							sourceSinkTable.tableSinkTable());
						replaceRegisteredTable(name, newTable);
					}
				}
			} else {
				TableSourceSinkTable newTable = new TableSourceSinkTable<>(
					new Some<>(new StreamTableSourceTable<>(
						(StreamTableSource<?>) tableSource,
						FlinkStatistic.UNKNOWN())),
					Option.empty());
				registerTableInternal(name, newTable);
			}

			throw new TableException("Only StreamTableSource can be registered in " +
				"StreamTableEnvironment");
		}
	}

	/**
	 * Get a table from either internal or external catalogs.
	 *
	 * @param name The name of the table.
	 * @return The table registered either internally or externally, None otherwise.
	 */
	private Optional<org.apache.calcite.schema.Table> getTable(String... name) {
		return catalogManager.resolveTable(name).flatMap(t ->
			getTableFromSchema(internalSchema.plus(), t.getTablePath())
		);
	}

	// recursively fetches a table from a schema.
	private Optional<org.apache.calcite.schema.Table> getTableFromSchema(SchemaPlus schema, List<String> path) {
		Optional<SchemaPlus> subSchema = Optional.of(schema);
		for (String pathPart : path.subList(0, path.size() - 1)) {
			subSchema = subSchema.flatMap(s -> Optional.of(s.getSubSchema(pathPart)));
		}

		return subSchema.flatMap(s -> Optional.of(s.getTable(path.get(path.size() - 1))));
	}
}
