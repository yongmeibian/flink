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

package org.apache.flink.table.utils;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.module.ModuleManager;

/**
 * Mocking {@link TableEnvironment} for tests.
 */
public class TableEnvironmentMock extends TableEnvironmentImpl {

	public final CatalogManager catalogManager;

	public final Executor executor;

	public final FunctionCatalog functionCatalog;

	public final Planner planner;

	protected TableEnvironmentMock(
			CatalogManager catalogManager,
			ModuleManager moduleManager,
			TableConfig tableConfig,
			Executor executor,
			FunctionCatalog functionCatalog,
			Planner planner,
			boolean isStreamingMode) {
		super(catalogManager, moduleManager, tableConfig, executor, functionCatalog, planner, isStreamingMode);

		this.catalogManager = catalogManager;
		this.executor = executor;
		this.functionCatalog = functionCatalog;
		this.planner = planner;
	}

	public static TableEnvironmentMock getStreamingInstance() {
		return mock().createBatchInstance();
	}

	public static TableEnvironmentMock getBatchInstance() {
		return mock().createStreamingInstance();
	}

	public static TableEnvironmentBuilder mock() {
		return new TableEnvironmentBuilder();
	}

	public static class TableEnvironmentBuilder {
		private CatalogManager catalogManager;
		private Executor executor;
		private FunctionCatalog functionCatalog;
		private Planner planner;
		private ModuleManager moduleManager;
		private TableConfig tableConfig;

		public TableEnvironmentBuilder setCatalogManager(CatalogManager catalogManager) {
			this.catalogManager = catalogManager;
			return this;
		}

		public TableEnvironmentBuilder setExecutor(Executor executor) {
			this.executor = executor;
			return this;
		}

		public TableEnvironmentBuilder setFunctionCatalog(FunctionCatalog functionCatalog) {
			this.functionCatalog = functionCatalog;
			return this;
		}

		public TableEnvironmentBuilder setPlanner(Planner planner) {
			this.planner = planner;
			return this;
		}

		public TableEnvironmentBuilder setModuleManager(ModuleManager moduleManager) {
			this.moduleManager = moduleManager;
			return this;
		}

		public TableEnvironmentBuilder setTableConfig(TableConfig tableConfig) {
			this.tableConfig = tableConfig;
			return this;
		}

		public TableEnvironmentMock createBatchInstance() {
			return create(false);
		}

		public TableEnvironmentMock createStreamingInstance() {
			return create(true);
		}

		private TableEnvironmentMock create(boolean isStreaming) {
			TableConfig tableConfig = this.tableConfig != null ? this.tableConfig : createTableConfig();
			CatalogManager catalogManager = this.catalogManager != null ? this.catalogManager : createCatalogManager();
			ModuleManager moduleManager = this.moduleManager != null ? this.moduleManager : new ModuleManager();
			return new TableEnvironmentMock(
				catalogManager,
				moduleManager,
				tableConfig,
				executor != null ? executor : createExecutor(),
				functionCatalog != null ? functionCatalog : createFunctionCatalog(
					tableConfig,
					catalogManager,
					moduleManager),
				planner != null ? planner : createPlanner(),
				isStreaming
			);
		}

		private static CatalogManager createCatalogManager() {
			return new CatalogManager(
				EnvironmentSettings.DEFAULT_BUILTIN_CATALOG,
				new GenericInMemoryCatalog(
					EnvironmentSettings.DEFAULT_BUILTIN_CATALOG,
					EnvironmentSettings.DEFAULT_BUILTIN_DATABASE));
		}

		private static TableConfig createTableConfig() {
			return TableConfig.getDefault();
		}

		private static ExecutorMock createExecutor() {
			return new ExecutorMock();
		}

		private static FunctionCatalog createFunctionCatalog(
			TableConfig config,
			CatalogManager catalogManager,
			ModuleManager moduleManager) {
			return new FunctionCatalog(config, catalogManager, moduleManager);
		}

		private static PlannerMock createPlanner() {
			return new PlannerMock();
		}
	}
}
