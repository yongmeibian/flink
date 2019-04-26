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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.CatalogAlreadyExistsException;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.ExternalCatalogAlreadyExistException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.operations.CatalogTableOperation;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A CatalogManager implementation for Flink.
 * TODO: [FLINK-11275] Decouple CatalogManager with Calcite
 *   Idealy FlinkCatalogManager should be in flink-table-api-java module.
 *   But due to that it currently depends on Calcite, a dependency that flink-table-api-java doesn't have right now.
 *   We temporarily put FlinkCatalogManager in flink-table-planner-blink.
 */
@Internal
public class FlinkCatalogManager implements CatalogManager {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkCatalogManager.class);

	// A map between names and catalogs.
	private Map<String, ReadableCatalog> catalogs;

	// TO BE REMOVED
	private Map<String, ExternalCatalog>  externalCatalogs;

	// The name of the default catalog and schema
	private String currentCatalogName;

	private String currentDatabaseName;

	public FlinkCatalogManager(String defaultCatalogName, ReadableCatalog defaultCatalog) {
		LOG.info("Initializing FlinkCatalogManager");
		catalogs = new LinkedHashMap<>();
		externalCatalogs = new LinkedHashMap<>();
		try {
			registerCatalog(defaultCatalogName, defaultCatalog);
			this.currentCatalogName = defaultCatalogName;
			this.currentDatabaseName = defaultCatalog.getCurrentDatabase();
		} catch (CatalogAlreadyExistsException e) {
			// ignore there are no catalogs so far
		}
	}

	@Override
	public void registerCatalog(String catalogName, ReadableCatalog catalog) throws CatalogAlreadyExistsException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		checkNotNull(catalog, "catalog cannot be null");

		if (catalogs.containsKey(catalogName) || externalCatalogs.containsKey(catalogName)) {
			throw new CatalogAlreadyExistsException(catalogName);
		}

		catalogs.put(catalogName, catalog);
		catalog.open();
	}

	@Deprecated
	public void registerExternalCatalog(
			String catalogName,
			ExternalCatalog catalog) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		checkNotNull(catalog, "catalog cannot be null");

		if (externalCatalogs.containsKey(catalogName) || catalogs.containsKey(catalogName)) {
			throw new ExternalCatalogAlreadyExistException(catalogName);
		}

		externalCatalogs.put(catalogName, catalog);
	}

	@Override
	public ReadableCatalog getCatalog(String catalogName) throws CatalogNotExistException {
		if (!catalogs.keySet().contains(catalogName)) {
			throw new CatalogNotExistException(catalogName);
		}

		return catalogs.get(catalogName);
	}

	@Deprecated
	public Optional<ExternalCatalog> getExternalCatalog(String externalCatalogName) {
		return Optional.ofNullable(externalCatalogs.get(externalCatalogName));
	}

	@Override
	public Set<String> getCatalogNames() {
		return catalogs.keySet();
	}

	public Set<String> getExternalCatalogNames() {
		return externalCatalogs.keySet();
	}

	@Override
	public String getCurrentCatalog() {
		return currentCatalogName;
	}

	@Override
	public void setCurrentCatalog(String catalogName) throws CatalogNotExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");

		if (!catalogs.keySet().contains(catalogName) && !externalCatalogs.keySet().contains(catalogName)) {
			throw new CatalogNotExistException(catalogName);
		}

		if (!currentCatalogName.equals(catalogName)) {
			currentCatalogName = catalogName;

			LOG.info("Set default catalog as '{}' and default database as '{}'",
				currentCatalogName, catalogs.get(currentCatalogName).getCurrentDatabase());
		}
	}

	@Override
	public String getCurrentDatabase() {
		return currentDatabaseName;
	}

	@Override
	public void setCurrentDatabase(String databaseName) throws DatabaseNotExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");

		if (!catalogs.keySet().contains(databaseName) && !externalCatalogs.keySet().contains(databaseName)) {
			throw new DatabaseNotExistException(currentCatalogName, databaseName);
		}

		if (!currentDatabaseName.equals(databaseName)) {
			currentDatabaseName = databaseName;

			LOG.info("Set the default catalog as '{}' and the default database as '{}'",
				currentCatalogName, currentDatabaseName);
		}
	}

	@Override
	public Optional<CatalogTableOperation> lookupTable(String... tablePath) {
		checkArgument(tablePath != null && tablePath.length != 0, "Table path must not be null or empty.");

		List<String> defaultPath = new ArrayList<>();
		defaultPath.add(currentCatalogName);
		defaultPath.add(currentDatabaseName);

		List<String> userPath = Arrays.asList(tablePath);
		defaultPath.addAll(userPath);

		Optional<CatalogTableOperation> inDefaultPath = lookupPath(defaultPath);

		if (inDefaultPath.isPresent()) {
			return inDefaultPath;
		} else {
			return lookupPath(userPath);
		}
	}

	private Optional<CatalogTableOperation> lookupPath(List<String> path) {
		try {
			Optional<TableSchema> potentialTable = lookupCatalogTable(path);

			if (!potentialTable.isPresent()) {
				potentialTable = lookupExternalTable(path);
			}
			return potentialTable.map(schema -> new CatalogTableOperation(path, schema));
		} catch (TableNotExistException e) {
			return Optional.empty();
		}
	}

	private Optional<TableSchema> lookupCatalogTable(List<String> path) throws TableNotExistException {
		if (path.size() >= 3) {
			ReadableCatalog currentCatalog = catalogs.get(path.get(0));
			String currentDatabaseName = path.get(1);
			String tableName = String.join(".", path.subList(2, path.size()));
			ObjectPath objectPath = new ObjectPath(currentDatabaseName, tableName);

			if (currentCatalog != null && currentCatalog.tableExists(objectPath)) {
				return Optional.of(currentCatalog.getTable(objectPath).getSchema());
			}
		}

		return Optional.empty();
	}

	private Optional<TableSchema> lookupExternalTable(List<String> path) {
		ExternalCatalog currentCatalog = externalCatalogs.get(path.get(0));
		return Optional.ofNullable(currentCatalog)
			.flatMap(externalCatalog -> extractPath(externalCatalog, path.subList(1, path.size() - 1)))
			.map(finalCatalog -> finalCatalog.getTable(path.get(path.size() - 1)))
			.map(ExternalTableUtil::getTableSchema);
	}

	private Optional<ExternalCatalog> extractPath(ExternalCatalog rootExternalCatalog, List<String> path) {
		ExternalCatalog schema = rootExternalCatalog;
		for (String pathPart : path) {
			schema = schema.getSubCatalog(pathPart);
			if (schema == null) {
				return Optional.empty();
			}
		}
		return Optional.of(schema);
	}
}
