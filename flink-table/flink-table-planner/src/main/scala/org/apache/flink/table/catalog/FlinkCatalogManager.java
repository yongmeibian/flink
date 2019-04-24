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

import org.apache.flink.table.api.CatalogAlreadyExistsException;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
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
public class FlinkCatalogManager implements CatalogManager {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkCatalogManager.class);

	// A map between names and catalogs.
	private Map<String, ReadableCatalog> catalogs;

	// The name of the default catalog and schema
	private String currentCatalogName;

	public FlinkCatalogManager() {
		LOG.info("Initializing FlinkCatalogManager");
		catalogs = new HashMap<>();
	}

	@Override
	public void registerCatalog(String catalogName, ReadableCatalog catalog) throws CatalogAlreadyExistsException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		checkNotNull(catalog, "catalog cannot be null");

		if (catalogs.containsKey(catalogName)) {
			throw new CatalogAlreadyExistsException(catalogName);
		}

		catalogs.put(catalogName, catalog);
		catalog.open();
	}

	@Override
	public ReadableCatalog getCatalog(String catalogName) throws CatalogNotExistException {
		if (!catalogs.keySet().contains(catalogName)) {
			throw new CatalogNotExistException(catalogName);
		}

		return catalogs.get(catalogName);
	}

	@Override
	public Set<String> getCatalogNames() {
		return catalogs.keySet();
	}

	@Override
	public ReadableCatalog getCurrentCatalog() {
		return catalogs.get(currentCatalogName);
	}

	@Override
	public void setCurrentCatalog(String catalogName) throws CatalogNotExistException {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");

		if (!catalogs.keySet().contains(catalogName)) {
			throw new CatalogNotExistException(catalogName);
		}

		if (currentCatalogName == null || !currentCatalogName.equals(catalogName)) {
			currentCatalogName = catalogName;

			LOG.info("Set default catalog as '{}' and default database as '{}'",
				currentCatalogName, catalogs.get(currentCatalogName).getCurrentDatabase());
		}
	}
}
