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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.operations.TableOperation;

import java.util.Map;
import java.util.Optional;

@Internal
public class TableOperationCatalogView implements CatalogView {
	private final TableOperation tableOperation;

	public TableOperationCatalogView(TableOperation tableOperation) {
		this.tableOperation = tableOperation;
	}

	@Override
	public String getOriginalQuery() {
		return tableOperation.toString();
	}

	@Override
	public String getExpandedQuery() {
		return tableOperation.toString();
	}

	@Override
	public Map<String, String> getProperties() {
		throw new UnsupportedOperationException("TableOperationCatalogView is not persistent.");
	}

	@Override
	public TableSchema getSchema() {
		return tableOperation.getTableSchema();
	}

	@Override
	public CatalogBaseTable copy() {
		throw new UnsupportedOperationException("TableOperationCatalogView is not persistent.");
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.empty();
	}

	@Override
	public Optional<String> getDetailedDescription() {
		return Optional.empty();
	}

	public TableOperation getTableOperation() {
		return tableOperation;
	}
}
