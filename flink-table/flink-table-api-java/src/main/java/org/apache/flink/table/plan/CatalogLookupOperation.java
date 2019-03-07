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

package org.apache.flink.table.plan;

import org.apache.flink.table.api.TableSchema;

import java.util.Collections;
import java.util.List;

public final class CatalogLookupOperation implements Operation {

	private final List<String> tablePath;
	private final TableSchema tableSchema;

	public CatalogLookupOperation(List<String> tablePath, TableSchema tableSchema) {
		this.tablePath = tablePath;
		this.tableSchema = tableSchema;
	}

	public List<String> getTablePath() {
		return tablePath;
	}

	@Override
	public List<Operation> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public TableSchema getSchema() {
		return tableSchema;
	}

	@Override
	public <R> R accept(OperationTreeVisitor<R> visitor) {
		return visitor.visitCatalogLookupOperation(this);
	}
}
