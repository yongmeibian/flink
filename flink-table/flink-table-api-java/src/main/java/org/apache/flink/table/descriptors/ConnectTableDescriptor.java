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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Describes a table connected from {@link TableEnvironment#connect(ConnectorDescriptor)}.
 *
 * <p>It can access {@link TableEnvironment} for fluently registering the table.
 */
@PublicEvolving
public abstract class ConnectTableDescriptor
	extends TableDescriptor<ConnectTableDescriptor> {

	private final Registration registration;

	/**
	 * A way to register a table in the {@link TableEnvironment} that this descriptor originates from.
	 */
	@Internal
	public interface Registration {

		/**
		 * Registers an external {@link TableSource} in this {@link TableEnvironment}'s catalog.
		 * Registered tables can be referenced in SQL queries.
		 *
		 * @param name        The name under which the {@link TableSource} is registered.
		 * @param tableSource The {@link TableSource} to register.
		 * @see TableEnvironment#registerTableSource(String, TableSource)
		 */
		void createTableSource(String name, TableSource<?> tableSource);

		/**
		 * Registers an external {@link TableSink} with already configured field names and field types in
		 * this {@link TableEnvironment}'s catalog.
		 * Registered sink tables can be referenced in SQL DML statements.
		 *
		 * @param name The name under which the {@link TableSink} is registered.
		 * @param tableSink The configured {@link TableSink} to register.
		 * @see TableEnvironment#registerTableSink(String, TableSink)
		 */
		void createTableSink(String name, TableSink<?> tableSink);

		/**
		 * Creates a temporary table in a given path.
		 *
		 * @param path Path where to register the given table
		 * @param table table to register
		 */
		void createTemporaryTable(String path, CatalogBaseTable table);
	}

	private @Nullable Schema schemaDescriptor;

	public ConnectTableDescriptor(Registration registration, ConnectorDescriptor connectorDescriptor) {
		super(connectorDescriptor);
		this.registration = registration;
	}

	/**
	 * Specifies the resulting table schema.
	 */
	public ConnectTableDescriptor withSchema(Schema schema) {
		schemaDescriptor = Preconditions.checkNotNull(schema, "Schema must not be null.");
		return this;
	}

	/**
	 * Searches for the specified table source, configures it accordingly, and registers it as
	 * a table under the given name.
	 *
	 * @param name table name to be registered in the table environment
	 * @deprecated use {@link #createTemporaryTable(String)}
	 */
	@Deprecated
	public void registerTableSource(String name) {
		Preconditions.checkNotNull(name);
		TableSource<?> tableSource = TableFactoryUtil.findAndCreateTableSource(this);
		registration.createTableSource(name, tableSource);
	}

	/**
	 * Searches for the specified table sink, configures it accordingly, and registers it as
	 * a table under the given name.
	 *
	 * @param name table name to be registered in the table environment
	 * @deprecated use {@link #createTemporaryTable(String)}
	 */
	@Deprecated
	public void registerTableSink(String name) {
		Preconditions.checkNotNull(name);
		TableSink<?> tableSink = TableFactoryUtil.findAndCreateTableSink(this);
		registration.createTableSink(name, tableSink);
	}

	/**
	 * Searches for the specified table source and sink, configures them accordingly, and registers
	 * them as a table under the given name.
	 *
	 * @param name table name to be registered in the table environment
	 * @deprecated use {@link #createTemporaryTable(String)}
	 */
	@Deprecated
	public void registerTableSourceAndSink(String name) {
		registerTableSource(name);
		registerTableSink(name);
	}

	/**
	 * Registers the table described by underlying properties in a given path.
	 *
	 * <p><b>NOTE:</b> The schema must be explicitly defined.
	 *
	 * @param path path where to register the temporary table
	 */
	public void createTemporaryTable(String path) {
		if (schemaDescriptor == null) {
			throw new TableException(
				"Table schema must be explicitly defined. To derive schema from the underlying connector" +
					" use registerTableSource/registerTableSink/registerTableSourceAndSink.");
		}

		Map<String, String> schemaProperties = schemaDescriptor.toProperties();
		TableSchema tableSchema = getTableSchema(schemaProperties);

		Map<String, String> properties = new HashMap<>(toProperties());
		schemaProperties.keySet().forEach(properties::remove);

		CatalogTableImpl catalogTable = new CatalogTableImpl(
			tableSchema,
			properties,
			""
		);

		registration.createTemporaryTable(path, catalogTable);
	}

	private TableSchema getTableSchema(Map<String, String> schemaProperties) {
		DescriptorProperties properties = new DescriptorProperties();
		properties.putProperties(schemaProperties);
		return properties.getTableSchema(Schema.SCHEMA);
	}

	@Override
	protected Map<String, String> additionalProperties() {
		if (schemaDescriptor != null) {
			return schemaDescriptor.toProperties();
		}
		return Collections.emptyMap();
	}
}
