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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.util.TestLogger;

import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.catalog.CatalogStructureBuilder.BUILTIN_CATALOG_NAME;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.database;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.root;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.table;
import static org.apache.flink.table.catalog.CatalogStructureBuilder.temporaryTable;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CatalogManager}. See also {@link PathResolutionTest}.
 */
public class CatalogManagerTest extends TestLogger {

	private static final String TEST_CATALOG_NAME = "test";
	private static final String TEST_CATALOG_DEFAULT_DB_NAME = "test";
	private static final String BUILTIN_DEFAULT_DATABASE_NAME = "default";

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testRegisterCatalog() throws Exception {
		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME))
			.build();

		assertEquals(1, manager.getCatalogs().size());
		assertFalse(manager.getCatalogs().contains(TEST_CATALOG_NAME));

		manager.registerCatalog(TEST_CATALOG_NAME, new GenericInMemoryCatalog(TEST_CATALOG_NAME));

		assertEquals(2, manager.getCatalogs().size());
		assertTrue(manager.getCatalogs().contains(TEST_CATALOG_NAME));
	}

	@Test
	public void testSetCurrentCatalog() throws Exception {
		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME))
			.catalog(
				TEST_CATALOG_NAME,
				database(TEST_CATALOG_DEFAULT_DB_NAME))
			.build();

		assertEquals(CatalogStructureBuilder.BUILTIN_CATALOG_NAME, manager.getCurrentCatalog());
		assertEquals(BUILTIN_DEFAULT_DATABASE_NAME, manager.getCurrentDatabase());

		manager.setCurrentCatalog(TEST_CATALOG_NAME);

		assertEquals(TEST_CATALOG_NAME, manager.getCurrentCatalog());
		assertEquals(TEST_CATALOG_DEFAULT_DB_NAME, manager.getCurrentDatabase());
	}

	@Test
	public void testRegisterCatalogWithExistingName() throws Exception {
		thrown.expect(CatalogException.class);

		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME))
			.catalog(TEST_CATALOG_NAME, database(TEST_CATALOG_DEFAULT_DB_NAME))
			.build();

		manager.registerCatalog(TEST_CATALOG_NAME, new GenericInMemoryCatalog(TEST_CATALOG_NAME));
	}

	@Test
	public void testShadowingTemporaryTables() throws Exception {
		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME))
			.catalog(TEST_CATALOG_NAME, database(TEST_CATALOG_DEFAULT_DB_NAME, table("test")))
			.build();

		ObjectIdentifier identifier = ObjectIdentifier.of(TEST_CATALOG_NAME, TEST_CATALOG_DEFAULT_DB_NAME, "test");
		CatalogTable temporaryTable = temporaryTable(identifier);

		manager.createTemporaryTable(temporaryTable, identifier, false);
		assertThat(manager.getTable(identifier).get(), equalTo(temporaryTable));

		manager.dropTemporaryTable(UnresolvedIdentifier.of(
			identifier.getCatalogName(),
			identifier.getDatabaseName(),
			identifier.getObjectName()));
		assertThat(manager.getTable(identifier).get(), not(equalTo(temporaryTable)));
	}

	@Test
	public void testThrowExceptionIfTemporaryTableExists() throws Exception {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("Temporary table `test`.`test`.`test` already exists");

		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME))
			.catalog(TEST_CATALOG_NAME, database(TEST_CATALOG_DEFAULT_DB_NAME))
			.build();

		ObjectIdentifier identifier = ObjectIdentifier.of(TEST_CATALOG_NAME, TEST_CATALOG_DEFAULT_DB_NAME, "test");
		CatalogTable temporaryTable = temporaryTable(identifier);

		manager.createTemporaryTable(temporaryTable, identifier, false);
		manager.createTemporaryTable(temporaryTable, identifier, false);
	}

	@Test
	public void testReplaceTemporaryTable() throws Exception {
		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME))
			.build();

		ObjectIdentifier identifier = ObjectIdentifier.of(TEST_CATALOG_NAME, TEST_CATALOG_DEFAULT_DB_NAME, "test");

		manager.createTemporaryTable(temporaryTable(identifier), identifier, false);
		manager.createTemporaryTable(view(), identifier, true);

		assertThat(manager.getTable(identifier).get(), instanceOf(CatalogView.class));
	}

	@Test
	public void testDropTemporaryNonExistingTable() throws Exception {
		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME, table("test")))
			.build();

		boolean dropped = manager.dropTemporaryTable(UnresolvedIdentifier.of("test"));

		assertThat(dropped, is(false));
	}

	@Test
	public void testDropTemporaryTable() throws Exception {
		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME, table("test")))
			.build();

		ObjectIdentifier identifier = ObjectIdentifier.of(BUILTIN_CATALOG_NAME, BUILTIN_DEFAULT_DATABASE_NAME, "test");
		manager.createTemporaryTable(temporaryTable(identifier), identifier, false);
		boolean dropped = manager.dropTemporaryTable(UnresolvedIdentifier.of("test"));

		assertThat(dropped, is(true));
	}

	@Test
	public void testListTemporaryTables() throws Exception {
		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME, table("test_in_builtin")))
			.catalog(TEST_CATALOG_NAME, database(TEST_CATALOG_DEFAULT_DB_NAME, table("test_in_catalog")))
			.build();

		ObjectIdentifier identifier1 = ObjectIdentifier.of(TEST_CATALOG_NAME, TEST_CATALOG_DEFAULT_DB_NAME, "test");
		manager.createTemporaryTable(temporaryTable(identifier1), identifier1, false);
		ObjectIdentifier identifier2 = ObjectIdentifier.of(BUILTIN_CATALOG_NAME, BUILTIN_DEFAULT_DATABASE_NAME, "test");
		manager.createTemporaryTable(temporaryTable(identifier2), identifier2, false);
		ObjectIdentifier viewIdentifier = ObjectIdentifier.of(BUILTIN_CATALOG_NAME, BUILTIN_DEFAULT_DATABASE_NAME, "testView");
		manager.createTemporaryTable(view(), viewIdentifier, false);

		assertThat(manager.listTemporaryTables(), equalTo(new String[]{
			identifier2.toString(),
			identifier1.toString()
		}));
	}

	@Test
	public void testListTemporaryViews() throws Exception {
		CatalogManager manager = root()
			.builtin(
				database(BUILTIN_DEFAULT_DATABASE_NAME, table("test_in_builtin")))
			.catalog(TEST_CATALOG_NAME, database(TEST_CATALOG_DEFAULT_DB_NAME, table("test_in_catalog")))
			.build();

		ObjectIdentifier identifier1 = ObjectIdentifier.of(TEST_CATALOG_NAME, TEST_CATALOG_DEFAULT_DB_NAME, "test");
		manager.createTemporaryTable(view(), identifier1, false);
		ObjectIdentifier identifier2 = ObjectIdentifier.of(BUILTIN_CATALOG_NAME, BUILTIN_DEFAULT_DATABASE_NAME, "test");
		manager.createTemporaryTable(view(), identifier2, false);
		ObjectIdentifier tableIdentifier = ObjectIdentifier.of(BUILTIN_CATALOG_NAME, BUILTIN_DEFAULT_DATABASE_NAME, "table");
		manager.createTemporaryTable(temporaryTable(tableIdentifier), tableIdentifier, false);

		assertThat(manager.listTemporaryViews(), equalTo(new String[]{
			identifier2.toString(),
			identifier1.toString()
		}));
	}

	@Test
	public void testSetNonExistingCurrentCatalog() throws Exception {
		thrown.expect(CatalogException.class);
		thrown.expectMessage("A catalog with name [nonexistent] does not exist.");

		CatalogManager manager = root().build();
		manager.setCurrentCatalog("nonexistent");
	}

	@Test
	public void testSetNonExistingCurrentDatabase() throws Exception {
		thrown.expect(CatalogException.class);
		thrown.expectMessage("A database with name [nonexistent] does not exist in the catalog: [builtin].");

		CatalogManager manager = root().build();
		// This catalog does not exist in the builtin catalog
		manager.setCurrentDatabase("nonexistent");
	}

	public CatalogView view() {
		return new CatalogViewImpl(
			"SELECT * FROM t",
			"SELECT * FROM t",
			TableSchema.builder().build(),
			new HashMap<>(),
			""
		);
	}
}
