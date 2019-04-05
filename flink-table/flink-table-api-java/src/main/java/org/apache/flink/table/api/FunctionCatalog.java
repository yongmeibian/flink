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
import org.apache.flink.table.expressions.AggregateFunctionDefinition;
import org.apache.flink.table.expressions.BuiltInFunctionDefinitions;
import org.apache.flink.table.expressions.FunctionDefinition;
import org.apache.flink.table.expressions.ScalarFunctionDefinition;
import org.apache.flink.table.expressions.TableFunctionDefinition;
import org.apache.flink.table.expressions.catalog.FunctionDefinitionCatalog;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A catalog for looking up (user-defined) functions, used during resolution & validation phase
 * of both Table API and SQL API.
 */
public class FunctionCatalog implements FunctionDefinitionCatalog {

	private final List<FunctionCatalogObservator> catalogObservators = new ArrayList<>();
	private final Map<String, FunctionDefinition> userFunctions = new LinkedHashMap<>();
	private final Map<String, FunctionDefinition> builtinFunctions;

	public FunctionCatalog() {
		builtinFunctions = BuiltInFunctionDefinitions.getDefinitions().stream()
			.collect(Collectors.toMap(
				definition -> normalizeName(definition.getName()),
				Function.identity(),
				(m1, m2) -> m2,
				LinkedHashMap::new
			));
	}

	private String normalizeName(String name) {
		return name.toUpperCase();
	}

	@Override
	public FunctionDefinition lookupFunction(String name) {
		final String normalizedName = normalizeName(name);
		final FunctionDefinition builtinFunction = builtinFunctions.get(normalizedName);
		if (builtinFunction != null) {
			return builtinFunction;
		}

		final FunctionDefinition userFunction = userFunctions.get(normalizedName);
		if (userFunction == null) {
			throw new ValidationException("Undefined function: " + name);
		}
		return userFunction;
	}

	public Collection<String> getUserDefinedFunctions() {
		return userFunctions.values().stream().map(FunctionDefinition::getName).collect(Collectors.toList());
	}

	public void registerScalarFunction(String name, ScalarFunction function) {
		final ScalarFunctionDefinition functionDefinition = new ScalarFunctionDefinition(name, function);
		this.userFunctions.put(normalizeName(name), functionDefinition);
		catalogObservators.forEach(o -> o.onScalarFunctionRegistration(functionDefinition));
	}

	public void registerAggregateFunction(
			String name,
			AggregateFunction function,
			TypeInformation resultType,
			TypeInformation accType) {
		final AggregateFunctionDefinition functionDefinition = new AggregateFunctionDefinition(
			name,
			function,
			resultType,
			accType);
		this.userFunctions.put(normalizeName(name), functionDefinition);
		catalogObservators.forEach(o -> o.onAggregateFunctionRegistration(functionDefinition));
	}

	public void registerTableFunction(String name, TableFunction<?> function, TypeInformation resultType) {
		final TableFunctionDefinition functionDefinition = new TableFunctionDefinition(name, function, resultType);
		this.userFunctions.put(normalizeName(name), functionDefinition);
		catalogObservators.forEach(o -> o.onTableFunctionRegistration(functionDefinition));
	}

	/**
	 * Registers given observator for notification on changes.
	 * @param catalogObservator observator to be notifified
	 */
	public void registerObservator(FunctionCatalogObservator catalogObservator) {
		this.catalogObservators.add(catalogObservator);
	}

	/**
	 * Allows tracking changes to the {@link FunctionCatalog}.
	 */
	public interface FunctionCatalogObservator {

		/**
		 * Called whenever {@link ScalarFunction} is registered.
		 *
		 * @param function registered function
		 */
		void onScalarFunctionRegistration(ScalarFunctionDefinition function);

		/**
		 * Called whenever {@link AggregateFunction} is registered.
		 *
		 * @param function registered function
		 */
		void onAggregateFunctionRegistration(AggregateFunctionDefinition function);

		/**
		 * Called whenever {@link TableFunction} is registered.
		 *
		 * @param function registered function
		 */
		void onTableFunctionRegistration(TableFunctionDefinition function);
	}

}
