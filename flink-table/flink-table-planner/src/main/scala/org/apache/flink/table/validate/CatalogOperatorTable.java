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

package org.apache.flink.table.validate;

import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.expressions.AggregateFunctionDefinition;
import org.apache.flink.table.expressions.FunctionDefinition;
import org.apache.flink.table.expressions.ScalarFunctionDefinition;
import org.apache.flink.table.expressions.TableFunctionDefinition;
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;

import java.util.List;
import java.util.Optional;

public class CatalogOperatorTable implements SqlOperatorTable {

	private final FunctionCatalog functionCatalog;
	private final FlinkTypeFactory typeFactory;

	public CatalogOperatorTable(
			FunctionCatalog functionCatalog,
			FlinkTypeFactory typeFactory) {
		this.functionCatalog = functionCatalog;
		this.typeFactory = typeFactory;
	}

	@Override
	public void lookupOperatorOverloads(
			SqlIdentifier opName,
			SqlFunctionCategory category,
			SqlSyntax syntax,
			List<SqlOperator> operatorList) {
		if (!opName.isSimple() ) {
			return;
		}

		if (category != null && !category.isUserDefinedNotSpecificFunction()) {
			return;
		}

		String name = opName.getSimple();
		Optional<FunctionDefinition> candidateFunction = functionCatalog.lookupFunction(name);

		candidateFunction.ifPresent(functionDefinition -> {
			if (functionDefinition instanceof AggregateFunctionDefinition) {
				AggregateFunctionDefinition aggregateFunctionDefinition = (AggregateFunctionDefinition) functionDefinition;
				SqlFunction aggregateFunction = UserDefinedFunctionUtils.createAggregateSqlFunction(
					name,
					name,
					aggregateFunctionDefinition.getAggregateFunction(),
					aggregateFunctionDefinition.getResultTypeInfo(),
					aggregateFunctionDefinition.getAccumulatorTypeInfo(),
					typeFactory
				);
				operatorList.add(aggregateFunction);
			} else if (functionDefinition instanceof ScalarFunctionDefinition) {
				ScalarFunctionDefinition scalarFunctionDefinition = (ScalarFunctionDefinition) functionDefinition;
				SqlFunction scalarFunction = UserDefinedFunctionUtils.createScalarSqlFunction(
					name,
					name,
					scalarFunctionDefinition.getScalarFunction(),
					typeFactory
				);
				operatorList.add(scalarFunction);
			} else if (functionDefinition instanceof TableFunctionDefinition &&
					category != null &&
					category.isTableFunction()) {
				TableFunctionDefinition tableFunctionDefinition = (TableFunctionDefinition) functionDefinition;
				SqlFunction tableFunction = UserDefinedFunctionUtils.createTableSqlFunction(
					name,
					name,
					tableFunctionDefinition.getTableFunction(),
					tableFunctionDefinition.getResultType(),
					typeFactory
				);
				operatorList.add(tableFunction);
			}
		});
	}

	@Override
	public List<SqlOperator> getOperatorList() {
		throw new UnsupportedOperationException("This should never be called");
	}
}
