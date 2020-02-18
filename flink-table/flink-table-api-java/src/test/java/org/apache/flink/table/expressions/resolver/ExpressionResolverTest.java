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

package org.apache.flink.table.expressions.resolver;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.FunctionLookup;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.delegation.PlannerTypeInferenceUtil;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.operations.CatalogQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class ExpressionResolverTest {

	@Parameterized.Parameters(name = "{0}")
	public static Collection<TestSpec> parameters() {
		return Arrays.asList(
			TestSpec.test("Lookup calls")
				.inputSchemas(
					TableSchema.builder()
						.field("f0", DataTypes.INT())
						.build()
				)
				.lookupFunction("func", new ScalarFunc())
				.select(call("func", 1, $("f0")))
				.equalTo(new CallExpression(
					FunctionIdentifier.of("func"),
					new ScalarFunc(),
					Arrays.asList(valueLiteral(1), new FieldReferenceExpression("f0", DataTypes.INT(), 0, 0)),
					DataTypes.INT().notNull().bridgedTo(int.class)
				)),

			TestSpec.test("Deeply nested calls")
				.inputSchemas(
					TableSchema.builder()
						.field("f0", DataTypes.INT())
						.build()
				)
				.lookupFunction("func", new ScalarFunc())
				.select(call(new ScalarFunc(), call("func", 1, $("f0"))))
				.equalTo(
					new CallExpression(
						FunctionIdentifier.of("func"),
						new ScalarFunc(),
						Collections.singletonList(
							new CallExpression(
								new ScalarFunc(),
								Collections.singletonList(new CallExpression(
									FunctionIdentifier.of("func"),
									new ScalarFunc(),
									Arrays.asList(
										valueLiteral(1),
										new FieldReferenceExpression("f0", DataTypes.INT(), 0, 0)),
									DataTypes.INT().notNull().bridgedTo(int.class)
								)),
								DataTypes.INT().notNull().bridgedTo(int.class)
							)),
						DataTypes.INT().notNull().bridgedTo(int.class))
				)
		);
	}

	@Parameterized.Parameter
	public TestSpec testSpec;

	@Test
	public void testResolvingExpressions() {
		assertThat(
			testSpec.getResolver().resolve(Arrays.asList(testSpec.expressions)),
			equalTo(testSpec.expectedExpressions));
	}

	@FunctionHint(input = @DataTypeHint(inputGroup = InputGroup.ANY), isVarArgs = true, output = @DataTypeHint(value = "INTEGER NOT NULL", bridgedTo = int.class))
	public static class ScalarFunc extends ScalarFunction {
		public int eval(Object... any) {
			return 0;
		}

		@Override
		public int hashCode() {
			return 0;
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof ScalarFunc;
		}
	}

	private static class TestSpec {
		private final String description;
		private TableSchema[] schemas;
		private Expression[] expressions;
		private List<ResolvedExpression> expectedExpressions;
		private Map<String, FunctionDefinition> functions = new HashMap<>();

		private TestSpec(String description) {
			this.description = description;
		}

		public static TestSpec test(String description) {
			return new TestSpec(description);
		}

		public TestSpec inputSchemas(TableSchema... schemas) {
			this.schemas = schemas;
			return this;
		}

		public TestSpec lookupFunction(String name, FunctionDefinition functionDefinition) {
			functions.put(name, functionDefinition);
			return this;
		}

		public TestSpec select(Expression... expressions) {
			this.expressions = expressions;
			return this;
		}

		public TestSpec equalTo(ResolvedExpression... resolvedExpressions) {
			this.expectedExpressions = Arrays.asList(resolvedExpressions);
			return this;
		}

		public ExpressionResolver getResolver() {
			DataTypeFactory dataTypeFactory = new DataTypeFactory() {
				@Override
				public Optional<DataType> createDataType(String name) {
					final LogicalType parsedType = LogicalTypeParser.parse(
						name,
						Thread.currentThread().getContextClassLoader());
					return Optional.of(fromLogicalToDataType(parsedType));
				}

				@Override
				public Optional<DataType> createDataType(UnresolvedIdentifier identifier) {
					return Optional.empty();
				}

				@Override
				public <T> DataType createDataType(Class<T> clazz) {
					return null;
				}

				@Override
				public <T> DataType createRawDataType(Class<T> clazz) {
					return null;
				}
			};
			FunctionLookup functionLookup = new FunctionLookup() {
				@Override
				public Optional<Result> lookupFunction(UnresolvedIdentifier identifier) {
					return Optional.ofNullable(functions.get(identifier.getObjectName()))
						.map(func -> new Result(FunctionIdentifier.of(identifier.getObjectName()), func));
				}

				@Override
				public PlannerTypeInferenceUtil getPlannerTypeInferenceUtil() {
					return null;
				}
			};
			return ExpressionResolver.resolverFor(
				new TableConfig(),
				name -> Optional.empty(),
				functionLookup,
				dataTypeFactory,
				Arrays.stream(schemas)
					.map(schema -> (QueryOperation) new CatalogQueryOperation(ObjectIdentifier.of("", "", ""), schema))
					.toArray(QueryOperation[]::new)
			).build();
		}

		@Override
		public String toString() {
			return description;
		}
	}
}
