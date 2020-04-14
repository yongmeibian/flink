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

package org.apache.flink.table.operations.utils;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.operations.ValuesQueryOperation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.table.utils.FunctionLookupMock;
import org.apache.flink.types.Row;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.row;
import static org.apache.flink.table.expressions.ApiExpressionUtils.typeLiteral;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link OperationTreeBuilder#values}.
 */
@RunWith(Parameterized.class)
public class ValuesOperationTreeBuilderTest {

	@Parameterized.Parameters(name = "{0}")
	public static Collection<TestSpec> parameters() {
		return asList(
			TestSpec.test("Flattening row constructor")
			.values(row(1, "ABC"), row(2, "EFG"))
			.equalTo(new ValuesQueryOperation(
				asList(
					asList(valueLiteral(1), valueLiteral("ABC")),
					asList(valueLiteral(2), valueLiteral("EFG"))
				),
				TableSchema.builder()
					.field("f0", DataTypes.INT().notNull())
					.field("f1", DataTypes.CHAR(3).notNull())
					.build()
			)),

			TestSpec.test("Finding common type")
				.values(row(1L, "ABC"), row(3.1f, "DEFG"))
				.equalTo(new ValuesQueryOperation(
					asList(
						asList(
							cast(valueLiteral(1L), DataTypes.FLOAT().notNull()),
							valueLiteral("ABC", DataTypes.VARCHAR(4).notNull())),
						asList(
							valueLiteral(3.1f),
							valueLiteral("DEFG", DataTypes.VARCHAR(4).notNull())
						)
					),
					TableSchema.builder()
						.field("f0", DataTypes.FLOAT().notNull())
						.field("f1", DataTypes.VARCHAR(4).notNull())
						.build()
				)),

			TestSpec.test("Explicit common type")
				.values(
					DataTypes.ROW(
						DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
						DataTypes.FIELD("name", DataTypes.STRING())),
					row(1L, "ABC"),
					row(3.1f, "DEFG")
				)
				.equalTo(new ValuesQueryOperation(
					asList(
						asList(
							cast(valueLiteral(1L), DataTypes.DECIMAL(10, 2)),
							cast(valueLiteral("ABC", DataTypes.STRING().notNull()), DataTypes.STRING())
						),
						asList(
							cast(valueLiteral(3.1f), DataTypes.DECIMAL(10, 2)),
							cast(valueLiteral("DEFG", DataTypes.STRING().notNull()), DataTypes.STRING())
						)
					),
					TableSchema.builder()
						.field("id", DataTypes.DECIMAL(10, 2))
						.field("name", DataTypes.STRING())
						.build()
				)),

			TestSpec.test("Explicit common type for nested rows")
				.values(
					DataTypes.ROW(
						DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
						DataTypes.FIELD(
							"details",
							DataTypes.ROW(
								DataTypes.FIELD("name", DataTypes.STRING()),
								DataTypes.FIELD("amount", DataTypes.DECIMAL(10, 2))
							))),
					row(1L, row("ABC", 3)),
					row(3.1f, row("DEFG", new BigDecimal("12345")))
				)
				.equalTo(new ValuesQueryOperation(
					asList(
						asList(
							cast(valueLiteral(1L), DataTypes.DECIMAL(10, 2)),
							rowCtor(
								DataTypes.ROW(
									DataTypes.FIELD("name", DataTypes.STRING()),
									DataTypes.FIELD("amount", DataTypes.DECIMAL(10, 2))),
								cast(valueLiteral("ABC", DataTypes.STRING().notNull()), DataTypes.STRING()),
								cast(valueLiteral(3), DataTypes.DECIMAL(10, 2)))
						),
						asList(
							cast(valueLiteral(3.1f), DataTypes.DECIMAL(10, 2)),
							rowCtor(
								DataTypes.ROW(
									DataTypes.FIELD("name", DataTypes.STRING()),
									DataTypes.FIELD("amount", DataTypes.DECIMAL(10, 2))),
								cast(valueLiteral("DEFG", DataTypes.STRING().notNull()), DataTypes.STRING()),
								cast(
									valueLiteral(new BigDecimal("12345"), DataTypes.DECIMAL(10, 2).notNull()),
									DataTypes.DECIMAL(10, 2)))
							)
					),
					TableSchema.builder()
						.field("id", DataTypes.DECIMAL(10, 2))
						.field("details", DataTypes.ROW(
							DataTypes.FIELD("name", DataTypes.STRING()),
							DataTypes.FIELD("amount", DataTypes.DECIMAL(10, 2))
						))
						.build()
				)),

			TestSpec.test("Finding common type for nested rows")
				.values(
					row(1L, row(1L, "ABC")),
					row(3.1f, row(3.1f, "DEFG"))
				)
				.equalTo(
					new ValuesQueryOperation(
						asList(
							asList(
								cast(valueLiteral(1L), DataTypes.FLOAT().notNull()),
								rowCtor(
									DataTypes.ROW(
										DataTypes.FIELD("f0", DataTypes.FLOAT().notNull()),
										DataTypes.FIELD("f1", DataTypes.VARCHAR(4).notNull())),
									cast(valueLiteral(1L), DataTypes.FLOAT().notNull()),
									valueLiteral("ABC", DataTypes.VARCHAR(4).notNull())
								)
							),
							asList(
								valueLiteral(3.1f),
								rowCtor(
									DataTypes.ROW(
										DataTypes.FIELD("f0", DataTypes.FLOAT().notNull()),
										DataTypes.FIELD("f1", DataTypes.VARCHAR(4).notNull())),
									valueLiteral(3.1f, DataTypes.FLOAT().notNull()),
									valueLiteral("DEFG", DataTypes.VARCHAR(4).notNull())
								)
							)
						),
						TableSchema.builder()
							.field("f0", DataTypes.FLOAT().notNull())
							.field(
								"f1",
								DataTypes.ROW(
									DataTypes.FIELD("f0", DataTypes.FLOAT().notNull()),
									DataTypes.FIELD("f1", DataTypes.VARCHAR(4).notNull())))
							.build()
					)),

			TestSpec.test("Finding common type. Insert cast for calls")
				.values(call(new IntScalarFunction()), row(3.1f))
				.equalTo(new ValuesQueryOperation(
					asList(
						singletonList(
							cast(
								new CallExpression(new IntScalarFunction(), Collections.emptyList(), DataTypes.INT()),
								DataTypes.FLOAT()
							)),
						singletonList(
							cast(valueLiteral(3.1f), DataTypes.FLOAT())
						)
					),
					TableSchema.builder()
						.field("f0", DataTypes.FLOAT())
						.build()
				)),

			TestSpec.test("Row in a function result is not flattened")
				.values(call(new RowScalarFunction()))
				.equalTo(new ValuesQueryOperation(
					singletonList(
						singletonList(new CallExpression(
							new RowScalarFunction(),
							Collections.emptyList(),
							DataTypes.ROW(
								DataTypes.FIELD("f0", DataTypes.INT()),
								DataTypes.FIELD("f1", DataTypes.STRING()))))
					),
					TableSchema.builder()
						.field("f0", DataTypes.ROW(
							DataTypes.FIELD("f0", DataTypes.INT()),
							DataTypes.FIELD("f1", DataTypes.STRING())))
						.build()
				))
		);
	}

	/**
	 * A simple function that returns a ROW.
	 */
	@FunctionHint(
		output = @DataTypeHint("ROW<f0 INT, f1 STRING>")
	)
	public static class RowScalarFunction extends ScalarFunction {
		public Row eval() {
			return Row.of(1, "ABC");
		}

		@Override
		public int hashCode() {
			return 0;
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof RowScalarFunction;
		}
	}

	/**
	 * A simple function that returns an int.
	 */
	public static class IntScalarFunction extends ScalarFunction {
		public Integer eval() {
			return 1;
		}

		@Override
		public int hashCode() {
			return 0;
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof IntScalarFunction;
		}
	}

	@Parameterized.Parameter
	public TestSpec testSpec;

	@Test
	public void testValues() {

		ValuesQueryOperation operation;
		if (testSpec.expectedRowType != null) {
			operation = (ValuesQueryOperation) testSpec.getTreeBuilder()
				.values(testSpec.expectedRowType, testSpec.expressions);
		} else {
			operation = (ValuesQueryOperation) testSpec.getTreeBuilder().values(testSpec.expressions);
		}
		assertThat(operation.getTableSchema(), equalTo(testSpec.queryOperation.getTableSchema()));
		assertThat(operation.getValues(), equalTo(testSpec.queryOperation.getValues()));
	}

	private static ResolvedExpression rowCtor(DataType dataType, ResolvedExpression... expression) {
		return new CallExpression(
			FunctionIdentifier.of("row"),
			BuiltInFunctionDefinitions.ROW,
			Arrays.asList(expression),
			dataType);
	}

	private static ResolvedExpression cast(ResolvedExpression expression, DataType dataType) {
		return new CallExpression(
			FunctionIdentifier.of("cast"),
			BuiltInFunctionDefinitions.CAST,
			Arrays.asList(expression, typeLiteral(dataType)),
			dataType);
	}

	private static class TestSpec {
		private final String description;
		private List<Expression> expressions;
		private ValuesQueryOperation queryOperation;
		private @Nullable DataType expectedRowType;

		private TestSpec(String description) {
			this.description = description;
		}

		public static TestSpec test(String description) {
			return new TestSpec(description);
		}

		public TestSpec values(Expression... expressions) {
			this.expressions = Arrays.asList(expressions);
			return this;
		}

		public TestSpec values(DataType rowType, Expression... expressions) {
			this.expressions = Arrays.asList(expressions);
			this.expectedRowType = rowType;
			return this;
		}

		public TestSpec equalTo(ValuesQueryOperation queryOperation) {
			this.queryOperation = queryOperation;
			return this;
		}

		public OperationTreeBuilder getTreeBuilder() {
			return OperationTreeBuilder.create(
				new TableConfig(),
				new FunctionLookupMock(Collections.emptyMap()),
				new DataTypeFactoryMock(),
				name -> Optional.empty(), // do not support
				true
			);
		}

		@Override
		public String toString() {
			return description;
		}
	}
}
