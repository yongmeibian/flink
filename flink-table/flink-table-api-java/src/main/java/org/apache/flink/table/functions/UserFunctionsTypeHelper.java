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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

@Internal
public class UserFunctionsTypeHelper {

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param aggregateFunction The AggregateFunction for which the accumulator type is inferred.
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T, ACC> TypeInformation<T> getReturnTypeOfAggregateFunction(
			UserDefinedAggregateFunction<T, ACC> aggregateFunction) {
		return getReturnTypeOfAggregateFunction(aggregateFunction, null);
	}

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param aggregateFunction The AggregateFunction for which the accumulator type is inferred.
	 * @param scalaType The implicitly inferred type of the accumulator type.
	 *
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T, ACC> TypeInformation<T> getReturnTypeOfAggregateFunction(
		UserDefinedAggregateFunction<T, ACC> aggregateFunction,
		TypeInformation<T> scalaType) {

		TypeInformation<T> userProvidedType = aggregateFunction.getResultType();
		if (userProvidedType != null) {
			return userProvidedType;
		} else if (scalaType != null) {
			return scalaType;
		} else {
			return TypeExtractor.createTypeInfo(
				aggregateFunction,
				UserDefinedAggregateFunction.class,
				aggregateFunction.getClass(),
				0);
		}
	}

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param aggregateFunction The AggregateFunction for which the accumulator type is inferred.
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T, ACC> TypeInformation<ACC> getAccumulatorTypeOfAggregateFunction(
		UserDefinedAggregateFunction<T, ACC> aggregateFunction) {
		return getAccumulatorTypeOfAggregateFunction(aggregateFunction, null);
	}

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param aggregateFunction The AggregateFunction for which the accumulator type is inferred.
	 * @param scalaType The implicitly inferred type of the accumulator type.
	 *
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T, ACC> TypeInformation<ACC> getAccumulatorTypeOfAggregateFunction(
		UserDefinedAggregateFunction<T, ACC> aggregateFunction,
		TypeInformation<ACC> scalaType) {

		TypeInformation<ACC> userProvidedType = aggregateFunction.getAccumulatorType();
		if (userProvidedType != null) {
			return userProvidedType;
		} else if (scalaType != null) {
			return scalaType;
		} else {
			return TypeExtractor.createTypeInfo(
				aggregateFunction,
				UserDefinedAggregateFunction.class,
				aggregateFunction.getClass(),
				1);
		}
	}

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param tableFunction The TableFunction for which the accumulator type is inferred.
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T> TypeInformation<T> getReturnTypeOfTableFunction(TableFunction<T> tableFunction) {
		return getReturnTypeOfTableFunction(tableFunction, null);
	}

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param tableFunction The TableFunction for which the accumulator type is inferred.
	 * @param scalaType The implicitly inferred type of the accumulator type.
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T> TypeInformation<T> getReturnTypeOfTableFunction(
		TableFunction<T> tableFunction,
		TypeInformation<T> scalaType) {

		TypeInformation<T> userProvidedType = tableFunction.getResultType();
		if (userProvidedType != null) {
			return userProvidedType;
		} else if (scalaType != null) {
			return scalaType;
		} else {
			return TypeExtractor.createTypeInfo(
				tableFunction,
				TableFunction.class,
				tableFunction.getClass(),
				0);
		}
	}

	private UserFunctionsTypeHelper() {
	}
}
