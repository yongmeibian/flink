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

package org.apache.flink.table.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.table.api.TableException;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

public final class TypeUtils {

	/**
	 * Returns field names for a given {@link TypeInformation}.
	 *
	 * @param inputType The TypeInformation extract the field names.
	 * @param <T> The type of the TypeInformation.
	 * @return An array holding the field names
	 */
	public static <T> String[] getFieldNames(TypeInformation<T> inputType) {
		validateType(inputType);

		final String[] fieldNames;
		if (inputType instanceof CompositeType) {
			fieldNames = ((CompositeType<T>) inputType).getFieldNames();
		} else {
			fieldNames = new String[]{"f0"};
		}

		if (Arrays.asList(fieldNames).contains("*")) {
			throw new TableException("Field name can not be '*'.");
		}

		return fieldNames;
	}

	/**
	 * Validate if class represented by the typeInfo is static and globally accessible
	 *
	 * @param typeInfo type to check
	 * @throws TableException if type does not meet these criteria
	 */
	public static <T> void validateType(TypeInformation<T> typeInfo) {
		Class<T> clazz = typeInfo.getTypeClass();
		if ((clazz.isMemberClass() && !Modifier.isStatic(clazz.getModifiers())) ||
			!Modifier.isPublic(clazz.getModifiers()) ||
			clazz.getCanonicalName() == null) {
			throw new TableException(
				String.format("Class '%s' described in type information '%s' must be static and globally accessible.",
					clazz,
					typeInfo));
		}
	}

	/**
	 * Returns field indexes for a given [[TypeInformation]].
	 *
	 * @param inputType The TypeInformation extract the field positions from.
	 * @return An array holding the field positions
	 */
	public static <T> int[] getFieldIndices(TypeInformation<T> inputType) {
		return IntStream.range(0, getFieldNames(inputType).length).toArray();
	}

	/**
	 * Returns field types for a given [[TypeInformation]].
	 *
	 * @param inputType The TypeInformation to extract field types from.
	 * @return An array holding the field types.
	 */
	public static <T> TypeInformation<?>[] getFieldTypes(TypeInformation<T> inputType) {
		validateType(inputType);

		if (inputType instanceof CompositeType) {
			return IntStream.range(0, inputType.getArity())
				.mapToObj(((CompositeType<T>) inputType)::getTypeAt)
				.toArray((IntFunction<TypeInformation<?>[]>) TypeInformation[]::new);
		} else {
			return new TypeInformation[]{inputType};
		}
	}

	private TypeUtils() {
	}
}
