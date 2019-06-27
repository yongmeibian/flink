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

package org.apache.flink.table.types.inference.validators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeValidator;
import org.apache.flink.table.types.inference.Signature;

import java.util.Collections;
import java.util.List;

/**
 * Validator that verifies only the argument count. Accepts any data type.
 */
@Internal
public class AnyTypeValidator implements InputTypeValidator {
	private final ArgumentCount argumentCount;
	private final int numberOfArguments;

	public AnyTypeValidator(int argumentCount) {
		numberOfArguments = argumentCount;
		this.argumentCount = new ExactArgumentCount(argumentCount);
	}

	@Override
	public ArgumentCount getArgumentCount() {
		return argumentCount;
	}

	@Override
	public boolean validate(CallContext callContext, boolean throwOnFailure) {
		return true;
	}

	@Override
	public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
		Signature signature = Signature.newInstance();
		for (int i = 0; i < numberOfArguments; i++) {
			signature.arg("ANY");
		}

		return Collections.singletonList(signature);
	}
}
