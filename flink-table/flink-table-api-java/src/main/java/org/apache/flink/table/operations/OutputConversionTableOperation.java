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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Collections;
import java.util.List;

/**
 * Special internal kind of {@link DmlTableOperation} that allows converting a tree of {@link TableOperation}s
 * to a {@link org.apache.flink.streaming.api.transformations.StreamTransformation} of given {@link TypeInformation}.
 * This is used to convert a relational query into a datastream.
 * @param <T>
 */
@Internal
public class OutputConversionTableOperation<T> extends DmlTableOperation {

	private final TableOperation child;
	private final TypeInformation<T> type;
	private final boolean isUpsert;

	public OutputConversionTableOperation(TableOperation child, TypeInformation<T> type, boolean isUpsert) {
		this.child = child;
		this.type = type;
		this.isUpsert = isUpsert;
	}

	public boolean isUpsert() {
		return isUpsert;
	}

	public TypeInformation<T> getType() {
		return type;
	}

	@Override
	public List<TableOperation> getChildren() {
		return Collections.singletonList(child);
	}

	@Override
	public <R> R accept(TableOperationVisitor<R> visitor) {
		return visitor.visitOutputConversion(this);
	}
}
