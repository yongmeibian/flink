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

package org.apache.flink.table.expressions.rules;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;

/**
 * Contains instances of {@link ResolverRule}.
 */
@PublicEvolving
public final class ResolverRules {

	/**
	 * Rule that resolves flatten call. See {@link FlattenCallRule} for details.
	 */
	public static final ResolverRule FLATTEN_CALL = new FlattenCallRule();

	/**
	 * Resolves {@link UnresolvedReferenceExpression}. See {@link ReferenceResolverRule} for details.
	 */
	public static final ResolverRule FIELD_RESOLVE = new ReferenceResolverRule();

	/**
	 * Resolves call based on argument types. See {@link ResolveCallByArgumentsRule} for details.
	 */
	public static final ResolverRule RESOLVE_CALL = new ResolveCallByArgumentsRule();

	/**
	 * Concatenates over aggregations with corresponding over window. See {@link OverWindowResolverRule} for details.
	 */
	public static final ResolverRule OVER_WINDOWS = new OverWindowResolverRule();

	/**
	 * Resolves '*' expressions to corresponding fields of inputs. See {@link StarReferenceFlatteningRule} for details.
	 */
	public static final ResolverRule FLATTEN_STAR_REFERENCE = new StarReferenceFlatteningRule();

	/*
		NON DEFAULT RULES
	 */

	/**
	 * Used in sort operation. It assures expression is wrapped in ordering expression. See {@link WrapInOrderRule}
	 * for details.
	 */
	public static final ResolverRule WRAP_IN_ORDER = new WrapInOrderRule();

	private ResolverRules() {
	}
}
