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

package org.apache.flink.table.plan;

import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionResolver;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.NamedExpression;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.PlannerExpressionConverter;
import org.apache.flink.table.expressions.UnresolvedAlias;
import org.apache.flink.table.plan.logical.Join;
import org.apache.flink.table.plan.logical.LogicalNode;
import org.apache.flink.table.plan.logical.LogicalOverWindow;
import org.apache.flink.table.plan.logical.Project;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import scala.Option;
import scala.Some;

import static org.apache.flink.table.expressions.ExpressionResolver.resolverFor;

public class OperationTreeBuilder {

	private final ExpressionVisitor<PlannerExpression> expressionBridge = PlannerExpressionConverter.INSTANCE();

	public Join join(
			LogicalNode left,
			LogicalNode right,
			JoinType joinType,
			Expression condition,
			boolean correlated) {

		ExpressionResolver resolver = resolverFor(left, right).build();
		PlannerExpression plannerExpression = resolver.resolve(condition).get(0).accept(expressionBridge);

		return new Join(left, right, joinType, new Some<>(plannerExpression), correlated);
	}

	public Join join(
			LogicalNode left,
			LogicalNode right,
			JoinType joinType,
			boolean correlated) {
		return new Join(left, right, joinType, Option.empty(), correlated);
	}

	public Project project(
			Iterable<Expression> projectList,
			LogicalNode child) {
		return projectInternal(projectList, child, false, Collections.emptyList());
	}

	public Project projectWithOverWindows(
			Iterable<Expression> projectList,
			LogicalNode child,
			List<LogicalOverWindow> overWindows) {
		return projectInternal(projectList, child, true, overWindows);
	}

	public Project projectWithExplicitAlias(
			Iterable<Expression> projectList,
			LogicalNode child) {
		return projectInternal(projectList, child, true, Collections.emptyList());
	}

	private Project projectInternal(
			Iterable<Expression> projectList,
			LogicalNode child,
			boolean explicitAlias,
			List<LogicalOverWindow> overWindows) {
		ExpressionResolver resolver = resolverFor(child).withOverWindows(overWindows).build();
		List<NamedExpression> projections = StreamSupport.stream(projectList.spliterator(), false)
			.flatMap(expr -> resolver.resolve(expr).stream().map(resolvedExpr -> resolvedExpr.accept(expressionBridge)))
			.map(UnresolvedAlias::new)
			.collect(Collectors.toList());

		return new Project(projections, child, explicitAlias);
	}
}
