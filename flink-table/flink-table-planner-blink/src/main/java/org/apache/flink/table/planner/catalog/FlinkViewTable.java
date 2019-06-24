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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;

import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.TranslatableTable;

import javax.annotation.Nullable;

import java.util.List;

/**
 * A bridge between a Flink's specific {@link CatalogView} and a Calcite's
 * {@link org.apache.calcite.plan.RelOptTable}. Equivalent to Calcite's
 * {@link org.apache.calcite.schema.impl.ViewTable}.
 */
public class FlinkViewTable extends FlinkPreparingTableBase {

	private final CatalogView view;
	private final List<String> schemaPath;

	/**
	 * Creates a {@link Prepare.AbstractPreparingTable} instance.
	 *
	 * @param relOptSchema The RelOptSchema that this table comes from
	 * @param rowType The table row type
	 * @param names The table qualified name
	 * @param statistic The table statistics
	 */
	public FlinkViewTable(
			@Nullable RelOptSchema relOptSchema,
			RelDataType rowType,
			Iterable<String> names,
			FlinkStatistic statistic,
			CatalogView view,
			List<String> schemaPath) {
		super(relOptSchema, rowType, names, statistic);
		this.view = view;
		this.schemaPath = schemaPath;
	}

	@Override
	public RelNode toRel(ToRelContext context) {
		return expandView(context, rowType, view.getExpandedQuery()).rel;
	}

	private RelRoot expandView(
		RelOptTable.ToRelContext context,
		RelDataType rowType, String queryString) {
		try {
			final RelRoot root =
				context.expandView(rowType, queryString, schemaPath, names);
			final RelNode rel = RelOptUtil.createCastRel(root.rel, rowType, true);
			// Expand any views
			final RelNode rel2 = rel.accept(
				new RelShuttleImpl() {
					@Override
					public RelNode visit(TableScan scan) {
						final RelOptTable table = scan.getTable();
						final TranslatableTable translatableTable =
							table.unwrap(TranslatableTable.class);
						if (translatableTable != null) {
							return translatableTable.toRel(context, table);
						}
						return super.visit(scan);
					}
				});
			return root.withRel(rel2);
		} catch (Exception e) {
			throw new RuntimeException("Error while parsing view definition: "
				+ queryString, e);
		}
	}
}
