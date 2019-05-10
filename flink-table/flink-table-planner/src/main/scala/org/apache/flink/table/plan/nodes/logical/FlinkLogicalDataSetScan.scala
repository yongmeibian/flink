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

package org.apache.flink.table.plan.nodes.logical

import java.util

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{AbstractRelNode, RelNode, RelWriter}
import org.apache.flink.api.java.DataSet

/**
  * Flink RelNode which matches along with DataStreamSource.
  * It ensures that types without deterministic field order (e.g. POJOs) are not part of
  * the plan translation.
  */
class FlinkLogicalDataSetScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    val dataSet: DataSet[Any],
    val fieldIdxs: Array[Int],
    val schema: RelDataType)
  extends AbstractRelNode(cluster, traitSet)
  with FlinkLogicalRel {


  override def deriveRowType(): RelDataType = schema

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalDataSetScan(cluster, traitSet, dataSet, fieldIdxs, schema)
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * estimateRowSize(getRowType))
  }

  override def explainTerms(pw: RelWriter): RelWriter = super.explainTerms(pw)
    .item("dataset", dataSet)
}
