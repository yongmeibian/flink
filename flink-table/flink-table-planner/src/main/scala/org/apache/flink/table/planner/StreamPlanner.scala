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

package org.apache.flink.table.planner
import java.util

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataTypeField, RelDataTypeFieldImpl, RelRecordType}
import org.apache.calcite.sql.{SqlIdentifier, SqlInsert, SqlKind, SqlNode}
import org.apache.calcite.tools.FrameworkConfig
import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{StreamQueryConfig, TableException}
import org.apache.flink.table.calcite.{FlinkPlannerImpl, FlinkRelBuilder}
import org.apache.flink.table.operations.{CatalogSinkTableOperation, Operation, PlannerTableOperation, TableOperation}
import org.apache.flink.table.plan.nodes.datastream.DataStreamRel
import org.apache.flink.table.runtime.types.CRow

import scala.collection.JavaConverters._

class StreamPlanner extends Planner{
  override def parse(stmt: String): Operation = {
    val planner = getFlinkPlanner
    // parse the sql query
    val parsed = planner.parse(stmt)

    parsed match {
      case insert: SqlInsert =>
        // validate the SQL query
        val query = insert.getSource

        // get name of sink table
        val targetTablePath = insert.getTargetTable.asInstanceOf[SqlIdentifier].names

        new CatalogSinkTableOperation(targetTablePath, toRel(planner, insert.getSource))
      case node if node.getKind.belongsTo(SqlKind.QUERY) =>
        toRel(planner, parsed)
      case _ =>
        throw new TableException(
          "Unsupported SQL query! parse() only accepts SQL queries of type " +
            "SELECT, UNION, INTERSECT, EXCEPT, VALUES, ORDER_BY or INSERT.")
    }
  }

  private def toRel(
      planner: FlinkPlannerImpl,
      parsed: SqlNode)
    : PlannerTableOperation = {
    // validate the sql query
    val validated = planner.validate(parsed)
    // transform to a relational tree
    val relational = planner.rel(validated)
    new PlannerTableOperation(relational.rel)
  }

  override def translate(
      tableOperations: util.List[TableOperation],
      queryConfig: StreamQueryConfig)
    : util.List[StreamTransformation[_]] = {
    tableOperations.asScala.map(translate(_, queryConfig)).asJava
  }

  private def translate(
      tableOperation: TableOperation,
      queryConfig: StreamQueryConfig)
    : StreamTransformation[_] = {
    tableOperation match {
      case s :

      case _ => translateToCRow(getRelBuilder.tableOperation(tableOperation).build(), queryConfig)
        .getTransformation
    }
  }

  override def explain(
    tableOperations: util.List[TableOperation],
    queryConfig: StreamQueryConfig): String = ???

  override def getCompletionHints(
      statement: String,
      position: Int)
    : Array[String] = {
    val planner = getFlinkPlanner
    planner.getCompletionHints(statement, position)
  }


  /** Returns the Calcite [[FrameworkConfig]] of this TableEnvironment. */
  @VisibleForTesting
  private[flink] def getFlinkPlanner: FlinkPlannerImpl = ???

  /** Returns the [[FlinkRelBuilder]] of this TableEnvironment. */
  private[flink] def getRelBuilder: FlinkRelBuilder = ???

  /* Implementation */

  /**
    * Translates a logical [[RelNode]] plan into a [[DataStream]] of type [[CRow]].
    *
    * @param logicalPlan The logical plan to translate.
    * @param queryConfig  The configuration for the query to generate.
    * @return The [[DataStream]] of type [[CRow]].
    */
  protected def translateToCRow(
    logicalPlan: RelNode,
    queryConfig: StreamQueryConfig): DataStream[CRow] = {

    logicalPlan match {
      case node: DataStreamRel =>
        node.translateToPlan(this, queryConfig)
      case _ =>
        throw new TableException("Cannot generate DataStream due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.")
    }
  }

  /**
    * Returns the record type of the optimized plan with field names of the logical plan.
    */
  private def getResultType(originRelNode: RelNode, optimizedPlan: RelNode): RelRecordType = {
    // zip original field names with optimized field types
    val fieldTypes = originRelNode.getRowType.getFieldList.asScala
      .zip(optimizedPlan.getRowType.getFieldList.asScala)
      // get name of original plan and type of optimized plan
      .map(x => (x._1.getName, x._2.getType))
      // add field indexes
      .zipWithIndex
      // build new field types
      .map(x => new RelDataTypeFieldImpl(x._1._1, x._2, x._1._2))

    // build a record type from list of field types
    new RelRecordType(
      fieldTypes.toList.asInstanceOf[List[RelDataTypeField]].asJava)
  }
}
