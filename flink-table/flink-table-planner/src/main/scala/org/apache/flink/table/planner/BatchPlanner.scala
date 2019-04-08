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

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.tools.RuleSet
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.codegen.RowConverterGenerator
import org.apache.flink.table.explain.PlanJsonParser
import org.apache.flink.table.operations.TableOperation
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetRel
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.runtime.MapRunner
import org.apache.flink.table.sinks._
import org.apache.flink.types.Row

/**
  * The abstract base class for batch TableEnvironments.
  *
  * A TableEnvironment can be used to:
  * - specify a SQL query on registered tables to obtain a [[TableOperation]]
  * - explain the AST and execution plan of a [[TableOperation]]
  *
  * @param execEnv The [[ExecutionEnvironment]] which is wrapped in this [[BatchPlanner()]].
  * @param config The [[TableConfig]] of this [[BatchPlanner()]].
  */
class BatchPlanner(
    private[flink] val execEnv: ExecutionEnvironment,
    config: TableConfig,
    functionCatalog: FunctionCatalog)
  extends PlannerBase(config, functionCatalog) {

  /**
    * Writes a [[Table]] to a [[TableSink]].
    *
    * Internally, the [[Table]] is translated into a [[DataSet]] and handed over to the
    * [[TableSink]] to write it.
    *
    * @param table The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @param queryConfig The configuration for the query to generate.
    * @tparam T The expected type of the [[DataSet]] which represents the [[Table]].
    */
  override private[flink] def writeToSink[T](
      table: TableOperation,
      sink: TableSink[T],
      queryConfig: QueryConfig): Unit = {

    // Check the query configuration to be a batch one.
    val batchQueryConfig = queryConfig match {
      case batchConfig: BatchQueryConfig => batchConfig
      case _ =>
        throw new TableException("BatchQueryConfig required to configure batch query.")
    }

    sink match {
      case batchSink: BatchTableSink[T] =>
        val outputType = sink.getOutputType
        // translate the Table into a DataSet and provide the type that the TableSink expects.
        val result: DataSet[T] = translate(table, batchQueryConfig)(outputType)
        // Give the DataSet to the TableSink to emit it.
        batchSink.emitDataSet(result)
      case _ =>
        throw new TableException("BatchTableSink required to emit batch Table.")
    }
  }

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[TableOperation]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    */
  def explain(table: TableOperation, queryConfig: QueryConfig): String =
    explain(table: TableOperation,
      extended = false,
      queryConfig)

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[TableOperation]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    * @param extended Flag to include detailed optimizer estimates.
    */
  private[flink] def explain(table: TableOperation, extended: Boolean, queryConfig: QueryConfig): String = {
    val ast = relBuilder.tableOperation(table).build()
    val optimizedPlan = optimize(ast)
    val dataSet = translate[Row](optimizedPlan, ast.getRowType, queryConfig) (
      new GenericTypeInfo (classOf[Row]))
    dataSet.output(new DiscardingOutputFormat[Row])
    val env = dataSet.getExecutionEnvironment
    val jasonSqlPlan = env.getExecutionPlan
    val sqlPlan = PlanJsonParser.getSqlExecutionPlan(jasonSqlPlan, extended)

    s"== Abstract Syntax Tree ==" +
      System.lineSeparator +
      s"${RelOptUtil.toString(ast)}" +
      System.lineSeparator +
      s"== Optimized Logical Plan ==" +
      System.lineSeparator +
      s"${RelOptUtil.toString(optimizedPlan)}" +
      System.lineSeparator +
      s"== Physical Execution Plan ==" +
      System.lineSeparator +
      s"$sqlPlan"
  }

  /**
    * Creates a final converter that maps the internal row type to external type.
    *
    * @param physicalTypeInfo the input of the sink
    * @param schema the input schema with correct field names (esp. for POJO field mapping)
    * @param requestedTypeInfo the output type of the sink
    * @param functionName name of the map function. Must not be unique but has to be a
    *                     valid Java class identifier.
    */
  protected def getConversionMapper[IN, OUT](
      physicalTypeInfo: TypeInformation[IN],
      schema: RowSchema,
      requestedTypeInfo: TypeInformation[OUT],
      functionName: String)
    : Option[MapFunction[IN, OUT]] = {

    val converterFunction = RowConverterGenerator.generateRowConverterFunction[OUT](
      physicalTypeInfo.asInstanceOf[TypeInformation[Row]],
      schema,
      requestedTypeInfo,
      functionName,
      config
    )

    // add a runner if we need conversion
    converterFunction.map { func =>
      new MapRunner[IN, OUT](
          func.name,
          func.code,
          func.returnType)
    }
  }

  /**
    * Returns the built-in normalization rules that are defined by the environment.
    */
  protected def getBuiltInNormRuleSet: RuleSet = FlinkRuleSets.DATASET_NORM_RULES

  /**
    * Returns the built-in optimization rules that are defined by the environment.
    */
  protected def getBuiltInPhysicalOptRuleSet: RuleSet = FlinkRuleSets.DATASET_OPT_RULES

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The original [[RelNode]] tree
    * @return The optimized [[RelNode]] tree
    */
  private[flink] def optimize(relNode: RelNode): RelNode = {
    val convSubQueryPlan = optimizeConvertSubQueries(relNode)
    val expandedPlan = optimizeExpandPlan(convSubQueryPlan)
    val decorPlan = RelDecorrelator.decorrelateQuery(expandedPlan)
    val normalizedPlan = optimizeNormalizeLogicalPlan(decorPlan)
    val logicalPlan = optimizeLogicalPlan(normalizedPlan)
    optimizePhysicalPlan(logicalPlan, FlinkConventions.DATASET)
  }

  /**
    * Translates a [[TableOperation]] into a [[DataSet]].
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataSet]] operators.
    *
    * @param table The root node of the relational expression tree.
    * @param queryConfig The configuration for the query to generate.
    * @param tpe   The [[TypeInformation]] of the resulting [[DataSet]].
    * @tparam A The type of the resulting [[DataSet]].
    * @return The [[DataSet]] that corresponds to the translated [[TableOperation]].
    */
  protected def translate[A](
      table: TableOperation,
      queryConfig: BatchQueryConfig)(implicit tpe: TypeInformation[A]): DataSet[A] = {
    val relNode = relBuilder.tableOperation(table).build()
    val dataSetPlan = optimize(relNode)
    translate(dataSetPlan, relNode.getRowType, queryConfig)
  }

  /**
    * Translates a logical [[RelNode]] into a [[DataSet]]. Converts to target type if necessary.
    *
    * @param logicalPlan The root node of the relational expression tree.
    * @param logicalType The row type of the result. Since the logicalPlan can lose the
    *                    field naming during optimization we pass the row type separately.
    * @param queryConfig The configuration for the query to generate.
    * @param tpe         The [[TypeInformation]] of the resulting [[DataSet]].
    * @tparam A The type of the resulting [[DataSet]].
    * @return The [[DataSet]] that corresponds to the translated [[TableOperation]].
    */
  protected def translate[A](
      logicalPlan: RelNode,
      logicalType: RelDataType,
      queryConfig: QueryConfig)(implicit tpe: TypeInformation[A]): DataSet[A] = {
    TableEnvironment.validateType(tpe)

    logicalPlan match {
      case node: DataSetRel =>
        val plan = node.translateToPlan(this, queryConfig.asInstanceOf[BatchQueryConfig])
        val conversion =
          getConversionMapper(
            plan.getType,
            new RowSchema(logicalType),
            tpe,
            "DataSetSinkConversion")
        conversion match {
          case None => plan.asInstanceOf[DataSet[A]] // no conversion necessary
          case Some(mapFunction: MapFunction[Row, A]) =>
            plan.map(mapFunction)
              .returns(tpe)
              .name(s"to: ${tpe.getTypeClass.getSimpleName}")
              .asInstanceOf[DataSet[A]]
        }

      case _ =>
        throw new TableException("Cannot generate DataSet due to an invalid logical plan. " +
          "This is a bug and should not happen. Please file an issue.")
    }
  }
}
