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
import _root_.java.lang.{Boolean => JBool}
import _root_.java.util.{List => JList}
import java.util

import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.{SqlIdentifier, SqlInsert, SqlKind, SqlNode}
import org.apache.calcite.tools.FrameworkConfig
import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api._
import org.apache.flink.table.calcite.{CalciteConfig, FlinkPlannerImpl, FlinkRelBuilder, FlinkTypeFactory}
import org.apache.flink.table.catalog._
import org.apache.flink.table.explain.PlanJsonParser
import org.apache.flink.table.expressions.{ExpressionBridge, PlannerExpression, PlannerExpressionConverter}
import org.apache.flink.table.factories.{TableFactoryService, TableFactoryUtil, TableSinkFactory}
import org.apache.flink.table.operations.OutputConversionModifyOperation.UpdateMode
import org.apache.flink.table.operations._
import org.apache.flink.table.plan.nodes.datastream.DataStreamRel
import org.apache.flink.table.plan.util.UpdatingPlanChecker
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.sinks.{AppendStreamTableSink, RetractStreamTableSink, TableSink, UpsertStreamTableSink}
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.util.JavaScalaConversionUtil

import _root_.scala.collection.JavaConverters._

class StreamPlanner(
  execEnv: StreamExecutionEnvironment,
  config: TableConfig,
  functionCatalog: FunctionCatalog,
  catalogManager: CatalogManager) extends Planner{

  private val internalSchema: CalciteSchema =
    asRootSchema(new CatalogManagerCalciteSchema(catalogManager, false))

  // temporary bridge between API and planner
  private val expressionBridge: ExpressionBridge[PlannerExpression] =
    new ExpressionBridge[PlannerExpression](functionCatalog, PlannerExpressionConverter.INSTANCE)

  private val planningConfigurationBuilder: PlanningConfigurationBuilder =
    new PlanningConfigurationBuilder(
      config,
      functionCatalog,
      internalSchema,
      expressionBridge)

  @VisibleForTesting
  private[flink] val optimizer: StreamOptimizer = new StreamOptimizer(
    () => config.getPlannerConfig
      .unwrap(classOf[CalciteConfig])
      .orElse(CalciteConfig.DEFAULT),
    planningConfigurationBuilder)

  override def parse(stmt: String): JList[Operation] = {
    val planner = getFlinkPlanner
    // parse the sql query
    val parsed = planner.parse(stmt)

    parsed match {
      case insert: SqlInsert =>
        val targetColumnList = insert.getTargetColumnList
        if (targetColumnList != null && insert.getTargetColumnList.size() != 0) {
          throw new ValidationException("Partial inserts are not supported")
        }

        // get name of sink table
        val targetTablePath = insert.getTargetTable.asInstanceOf[SqlIdentifier].names

        List(new CatalogSinkModifyOperation(targetTablePath, toRel(planner, insert.getSource))
          .asInstanceOf[Operation]).asJava
      case node if node.getKind.belongsTo(SqlKind.QUERY) =>
        List(toRel(planner, parsed).asInstanceOf[Operation]).asJava
      case _ =>
        throw new TableException(
          "Unsupported SQL query! parse() only accepts SQL queries of type " +
            "SELECT, UNION, INTERSECT, EXCEPT, VALUES, ORDER_BY or INSERT.")
    }
  }

  private def toRel(
      planner: FlinkPlannerImpl,
      parsed: SqlNode)
    : PlannerQueryOperation = {
    // validate the sql query
    val validated = planner.validate(parsed)
    // transform to a relational tree
    val relational = planner.rel(validated)
    new PlannerQueryOperation(relational.rel)
  }

  override def translate(tableOperations: util.List[ModifyOperation])
    : util.List[StreamTransformation[_]] = {
    tableOperations.asScala.map(translate).asJava
  }

  private def translate(tableOperation: ModifyOperation)
    : StreamTransformation[_] = {
    tableOperation match {
      case s : UnregisteredSinkModifyOperation[_] =>
        writeToSink(s.getChild, s.getSink, unwrapQueryConfig).getTransformation

      case catalogSink: CatalogSinkModifyOperation =>
        getTableSink(catalogSink.getTablePath)
          .map(sink => {
            val query = catalogSink.getChild
            // validate schema of source table and table sink
            val srcFieldTypes = query.getTableSchema.getFieldTypes
            val sinkFieldTypes = sink.getTableSchema.getFieldTypes

            if (srcFieldTypes.length != sinkFieldTypes.length ||
              srcFieldTypes.zip(sinkFieldTypes).exists { case (srcF, snkF) => srcF != snkF }) {

              val srcFieldNames = query.getTableSchema.getFieldNames
              val sinkFieldNames = sink.getTableSchema.getFieldNames

              // format table and table sink schema strings
              val srcSchema = srcFieldNames.zip(srcFieldTypes)
                .map { case (n, t) => s"$n: ${t.getTypeClass.getSimpleName}" }
                .mkString("[", ", ", "]")
              val sinkSchema = sinkFieldNames.zip(sinkFieldTypes)
                .map { case (n, t) => s"$n: ${t.getTypeClass.getSimpleName}" }
                .mkString("[", ", ", "]")

              throw new ValidationException(
                s"Field types of query result and registered TableSink " +
                  s"${catalogSink.getTablePath} do not match.\n" +
                  s"Query result schema: $srcSchema\n" +
                  s"TableSink schema:    $sinkSchema")
            }
            writeToSink(catalogSink.getChild, sink, unwrapQueryConfig).getTransformation
          }) match {
          case Some(t) => t
          case None => throw new TableException(s"Sink ${catalogSink.getTablePath} does not exists")
        }

      case outputConversion: OutputConversionModifyOperation =>
        val (isRetract, withChangeFlag) = outputConversion.getUpdateMode match {
          case UpdateMode.RETRACT => (true, true)
          case UpdateMode.APPEND => (false, false)
          case UpdateMode.UPSERT => (false, true)
        }

        translateToType(
          tableOperation.getChild,
          unwrapQueryConfig,
          isRetract,
          withChangeFlag,
          TypeConversions.fromDataTypeToLegacyInfo(outputConversion.getType))
          .getTransformation

      case _ =>
        val dataStreamPlan = optimizer
          .optimize(tableOperation.getChild, updatesAsRetraction = false, getRelBuilder)
        translateToCRow(dataStreamPlan, unwrapQueryConfig).getTransformation
    }
  }

  private def unwrapQueryConfig = {
    config.getPlannerConfig.unwrap(classOf[QueryConfigProvider]).get().getConfig
  }

  override def explain(
      tableOperations: util.List[QueryOperation],
      extended: Boolean)
    : String = {
    tableOperations.asScala.map(explain(_, unwrapQueryConfig))
      .mkString(s"${System.lineSeparator}${System.lineSeparator}")
  }

  private def explain(tableOperation: QueryOperation, queryConfig: StreamQueryConfig) = {
    val ast = getRelBuilder.tableOperation(tableOperation).build()
    val optimizedPlan = optimizer
      .optimize(tableOperation, updatesAsRetraction = false, getRelBuilder)
    val dataStream = translateToCRow(optimizedPlan, queryConfig)

    val env = dataStream.getExecutionEnvironment
    val jsonSqlPlan = env.getExecutionPlan

    val sqlPlan = PlanJsonParser.getSqlExecutionPlan(jsonSqlPlan, false)

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

  override def getCompletionHints(
      statement: String,
      position: Int)
    : Array[String] = {
    val planner = getFlinkPlanner
    planner.getCompletionHints(statement, position)
  }


  /** Returns the Calcite [[FrameworkConfig]] of this TableEnvironment. */
  private def getFlinkPlanner: FlinkPlannerImpl = {
    val currentCatalogName = catalogManager.getCurrentCatalog
    val currentDatabase = catalogManager.getCurrentDatabase

    planningConfigurationBuilder.createFlinkPlanner(currentCatalogName, currentDatabase)
  }

  /** Returns the [[FlinkRelBuilder]] of this TableEnvironment. */
  private[flink] def getRelBuilder: FlinkRelBuilder = {
    val currentCatalogName = catalogManager.getCurrentCatalog
    val currentDatabase = catalogManager.getCurrentDatabase

    planningConfigurationBuilder.createRelBuilder(currentCatalogName, currentDatabase)
  }

  private[flink] def getConfig: TableConfig = config

  private[flink] def getExecutionEnvironment: StreamExecutionEnvironment = execEnv

  /* Implementation */

  /**
    * Translates a logical [[RelNode]] plan into a [[DataStream]] of type [[CRow]].
    *
    * @param logicalPlan The logical plan to translate.
    * @param queryConfig  The configuration for the query to generate.
    * @return The [[DataStream]] of type [[CRow]].
    */
  private def translateToCRow(
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
    * Translates a [[Table]] into a [[DataStream]].
    *
    * The transformation involves optimizing the relational expression tree as defined by
    * Table API calls and / or SQL queries and generating corresponding [[DataStream]] operators.
    *
    * @param table The root node of the relational expression tree.
    * @param queryConfig The configuration for the query to generate.
    * @param updatesAsRetraction Set to true to encode updates as retraction messages.
    * @param withChangeFlag Set to true to emit records with change flags.
    * @param tpe The [[TypeInformation]] of the resulting [[DataStream]].
    * @tparam A The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  private def translateToType[A](
      table: QueryOperation,
      queryConfig: StreamQueryConfig,
      updatesAsRetraction: Boolean,
      withChangeFlag: Boolean,
      tpe: TypeInformation[A])
    : DataStream[A] = {
    val dataStreamPlan = optimizer.optimize(table, updatesAsRetraction, getRelBuilder)
    val rowType = getResultType(table.getTableSchema.getFieldNames, dataStreamPlan)

    // if no change flags are requested, verify table is an insert-only (append-only) table.
    if (!withChangeFlag && !UpdatingPlanChecker.isAppendOnly(dataStreamPlan)) {
      throw new TableException(
        "Table is not an append-only table. " +
          "Use the toRetractStream() in order to handle add and retract messages.")
    }

    // get CRow plan
    val plan: DataStream[CRow] = translateToCRow(dataStreamPlan, queryConfig)

    StreamConversionUtils.convert(plan, rowType, queryConfig, withChangeFlag, tpe, config)
  }

  /**
    * Writes a [[Table]] to a [[TableSink]].
    *
    * Internally, the [[Table]] is translated into a [[DataStream]] and handed over to the
    * [[TableSink]] to write it.
    *
    * @param tableOperation The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @param queryConfig The configuration for the query to generate.
    * @tparam T The expected type of the [[DataStream]] which represents the [[Table]].
    */
  private def writeToSink[T](
      tableOperation: QueryOperation,
      sink: TableSink[T],
      queryConfig: StreamQueryConfig)
    : DataStream[_] = {

    sink match {

      case retractSink: RetractStreamTableSink[_] =>
        // retraction sink can always be used
        val outputType = sink.getOutputType
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        val result: DataStream[T] =
          translateToType(
            tableOperation,
            queryConfig,
            updatesAsRetraction = true,
            withChangeFlag = true,
            outputType)
        // Give the DataStream to the TableSink to emit it.
        retractSink.asInstanceOf[RetractStreamTableSink[Any]]
          .emitDataStream(result.asInstanceOf[DataStream[JTuple2[JBool, Any]]])
        result

      case upsertSink: UpsertStreamTableSink[_] =>
        // optimize plan
        val optimizedPlan = optimizer
          .optimize(tableOperation, updatesAsRetraction = false, getRelBuilder)
        // check for append only table
        val isAppendOnlyTable = UpdatingPlanChecker.isAppendOnly(optimizedPlan)
        upsertSink.setIsAppendOnly(isAppendOnlyTable)
        // extract unique key fields
        val tableKeys: Option[Array[String]] = UpdatingPlanChecker.getUniqueKeyFields(optimizedPlan)
        // check that we have keys if the table has changes (is not append-only)
        tableKeys match {
          case Some(keys) => upsertSink.setKeyFields(keys)
          case None if isAppendOnlyTable => upsertSink.setKeyFields(null)
          case None if !isAppendOnlyTable => throw new TableException(
            "UpsertStreamTableSink requires that Table has full primary keys if it is updated.")
        }
        val outputType = sink.getOutputType
        val resultType = getResultType(tableOperation.getTableSchema.getFieldNames, optimizedPlan)
        val cRowStream = translateToCRow(optimizedPlan, queryConfig)
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        val result: DataStream[T] = StreamConversionUtils.convert(
          cRowStream,
          resultType,
          queryConfig,
          withChangeFlag = true,
          outputType,
          config)
        // Give the DataStream to the TableSink to emit it.
        upsertSink.asInstanceOf[UpsertStreamTableSink[Any]]
          .emitDataStream(result.asInstanceOf[DataStream[JTuple2[JBool, Any]]])
        result

      case appendSink: AppendStreamTableSink[_] =>
        // optimize plan
        val optimizedPlan = optimizer
          .optimize(tableOperation, updatesAsRetraction = false, getRelBuilder)
        // verify table is an insert-only (append-only) table
        if (!UpdatingPlanChecker.isAppendOnly(optimizedPlan)) {
          throw new TableException(
            "AppendStreamTableSink requires that Table has only insert changes.")
        }
        val outputType = sink.getOutputType
        val resultType = getResultType(tableOperation.getTableSchema.getFieldNames, optimizedPlan)
        val cRowStream = translateToCRow(optimizedPlan, queryConfig)
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        val result: DataStream[T] = StreamConversionUtils.convert(
          cRowStream,
          resultType,
          queryConfig,
          withChangeFlag = false,
          outputType,
          config)
        // Give the DataStream to the TableSink to emit it.
        appendSink.asInstanceOf[AppendStreamTableSink[T]].emitDataStream(result)
        result

      case _ =>
        throw new TableException("Stream Tables can only be emitted by AppendStreamTableSink, " +
          "RetractStreamTableSink, or UpsertStreamTableSink.")
    }
  }

  /**
    * Returns the record type of the optimized plan with field names of the logical plan.
    */
  private def getResultType(originalNames: Array[String], optimizedPlan: RelNode): TableSchema = {
    val fieldTypes = optimizedPlan.getRowType.getFieldList.asScala.map(_.getType)
      .map(FlinkTypeFactory.toTypeInfo).toArray

    new TableSchema(originalNames, fieldTypes)
  }

  private def getTableSink(tablePath: JList[String]): Option[TableSink[_]] = {
    JavaScalaConversionUtil.toScala(catalogManager.resolveTable(tablePath.asScala: _*)) match {
      case Some(s) if s.getExternalCatalogTable.isPresent =>

        Option(TableFactoryUtil.findAndCreateTableSink(s.getExternalCatalogTable.get()))

      case Some(s) if JavaScalaConversionUtil.toScala(s.getCatalogTable)
        .exists(_.isInstanceOf[ConnectorCatalogTable[_, _]]) =>

        JavaScalaConversionUtil
          .toScala(s.getCatalogTable.get().asInstanceOf[ConnectorCatalogTable[_, _]].getTableSink)

      case Some(s) if JavaScalaConversionUtil.toScala(s.getCatalogTable)
        .exists(_.isInstanceOf[CatalogTable]) =>

        val sinkProperties = s.getCatalogTable.get().asInstanceOf[CatalogTable].toProperties
        Option(TableFactoryService.find(classOf[TableSinkFactory[_]], sinkProperties)
          .createTableSink(sinkProperties))

      case _ => None
    }
  }
}
