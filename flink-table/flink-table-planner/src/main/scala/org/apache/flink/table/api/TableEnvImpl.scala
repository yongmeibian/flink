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

package org.apache.flink.table.api

import _root_.java.util.concurrent.atomic.AtomicInteger

import com.google.common.collect.ImmutableList
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException
import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.{HepMatchOrder, HepPlanner, HepProgram, HepProgramBuilder}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql._
import org.apache.calcite.tools._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.calcite._
import org.apache.flink.table.catalog._
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction, UserDefinedAggregateFunction}
import org.apache.flink.table.operations.{CatalogTableOperation, OperationTreeBuilder, PlannerTableOperation}
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.apache.flink.table.plan.schema.TableSourceSinkTable
import org.apache.flink.table.planner.PlanningConfigurationBuilder
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.util.JavaScalaConversionUtil
import org.apache.flink.table.util.JavaScalaConversionUtil.toScala
import org.apache.flink.table.validate.FunctionCatalog

import _root_.scala.collection.JavaConverters._

/**
  * The abstract base class for the implementation of batch and stream TableEnvironments.
  *
  * @param config The configuration of the TableEnvironment
  */
abstract class TableEnvImpl(val config: TableConfig) extends TableEnvironment {

  // Table API/SQL function catalog
  private[flink] val functionCatalog: FunctionCatalog = new FunctionCatalog()

  private val BUILTIN_CATALOG_NAME = "builtin"
  private val catalogManager = new CatalogManager(BUILTIN_CATALOG_NAME,
    new GenericInMemoryCatalog(BUILTIN_CATALOG_NAME))
  private val internalSchema: CalciteSchema =
    asRootSchema(new CatalogManagerSchema(catalogManager, isBatch))

  // temporary bridge between API and planner
  private[flink] val expressionBridge: ExpressionBridge[PlannerExpression] =
    new ExpressionBridge[PlannerExpression](functionCatalog, PlannerExpressionConverter.INSTANCE)

  // a counter for unique attribute names
  private[flink] val attrNameCntr: AtomicInteger = new AtomicInteger(0)

  private[flink] val operationTreeBuilder = new OperationTreeBuilder(this)

  private val planningConfigurationBuilder: PlanningConfigurationBuilder =
    new PlanningConfigurationBuilder(
      config,
      functionCatalog,
      internalSchema,
      expressionBridge)

  protected def calciteConfig: CalciteConfig = config.getPlannerConfig
    .unwrap(classOf[CalciteConfig])
    .orElse(CalciteConfig.DEFAULT)

  def getConfig: TableConfig = config

  private def isBatch: Boolean = this match {
    case _: BatchTableEnvImpl => true
    case _ => false
  }

  private[flink] def queryConfig: QueryConfig = this match {
    case _: BatchTableEnvImpl => new BatchQueryConfig
    case _: StreamTableEnvImpl => new StreamQueryConfig
    case _ => null
  }

  /**
    * Returns the normalization rule set for this environment
    * including a custom RuleSet configuration.
    */
  protected def getNormRuleSet: RuleSet = {
    calciteConfig.normRuleSet match {

      case None =>
        getBuiltInNormRuleSet

      case Some(ruleSet) =>
        if (calciteConfig.replacesNormRuleSet) {
          ruleSet
        } else {
          RuleSets.ofList((getBuiltInNormRuleSet.asScala ++ ruleSet.asScala).asJava)
        }
    }
  }

  /**
    * Returns the logical optimization rule set for this environment
    * including a custom RuleSet configuration.
    */
  protected def getLogicalOptRuleSet: RuleSet = {
    calciteConfig.logicalOptRuleSet match {

      case None =>
        getBuiltInLogicalOptRuleSet

      case Some(ruleSet) =>
        if (calciteConfig.replacesLogicalOptRuleSet) {
          ruleSet
        } else {
          RuleSets.ofList((getBuiltInLogicalOptRuleSet.asScala ++ ruleSet.asScala).asJava)
        }
    }
  }

  /**
    * Returns the physical optimization rule set for this environment
    * including a custom RuleSet configuration.
    */
  protected def getPhysicalOptRuleSet: RuleSet = {
    calciteConfig.physicalOptRuleSet match {

      case None =>
        getBuiltInPhysicalOptRuleSet

      case Some(ruleSet) =>
        if (calciteConfig.replacesPhysicalOptRuleSet) {
          ruleSet
        } else {
          RuleSets.ofList((getBuiltInPhysicalOptRuleSet.asScala ++ ruleSet.asScala).asJava)
        }
    }
  }

  /**
    * Returns the built-in normalization rules that are defined by the environment.
    */
  protected def getBuiltInNormRuleSet: RuleSet

  /**
    * Returns the built-in logical optimization rules that are defined by the environment.
    */
  protected def getBuiltInLogicalOptRuleSet: RuleSet = {
    FlinkRuleSets.LOGICAL_OPT_RULES
  }

  /**
    * Returns the built-in physical optimization rules that are defined by the environment.
    */
  protected def getBuiltInPhysicalOptRuleSet: RuleSet

  protected def optimizeConvertSubQueries(relNode: RelNode): RelNode = {
    runHepPlannerSequentially(
      HepMatchOrder.BOTTOM_UP,
      FlinkRuleSets.TABLE_SUBQUERY_RULES,
      relNode,
      relNode.getTraitSet)
  }

  protected def optimizeExpandPlan(relNode: RelNode): RelNode = {
    val result = runHepPlannerSimultaneously(
      HepMatchOrder.TOP_DOWN,
      FlinkRuleSets.EXPAND_PLAN_RULES,
      relNode,
      relNode.getTraitSet)

    runHepPlannerSequentially(
      HepMatchOrder.TOP_DOWN,
      FlinkRuleSets.POST_EXPAND_CLEAN_UP_RULES,
      result,
      result.getTraitSet)
  }

  protected def optimizeNormalizeLogicalPlan(relNode: RelNode): RelNode = {
    val normRuleSet = getNormRuleSet
    if (normRuleSet.iterator().hasNext) {
      runHepPlannerSequentially(HepMatchOrder.BOTTOM_UP, normRuleSet, relNode, relNode.getTraitSet)
    } else {
      relNode
    }
  }

  protected def optimizeLogicalPlan(relNode: RelNode): RelNode = {
    val logicalOptRuleSet = getLogicalOptRuleSet
    val logicalOutputProps = relNode.getTraitSet.replace(FlinkConventions.LOGICAL).simplify()
    if (logicalOptRuleSet.iterator().hasNext) {
      runVolcanoPlanner(logicalOptRuleSet, relNode, logicalOutputProps)
    } else {
      relNode
    }
  }

  protected def optimizePhysicalPlan(relNode: RelNode, convention: Convention): RelNode = {
    val physicalOptRuleSet = getPhysicalOptRuleSet
    val physicalOutputProps = relNode.getTraitSet.replace(convention).simplify()
    if (physicalOptRuleSet.iterator().hasNext) {
      runVolcanoPlanner(physicalOptRuleSet, relNode, physicalOutputProps)
    } else {
      relNode
    }
  }

  /**
    * run HEP planner with rules applied one by one. First apply one rule to all of the nodes
    * and only then apply the next rule. If a rule creates a new node preceding rules will not
    * be applied to the newly created node.
    */
  protected def runHepPlannerSequentially(
    hepMatchOrder: HepMatchOrder,
    ruleSet: RuleSet,
    input: RelNode,
    targetTraits: RelTraitSet): RelNode = {

    val builder = new HepProgramBuilder
    builder.addMatchOrder(hepMatchOrder)

    val it = ruleSet.iterator()
    while (it.hasNext) {
      builder.addRuleInstance(it.next())
    }
    runHepPlanner(builder.build(), input, targetTraits)
  }

  /**
    * run HEP planner with rules applied simultaneously. Apply all of the rules to the given
    * node before going to the next one. If a rule creates a new node all of the rules will
    * be applied to this new node.
    */
  protected def runHepPlannerSimultaneously(
    hepMatchOrder: HepMatchOrder,
    ruleSet: RuleSet,
    input: RelNode,
    targetTraits: RelTraitSet): RelNode = {

    val builder = new HepProgramBuilder
    builder.addMatchOrder(hepMatchOrder)

    builder.addRuleCollection(ruleSet.asScala.toList.asJava)
    runHepPlanner(builder.build(), input, targetTraits)
  }

  /**
    * run HEP planner
    */
  protected def runHepPlanner(
    hepProgram: HepProgram,
    input: RelNode,
    targetTraits: RelTraitSet): RelNode = {

    val planner = new HepPlanner(hepProgram, planningConfigurationBuilder.getContext)
    planner.setRoot(input)
    if (input.getTraitSet != targetTraits) {
      planner.changeTraits(input, targetTraits.simplify)
    }
    planner.findBestExp
  }

  /**
    * run VOLCANO planner
    */
  protected def runVolcanoPlanner(
    ruleSet: RuleSet,
    input: RelNode,
    targetTraits: RelTraitSet): RelNode = {
    val optProgram = Programs.ofRules(ruleSet)

    val output = try {
      optProgram.run(getPlanner, input, targetTraits,
        ImmutableList.of(), ImmutableList.of())
    } catch {
      case e: CannotPlanException =>
        throw new TableException(
          s"Cannot generate a valid execution plan for the given query: \n\n" +
            s"${RelOptUtil.toString(input)}\n" +
            s"This exception indicates that the query uses an unsupported SQL feature.\n" +
            s"Please check the documentation for the set of currently supported SQL features.")
      case t: TableException =>
        throw new TableException(
          s"Cannot generate a valid execution plan for the given query: \n\n" +
            s"${RelOptUtil.toString(input)}\n" +
            s"${t.getMessage}\n" +
            s"Please check the documentation for the set of currently supported SQL features.")
      case a: AssertionError =>
        // keep original exception stack for caller
        throw a
    }
    output
  }

  override def fromTableSource(source: TableSource[_]): Table = {
    val name = createUniqueTableName()
    registerTableSourceInternal(name, source)
    scan(name)
  }

  override def registerExternalCatalog(name: String, externalCatalog: ExternalCatalog): Unit = {
    catalogManager.registerExternalCatalog(name, externalCatalog)
  }

  override def getRegisteredExternalCatalog(name: String): ExternalCatalog = {
    toScala(catalogManager.getExternalCatalog(name)) match {
      case Some(catalog) => catalog
      case None => throw new ExternalCatalogNotExistException(name)
    }
  }

  override def registerFunction(name: String, function: ScalarFunction): Unit = {
    // check if class could be instantiated
    checkForInstantiation(function.getClass)

    functionCatalog.registerScalarFunction(
      name,
      function,
      planningConfigurationBuilder.getTypeFactory)
  }

  /**
    * Registers a [[TableFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  private[flink] def registerTableFunctionInternal[T: TypeInformation](
    name: String, function: TableFunction[T]): Unit = {
    // check if class not Scala object
    checkNotSingleton(function.getClass)
    // check if class could be instantiated
    checkForInstantiation(function.getClass)

    val typeInfo: TypeInformation[_] = if (function.getResultType != null) {
      function.getResultType
    } else {
      implicitly[TypeInformation[T]]
    }

    functionCatalog.registerTableFunction(
      name,
      function,
      typeInfo,
      planningConfigurationBuilder.getTypeFactory)
  }

  /**
    * Registers an [[AggregateFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  private[flink] def registerAggregateFunctionInternal[T: TypeInformation, ACC: TypeInformation](
      name: String, function: UserDefinedAggregateFunction[T, ACC]): Unit = {
    // check if class not Scala object
    checkNotSingleton(function.getClass)
    // check if class could be instantiated
    checkForInstantiation(function.getClass)

    val resultTypeInfo: TypeInformation[_] = getResultTypeOfAggregateFunction(
      function,
      implicitly[TypeInformation[T]])

    val accTypeInfo: TypeInformation[_] = getAccumulatorTypeOfAggregateFunction(
      function,
      implicitly[TypeInformation[ACC]])

    functionCatalog.registerAggregateFunction(
      name,
      function,
      resultTypeInfo,
      accTypeInfo,
      planningConfigurationBuilder.getTypeFactory)
  }

  override def registerTable(name: String, table: Table): Unit = {

    // check that table belongs to this table environment
    if (table.asInstanceOf[TableImpl].tableEnv != this) {
      throw new TableException(
        "Only tables that belong to this TableEnvironment can be registered.")
    }

    checkValidTableName(name)
    val path = new ObjectPath(defaultCatalog.getCurrentDatabase, name)
    defaultCatalog.createTable(path, new TableOperationCatalogView(table.getTableOperation), false)
  }

  override def registerTableSource(name: String, tableSource: TableSource[_]): Unit = {
    checkValidTableName(name)
    registerTableSourceInternal(name, tableSource)
  }

  override def registerCatalog(name: String, catalog: Catalog): Unit = {
    catalogManager.registerCatalog(name, catalog)
  }

  override def getCatalog(catalogName: String): Catalog = {
    catalogManager.getCatalog(catalogName)
  }

  override def getCurrentCatalogName: String = {
    catalogManager.getCurrentCatalogName
  }

  override def getCurrentDatabaseName: String = {
    catalogManager.getCurrentDatabaseName
  }

  override def setCurrentCatalog(name: String): Unit = {
    catalogManager.setCurrentCatalog(name)
    val defaultDb = catalogManager.getCatalog(name).getCurrentDatabase
    catalogManager.setCurrentDatabase(defaultDb)
  }

  override def setCurrentDatabase(catalogName: String, databaseName: String): Unit = {
    catalogManager.setCurrentCatalog(catalogName)
    catalogManager.setCurrentDatabase(databaseName)
  }

  /**
    * Registers an internal [[TableSource]] in this [[TableEnvironment]]'s catalog without
    * name checking. Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  protected def registerTableSourceInternal(name: String, tableSource: TableSource[_]): Unit

  override def registerTableSink(
      name: String,
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]],
      tableSink: TableSink[_]): Unit

  override def registerTableSink(name: String, configuredSink: TableSink[_]): Unit

  /**
    * Replaces a registered Table with another Table under the same name.
    * We use this method to replace a [[org.apache.flink.table.plan.schema.DataStreamTable]]
    * with a [[org.apache.calcite.schema.TranslatableTable]].
    *
    * @param name Name of the table to replace.
    * @param table The table that replaces the previous table.
    */
  protected def replaceRegisteredTable(name: String, table: AbstractTable): Unit = {
    val defaultCatalog = catalogManager.getCatalog(BUILTIN_CATALOG_NAME)
    val defaultDb = defaultCatalog.getCurrentDatabase
    val path = new ObjectPath(defaultDb, name)
    defaultCatalog.alterTable(path, new CalciteCatalogTable(table, getTypeFactory), false)
  }

  @throws[TableException]
  override def scan(tablePath: String*): Table = {
     scanInternal(tablePath.toArray) match {
      case Some(table) => new TableImpl(this, table)
      case None => throw new TableException(s"Table '${tablePath.mkString(".")}' was not found.")
    }
  }

  private[flink] def scanInternal(tablePath: Array[String]): Option[CatalogTableOperation] = {
    JavaScalaConversionUtil.toScala(catalogManager.resolveTable(tablePath : _*))
  }

  private def extractTableSchema(table: schema.Table): TableSchema = {
    val relDataType = table.getRowType(planningConfigurationBuilder.getTypeFactory)
    val fieldNames = relDataType.getFieldNames
    val fieldTypes = relDataType.getFieldList.asScala
      .map(field => FlinkTypeFactory.toTypeInfo(field.getType))
    new TableSchema(fieldNames.asScala.toArray, fieldTypes.toArray)
  }

  private def getSchema(schemaPath: Array[String]): SchemaPlus = {
    var schema = internalSchema.plus()
    for (schemaName <- schemaPath) {
      schema = schema.getSubSchema(schemaName)
      if (schema == null) {
        return schema
      }
    }
    schema
  }

  override def listTables(): Array[String] = {
    val currentCalogName = catalogManager.getCurrentCatalogName
    val currentCatalog = catalogManager.getCatalog(currentCalogName)
    currentCatalog.listTables(catalogManager.getCurrentDatabaseName).asScala.toArray
  }

  override def listUserDefinedFunctions(): Array[String] = {
    functionCatalog.getUserDefinedFunctions.toArray
  }

  override def explain(table: Table): String

  override def getCompletionHints(statement: String, position: Int): Array[String] = {
    val planner = new FlinkPlannerImpl(
      getFrameworkConfig,
      getPlanner,
      getTypeFactory)
    planner.getCompletionHints(statement, position)
  }

  override def sqlQuery(query: String): Table = {
    val planner = new FlinkPlannerImpl(getFrameworkConfig, getPlanner, getTypeFactory)
    // parse the sql query
    val parsed = planner.parse(query)
    if (null != parsed && parsed.getKind.belongsTo(SqlKind.QUERY)) {
      // validate the sql query
      val validated = planner.validate(parsed)
      // transform to a relational tree
      val relational = planner.rel(validated)
      new TableImpl(this, new PlannerTableOperation(relational.rel))
    } else {
      throw new TableException(
        "Unsupported SQL query! sqlQuery() only accepts SQL queries of type " +
          "SELECT, UNION, INTERSECT, EXCEPT, VALUES, and ORDER_BY.")
    }
  }

  override def sqlUpdate(stmt: String): Unit = {
    sqlUpdate(stmt, this.queryConfig)
  }

  override def sqlUpdate(stmt: String, config: QueryConfig): Unit = {
    val planner = new FlinkPlannerImpl(getFrameworkConfig, getPlanner, getTypeFactory)
    // parse the sql query
    val parsed = planner.parse(stmt)
    parsed match {
      case insert: SqlInsert =>
        // validate the SQL query
        val query = insert.getSource
        val validatedQuery = planner.validate(query)

        // get query result as Table
        val queryResult = new TableImpl(this,
          new PlannerTableOperation(planner.rel(validatedQuery).rel))

        // get name of sink table
        val targetTableName = insert.getTargetTable.asInstanceOf[SqlIdentifier].names.get(0)

        // insert query result into sink table
        insertInto(queryResult, targetTableName, config)
      case _ =>
        throw new TableException(
          "Unsupported SQL query! sqlUpdate() only accepts SQL statements of type INSERT.")
    }
  }

  /**
    * Writes a [[Table]] to a [[TableSink]].
    *
    * @param table The [[Table]] to write.
    * @param sink The [[TableSink]] to write the [[Table]] to.
    * @param conf The [[QueryConfig]] to use.
    * @tparam T The data type that the [[TableSink]] expects.
    */
  private[flink] def writeToSink[T](table: Table, sink: TableSink[T], conf: QueryConfig): Unit

  /**
    * Writes the [[Table]] to a [[TableSink]] that was registered under the specified name.
    *
    * @param table The table to write to the TableSink.
    * @param sinkTableName The name of the registered TableSink.
    * @param conf The query configuration to use.
    */
  private[flink] def insertInto(table: Table, sinkTableName: String, conf: QueryConfig): Unit = {

    // check that sink table exists
    if (null == sinkTableName) throw new TableException("Name of TableSink must not be null.")
    if (sinkTableName.isEmpty) throw new TableException("Name of TableSink must not be empty.")

    getTable(sinkTableName) match {

      case None =>
        throw new TableException(s"No table was registered under the name $sinkTableName.")

      case Some(s: TableSourceSinkTable[_, _]) if s.tableSinkTable.isDefined =>
        val tableSink = s.tableSinkTable.get.tableSink
        // validate schema of source table and table sink
        val srcFieldTypes = table.getSchema.getFieldTypes
        val sinkFieldTypes = tableSink.getFieldTypes

        if (srcFieldTypes.length != sinkFieldTypes.length ||
          srcFieldTypes.zip(sinkFieldTypes).exists { case (srcF, snkF) => srcF != snkF }) {

          val srcFieldNames = table.getSchema.getFieldNames
          val sinkFieldNames = tableSink.getFieldNames

          // format table and table sink schema strings
          val srcSchema = srcFieldNames.zip(srcFieldTypes)
            .map { case (n, t) => s"$n: ${t.getTypeClass.getSimpleName}" }
            .mkString("[", ", ", "]")
          val sinkSchema = sinkFieldNames.zip(sinkFieldTypes)
            .map { case (n, t) => s"$n: ${t.getTypeClass.getSimpleName}" }
            .mkString("[", ", ", "]")

          throw new ValidationException(
            s"Field types of query result and registered TableSink " +
              s"$sinkTableName do not match.\n" +
              s"Query result schema: $srcSchema\n" +
              s"TableSink schema:    $sinkSchema")
        }
        // emit the table to the configured table sink
        writeToSink(table, tableSink, conf)

      case Some(_) =>
        throw new TableException(s"The table registered as $sinkTableName is not a TableSink. " +
          s"You can only emit query results to a registered TableSink.")
    }
  }

  /**
    * Registers a Calcite [[AbstractTable]] in the TableEnvironment's catalog.
    *
    * @param name The name under which the table will be registered.
    * @param table The table to register in the catalog
    * @throws TableException if another table is registered under the provided name.
    */
  @throws[TableException]
  protected def registerTableInternal(name: String, table: CatalogBaseTable): Unit = {
    val defaultCatalog = catalogManager.getCatalog(BUILTIN_CATALOG_NAME)
    val path = new ObjectPath(defaultCatalog.getCurrentDatabase, name)
    defaultCatalog.createTable(path, table, false)
  }

  /** Returns a unique table name according to the internal naming pattern. */
  protected def createUniqueTableName(): String

  /**
    * Checks if the chosen table name is valid.
    *
    * @param name The table name to check.
    */
  protected def checkValidTableName(name: String): Unit

  /**
    * Get a table from either internal or external catalogs.
    *
    * @param name The name of the table.
    * @return The table registered either internally or externally, None otherwise.
    */
  protected def getTable(name: String): Option[org.apache.calcite.schema.Table] = {

    // recursively fetches a table from a schema.
    def getTableFromSchema(
        schema: SchemaPlus,
        path: List[String]): Option[org.apache.calcite.schema.Table] = {

      path match {
        case tableName :: Nil =>
          // look up table
          Option(schema.getTable(tableName))
        case subschemaName :: remain =>
          // look up subschema
          val subschema = Option(schema.getSubSchema(subschemaName))
          subschema match {
            case Some(s) =>
              // search for table in subschema
              getTableFromSchema(s, remain)
            case None =>
              // subschema does not exist
              None
          }
      }
    }

    JavaScalaConversionUtil.toScala(catalogManager.resolveTable(name.split('.'):_*))
      .flatMap(t =>
        getTableFromSchema(internalSchema.plus(), t.getTablePath.asScala.toList)
      )
  }

  /** Returns a unique temporary attribute name. */
  private[flink] def createUniqueAttributeName(): String = {
    "TMP_" + attrNameCntr.getAndIncrement()
  }

  /** Returns the [[FlinkRelBuilder]] of this TableEnvironment. */
  private[flink] def getRelBuilder: FlinkRelBuilder = {
    val currentCatalogName = catalogManager.getCurrentCatalogName
    val currentCatalog = catalogManager.getCatalog(currentCatalogName)

    val currentDatabase = currentCatalog.getCurrentDatabase

    planningConfigurationBuilder.createRelBuilder(List(currentCatalogName, currentDatabase).asJava)
  }

  /** Returns the Calcite [[org.apache.calcite.plan.RelOptPlanner]] of this TableEnvironment. */
  private[flink] def getPlanner: RelOptPlanner = {
    planningConfigurationBuilder.getPlanner
  }

  /** Returns the [[FlinkTypeFactory]] of this TableEnvironment. */
  private[flink] def getTypeFactory: FlinkTypeFactory = {
    planningConfigurationBuilder.getTypeFactory
  }

  private[flink] def getFunctionCatalog: FunctionCatalog = {
    functionCatalog
  }

  /** Returns the Calcite [[FrameworkConfig]] of this TableEnvironment. */
  private[flink] def getFrameworkConfig: FrameworkConfig = {
    val currentCatalogName = catalogManager.getCurrentCatalogName
    val currentCatalog = catalogManager.getCatalog(currentCatalogName)

    val currentDatabase = currentCatalog.getCurrentDatabase
    val defaultSchema = internalSchema.getSubSchema(currentCatalogName, true)
      .getSubSchema(currentDatabase, true)

    planningConfigurationBuilder.createFrameworkConfig(defaultSchema.plus())
  }
}
