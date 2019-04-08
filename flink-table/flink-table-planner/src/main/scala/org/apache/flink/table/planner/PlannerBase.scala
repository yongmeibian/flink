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

import _root_.java.util.concurrent.atomic.AtomicInteger

import com.google.common.collect.ImmutableList
import org.apache.calcite.config.Lex
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException
import org.apache.calcite.plan.hep.{HepMatchOrder, HepPlanner, HepProgram, HepProgramBuilder}
import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.sql._
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.util.ChainedSqlOperatorTable
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.tools.{ValidationException => _, _}
import org.apache.flink.table.api._
import org.apache.flink.table.calcite._
import org.apache.flink.table.codegen.ExpressionReducer
import org.apache.flink.table.expressions.{ExpressionBridge, PlannerExpression, PlannerExpressionConverter}
import org.apache.flink.table.operations.{PlannerTableOperation, TableOperation}
import org.apache.flink.table.plan.TableOperationConverter
import org.apache.flink.table.plan.cost.DataSetCostFactory
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.apache.flink.table.plan.schema.TableSourceSinkTable
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.validate.CalciteFunctionCatalog

import _root_.scala.collection.JavaConverters._

/**
  * The abstract base class for batch and stream TableEnvironments.
  *
  * @param config The configuration of the TableEnvironment
  */
abstract class PlannerBase(val config: TableConfig, functionCatalog: FunctionCatalog)
  extends Planner{

  // the catalog to hold all registered and translated tables
  // we disable caching here to prevent side effects
  private val internalSchema: CalciteSchema = CalciteSchema.createRootSchema(false, false)
  private[flink] val rootSchema: SchemaPlus = internalSchema.plus()

  private val typeSystem = new FlinkTypeSystem
  private val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(typeSystem)

  // Table API/SQL function catalog
  private val calciteFunctionCatalog = CalciteFunctionCatalog(typeFactory, functionCatalog)

  private[flink] val expressionBridge = new ExpressionBridge[PlannerExpression](functionCatalog,
    PlannerExpressionConverter.INSTANCE)

  // the configuration to create a Calcite planner
  private lazy val frameworkConfig: FrameworkConfig = Frameworks
    .newConfigBuilder
    .defaultSchema(rootSchema)
    .parserConfig(getSqlParserConfig)
    .costFactory(new DataSetCostFactory)
    .typeSystem(typeSystem)
    .operatorTable(getSqlOperatorTable)
    .sqlToRelConverterConfig(getSqlToRelConverterConfig)
    .context(Contexts.of(
      new TableOperationConverter.ToRelConverterSupplier(expressionBridge)
    ))
    // set the executor to evaluate constant expressions
    .executor(new ExpressionReducer(config))
    .build

  // the builder for Calcite RelNodes, Calcite's representation of a relational expression tree.
  protected lazy val relBuilder: FlinkRelBuilder = FlinkRelBuilder.create(frameworkConfig, expressionBridge)

  // the planner instance used to optimize queries of this TableEnvironment
  private lazy val planner: RelOptPlanner = relBuilder.getPlanner

  // a counter for unique attribute names
  private[flink] val attrNameCntr: AtomicInteger = new AtomicInteger(0)

  /** Returns the table config to define the runtime behavior of the Table API. */
  def getConfig: TableConfig = config

  protected def calciteConfig: CalciteConfig = config.getPlannerConfig.unwrap(classOf[CalciteConfig])
    .orElse(CalciteConfig.DEFAULT)

  override def getCompletionHints(statement: String, position: Int): Array[String] = {
    val planner = new FlinkPlannerImpl(
      getFrameworkConfig,
      getPlanner,
      getTypeFactory)
    planner.getCompletionHints(statement, position)
  }

  /**
    * Evaluates a SQL query on registered tables and retrieves the result as a [[TableOperation]].
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[TableOperation]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[TableOperation]] as follows:
    *
    * {{{
    *   val table: Table = ...
    *   // the table is not registered to the table environment
    *   tEnv.sqlQuery(s"SELECT * FROM $table")
    * }}}
    *
    * @param query The SQL query to evaluate.
    * @return The result of the query as Table
    */
  override def sqlQuery(query: String): TableOperation = {
    val planner = new FlinkPlannerImpl(getFrameworkConfig, getPlanner, getTypeFactory)
    // parse the sql query
    val parsed = planner.parse(query)
    if (null != parsed && parsed.getKind.belongsTo(SqlKind.QUERY)) {
      // validate the sql query
      val validated = planner.validate(parsed)
      // transform to a relational tree
      val relational = planner.rel(validated)
      new PlannerTableOperation(relational.rel)
    } else {
      throw new TableException(
        "Unsupported SQL query! sqlQuery() only accepts SQL queries of type " +
          "SELECT, UNION, INTERSECT, EXCEPT, VALUES, and ORDER_BY.")
    }
  }

  /**
    * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
    * NOTE: Currently only SQL INSERT statements are supported.
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[TableOperation]] as follows:
    *
    * {{{
    *   // register the table sink into which the result is inserted.
    *   tEnv.registerTableSink("sinkTable", fieldNames, fieldsTypes, tableSink)
    *   val sourceTable: Table = ...
    *   // sourceTable is not registered to the table environment
    *   tEnv.sqlUpdate(s"INSERT INTO sinkTable SELECT * FROM $sourceTable")
    * }}}
    *
    * @param stmt The SQL statement to evaluate.
    * @param config The [[QueryConfig]] to use.
    */
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
        val queryResult = new PlannerTableOperation(planner.rel(validatedQuery).rel)

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
    * Returns the built-in normalization rules that are defined by the environment.
    */
  protected def getBuiltInNormRuleSet: RuleSet

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
    * Returns the SqlToRelConverter config.
    */
  private def getSqlToRelConverterConfig: SqlToRelConverter.Config = {
    calciteConfig.sqlToRelConverterConfig match {

      case None =>
        SqlToRelConverter.configBuilder()
          .withTrimUnusedFields(false)
          .withConvertTableAccess(false)
          .withInSubQueryThreshold(Integer.MAX_VALUE)
          .withRelBuilderFactory(new FlinkRelBuilderFactory(expressionBridge))
          .build()

      case Some(c) => c
    }
  }

  /**
    * Returns the operator table for this environment including a custom Calcite configuration.
    */
  private def getSqlOperatorTable: SqlOperatorTable = {
    calciteConfig.sqlOperatorTable match {

      case None =>
        calciteFunctionCatalog.getSqlOperatorTable

      case Some(table) =>
        if (calciteConfig.replacesSqlOperatorTable) {
          table
        } else {
          ChainedSqlOperatorTable.of(calciteFunctionCatalog.getSqlOperatorTable, table)
        }
    }
  }

  /**
    * Returns the normalization rule set for this environment
    * including a custom RuleSet configuration.
    */
  private def getNormRuleSet: RuleSet = {
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
  private def getLogicalOptRuleSet: RuleSet = {
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
  private def getPhysicalOptRuleSet: RuleSet = {
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
    * Returns the SQL parser config for this environment including a custom Calcite configuration.
    */
  private def getSqlParserConfig: SqlParser.Config = {
    calciteConfig.sqlParserConfig match {

      case None =>
        // we use Java lex because back ticks are easier than double quotes in programming
        // and cases are preserved
        SqlParser
          .configBuilder()
          .setLex(Lex.JAVA)
          .build()

      case Some(sqlParserConfig) =>
        sqlParserConfig
    }
  }

  /**
    * Returns the built-in logical optimization rules that are defined by the environment.
    */
  private def getBuiltInLogicalOptRuleSet: RuleSet = {
    FlinkRuleSets.LOGICAL_OPT_RULES
  }

  /**
    * run HEP planner with rules applied simultaneously. Apply all of the rules to the given
    * node before going to the next one. If a rule creates a new node all of the rules will
    * be applied to this new node.
    */
  private def runHepPlannerSimultaneously(
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
  private def runHepPlanner(
    hepProgram: HepProgram,
    input: RelNode,
    targetTraits: RelTraitSet): RelNode = {

    val planner = new HepPlanner(hepProgram, frameworkConfig.getContext)
    planner.setRoot(input)
    if (input.getTraitSet != targetTraits) {
      planner.changeTraits(input, targetTraits.simplify)
    }
    planner.findBestExp
  }

  /**
    * run VOLCANO planner
    */
  private def runVolcanoPlanner(
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

  /**
    * Writes the [[TableOperation]] to a [[TableSink]] that was registered under the specified name.
    *
    * @param table The table to write to the TableSink.
    * @param sinkTableName The name of the registered TableSink.
    * @param conf The query configuration to use.
    */
  private[flink] def insertInto(table: TableOperation, sinkTableName: String, conf: QueryConfig): Unit = {

    // check that sink table exists
    if (null == sinkTableName) throw new TableException("Name of TableSink must not be null.")
    if (sinkTableName.isEmpty) throw new TableException("Name of TableSink must not be empty.")

    getTable(sinkTableName) match {

      case None =>
        throw new TableException(s"No table was registered under the name $sinkTableName.")

      case Some(s: TableSourceSinkTable[_, _]) if s.tableSinkTable.isDefined =>
        val tableSink = s.tableSinkTable.get.tableSink
        // validate schema of source table and table sink
        val srcFieldTypes = table.getTableSchema.getFieldTypes
        val sinkFieldTypes = tableSink.getFieldTypes

        if (srcFieldTypes.length != sinkFieldTypes.length ||
          srcFieldTypes.zip(sinkFieldTypes).exists { case (srcF, snkF) => srcF != snkF }) {

          val srcFieldNames = table.getTableSchema.getFieldNames
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
  protected def registerTableInternal(name: String, table: AbstractTable): Unit = {

    if (isRegistered(name)) {
      throw new TableException(s"Table \'$name\' already exists. " +
        s"Please, choose a different name.")
    } else {
      rootSchema.add(name, table)
    }
  }

  /**
    * Checks if a table is registered under the given name.
    *
    * @param name The table name to check.
    * @return true, if a table is registered under the name, false otherwise.
    */
  private def isRegistered(name: String): Boolean = {
    rootSchema.getTableNames.contains(name)
  }

  /**
    * Get a table from either internal or external catalogs.
    *
    * @param name The name of the table.
    * @return The table registered either internally or externally, None otherwise.
    */
  private[flink] def getTable(name: String): Option[org.apache.calcite.schema.Table] = {

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

    val pathNames = name.split('.').toList
    getTableFromSchema(rootSchema, pathNames)
  }

  /** Returns the [[FlinkRelBuilder]] of this TableEnvironment. */
  private[flink] def getRelBuilder: FlinkRelBuilder = {
    relBuilder
  }

  /** Returns the Calcite [[org.apache.calcite.plan.RelOptPlanner]] of this TableEnvironment. */
  private[flink] def getPlanner: RelOptPlanner = {
    planner
  }

  /** Returns the [[FlinkTypeFactory]] of this TableEnvironment. */
  private[flink] def getTypeFactory: FlinkTypeFactory = {
    typeFactory
  }

  private[flink] def getFunctionCatalog: FunctionCatalog = {
    functionCatalog
  }

  /** Returns the Calcite [[FrameworkConfig]] of this TableEnvironment. */
  private[flink] def getFrameworkConfig: FrameworkConfig = {
    frameworkConfig
  }
}


