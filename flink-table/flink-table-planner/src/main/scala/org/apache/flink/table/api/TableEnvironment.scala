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

import _root_.java.lang.reflect.Modifier
import _root_.java.util.concurrent.atomic.AtomicInteger

import org.apache.calcite.schema
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.{ExecutionEnvironment => JavaBatchExecEnv}
import org.apache.flink.api.scala.{ExecutionEnvironment => ScalaBatchExecEnv}
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaStreamExecEnv}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment => ScalaStreamExecEnv}
import org.apache.flink.table.api.java.{BatchTableEnvironment => JavaBatchTableEnv, StreamTableEnvironment => JavaStreamTableEnv}
import org.apache.flink.table.api.scala.{BatchTableEnvironment => ScalaBatchTableEnv, StreamTableEnvironment => ScalaStreamTableEnv}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.catalog.{CatalogManager, ExternalCatalog, ExternalCatalogSchema}
import org.apache.flink.table.descriptors.{ConnectorDescriptor, TableDescriptor}
import org.apache.flink.table.expressions.{ExpressionBridge, PlannerExpression, PlannerExpressionConverter}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.operations.{CatalogTableOperation, OperationTreeBuilder, TableOperation}
import org.apache.flink.table.plan.schema.RelTable
import org.apache.flink.table.planner.{Planner, PlannerBase}
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource

import _root_.scala.annotation.varargs
import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable

/**
  * The abstract base class for batch and stream TableEnvironments.
  *
  * @param config The configuration of the TableEnvironment
  */
abstract class TableEnvironment(
    val config: TableConfig,
    private[flink] val functionCatalog: FunctionCatalog,
    private[flink] val catalogManager: CatalogManager,
    private[flink] val planner: Planner) {

  // a counter for unique attribute names
  private[flink] val attrNameCntr: AtomicInteger = new AtomicInteger(0)

  // registered external catalog names -> catalog
  private val externalCatalogs = new mutable.HashMap[String, ExternalCatalog]

  private[flink] val expressionBridge = new ExpressionBridge[PlannerExpression](functionCatalog,
    PlannerExpressionConverter.INSTANCE)

  private[flink] val operationTreeBuilder = new OperationTreeBuilder(this)


  /*
    HACKS! WORKAROUNDS!
   */
  private val rootSchema = planner.asInstanceOf[PlannerBase].rootSchema

  private[flink] def getRelBuilder = planner.asInstanceOf[PlannerBase].getRelBuilder

  private[flink] def insertInto(
      table: TableOperation,
      sinkTableName: String,
      conf: QueryConfig): Unit = {
    planner.asInstanceOf[PlannerBase].insertInto(table, sinkTableName, conf)
  }

  private[flink] def insertInto(
    table: TableOperation,
    sinkTableName: String): Unit = {
    planner.asInstanceOf[PlannerBase].insertInto(table, sinkTableName, defaultQueryConfig)
  }
  /*
    HACKS! WORKAROUNDS!
   */

  /** Returns the table config to define the runtime behavior of the Table API. */
  def getConfig: TableConfig = config

  def defaultQueryConfig: QueryConfig

  /**
    * Creates a table from a table source.
    *
    * @param source table source used as table
    */
  def fromTableSource(source: TableSource[_]): Table = {
    val name = createUniqueTableName()
    registerTableSourceInternal(name, source)
    scan(name)
  }

  /**
    * Registers an [[ExternalCatalog]] under a unique name in the TableEnvironment's schema.
    * All tables registered in the [[ExternalCatalog]] can be accessed.
    *
    * @param name            The name under which the externalCatalog will be registered
    * @param externalCatalog The externalCatalog to register
    */
  def registerExternalCatalog(name: String, externalCatalog: ExternalCatalog): Unit = {
    if (rootSchema.getSubSchema(name) != null) {
      throw new ExternalCatalogAlreadyExistException(name)
    }
    this.externalCatalogs.put(name, externalCatalog)
    // create an external catalog Calcite schema, register it on the root schema
    ExternalCatalogSchema.registerCatalog(this, rootSchema, name, externalCatalog)
  }

  /**
    * Gets a registered [[ExternalCatalog]] by name.
    *
    * @param name The name to look up the [[ExternalCatalog]]
    * @return The [[ExternalCatalog]]
    */
  def getRegisteredExternalCatalog(name: String): ExternalCatalog = {
    this.externalCatalogs.get(name) match {
      case Some(catalog) => catalog
      case None => throw new ExternalCatalogNotExistException(name)
    }
  }

  /**
    * Registers a [[ScalarFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  def registerFunction(name: String, function: ScalarFunction): Unit = {
    // check if class could be instantiated
    checkForInstantiation(function.getClass)

    functionCatalog.registerScalarFunction(name, function)
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

    functionCatalog.registerTableFunction(name, function, typeInfo)
  }

  /**
    * Registers an [[AggregateFunction]] under a unique name. Replaces already existing
    * user-defined functions under this name.
    */
  private[flink] def registerAggregateFunctionInternal[T: TypeInformation, ACC: TypeInformation](
      name: String, function: AggregateFunction[T, ACC]): Unit = {
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

    functionCatalog.registerAggregateFunction(name, function, resultTypeInfo, accTypeInfo)
  }

  /**
    * Registers a [[Table]] under a unique name in the TableEnvironment's catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name The name under which the table will be registered.
    * @param table The table to register.
    */
  def registerTable(name: String, table: Table): Unit = {

    // check that table belongs to this table environment
    if (table.asInstanceOf[TableImpl].tableEnv != this) {
      throw new TableException(
        "Only tables that belong to this TableEnvironment can be registered.")
    }

    checkValidTableName(name)
    val tableTable = new RelTable(table.asInstanceOf[TableImpl].getRelNode)
    registerTableInternal(name, tableTable)
  }

  /**
    * Registers an external [[TableSource]] in this [[TableEnvironment]]'s catalog.
    * Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  def registerTableSource(name: String, tableSource: TableSource[_]): Unit = {
    checkValidTableName(name)
    registerTableSourceInternal(name, tableSource)
  }

  /**
    * Registers an internal [[TableSource]] in this [[TableEnvironment]]'s catalog without
    * name checking. Registered tables can be referenced in SQL queries.
    *
    * @param name        The name under which the [[TableSource]] is registered.
    * @param tableSource The [[TableSource]] to register.
    */
  protected def registerTableSourceInternal(name: String, tableSource: TableSource[_]): Unit

  /**
    * Registers an external [[TableSink]] with given field names and types in this
    * [[TableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * @param name The name under which the [[TableSink]] is registered.
    * @param fieldNames The field names to register with the [[TableSink]].
    * @param fieldTypes The field types to register with the [[TableSink]].
    * @param tableSink The [[TableSink]] to register.
    */
  def registerTableSink(
      name: String,
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]],
      tableSink: TableSink[_]): Unit

  /**
    * Registers an external [[TableSink]] with already configured field names and field types in
    * this [[TableEnvironment]]'s catalog.
    * Registered sink tables can be referenced in SQL DML statements.
    *
    * @param name The name under which the [[TableSink]] is registered.
    * @param configuredSink The configured [[TableSink]] to register.
    */
  def registerTableSink(name: String, configuredSink: TableSink[_]): Unit

  /**
    * Replaces a registered Table with another Table under the same name.
    * We use this method to replace a [[org.apache.flink.table.plan.schema.DataStreamTable]]
    * with a [[org.apache.calcite.schema.TranslatableTable]].
    *
    * @param name Name of the table to replace.
    * @param table The table that replaces the previous table.
    */
  protected def replaceRegisteredTable(name: String, table: AbstractTable): Unit = {

    if (isRegistered(name)) {
      rootSchema.add(name, table)
    } else {
      throw new TableException(s"Table \'$name\' is not registered.")
    }
  }

  /**
    * Scans a registered table and returns the resulting [[Table]].
    *
    * A table to scan must be registered in the TableEnvironment. It can be either directly
    * registered as DataStream, DataSet, or Table or as member of an [[ExternalCatalog]].
    *
    * Examples:
    *
    * - Scanning a directly registered table
    * {{{
    *   val tab: Table = tableEnv.scan("tableName")
    * }}}
    *
    * - Scanning a table from a registered catalog
    * {{{
    *   val tab: Table = tableEnv.scan("catalogName", "dbName", "tableName")
    * }}}
    *
    * @param tablePath The path of the table to scan.
    * @throws TableException if no table is found using the given table path.
    * @return The resulting [[Table]].
    */
  @throws[TableException]
  @varargs
  def scan(tablePath: String*): Table = {
    scanInternal(tablePath.toArray) match {
      case Some(table) => table
      case None => throw new TableException(s"Table '${tablePath.mkString(".")}' was not found.")
    }
  }

  /**
    * Creates a table source and/or table sink from a descriptor.
    *
    * Descriptors allow for declaring the communication to external systems in an
    * implementation-agnostic way. The classpath is scanned for suitable table factories that match
    * the desired configuration.
    *
    * The following example shows how to read from a connector using a JSON format and
    * registering a table source as "MyTable":
    *
    * {{{
    *
    * tableEnv
    *   .connect(
    *     new ExternalSystemXYZ()
    *       .version("0.11"))
    *   .withFormat(
    *     new Json()
    *       .jsonSchema("{...}")
    *       .failOnMissingField(false))
    *   .withSchema(
    *     new Schema()
    *       .field("user-name", "VARCHAR").from("u_name")
    *       .field("count", "DECIMAL")
    *   .registerSource("MyTable")
    * }}}
    *
    * @param connectorDescriptor connector descriptor describing the external system
    */
  def connect(connectorDescriptor: ConnectorDescriptor): TableDescriptor

  private[flink] def scanInternal(tablePath: Array[String]): Option[Table] = {
    require(tablePath != null && !tablePath.isEmpty, "tablePath must not be null or empty.")
    val schemaPaths = tablePath.slice(0, tablePath.length - 1)
    val schema = getSchema(schemaPaths)
    if (schema != null) {
      val tableName = tablePath(tablePath.length - 1)
      val table = schema.getTable(tableName)
      if (table != null) {
        return Some(new TableImpl(this,
          new CatalogTableOperation(tablePath.toList.asJava, extractTableSchema(table))))
      }
    }
    None
  }

  private def extractTableSchema(table: schema.Table): TableSchema = {
    val relDataType = table.getRowType(getRelBuilder.getTypeFactory)
    val fieldNames = relDataType.getFieldNames
    val fieldTypes = relDataType.getFieldList.asScala
      .map(field => FlinkTypeFactory.toTypeInfo(field.getType))
    new TableSchema(fieldNames.asScala.toArray, fieldTypes.toArray)
  }

  private def getSchema(schemaPath: Array[String]): SchemaPlus = {
    var schema = rootSchema
    for (schemaName <- schemaPath) {
      schema = schema.getSubSchema(schemaName)
      if (schema == null) {
        return schema
      }
    }
    schema
  }

  /**
    * Gets the names of all tables registered in this environment.
    *
    * @return A list of the names of all registered tables.
    */
  def listTables(): Array[String] = {
    rootSchema.getTableNames.asScala.toArray
  }

  /**
    * Gets the names of all functions registered in this environment.
    */
  def listUserDefinedFunctions(): Array[String] = {
    functionCatalog.getUserDefinedFunctions.asScala.toArray
  }

  /**
    * Returns the AST of the specified Table API and SQL queries and the execution plan to compute
    * the result of the given [[Table]].
    *
    * @param table The table for which the AST and execution plan will be returned.
    */
  def explain(table: Table): String = {
    planner.explain(table.asInstanceOf[TableImpl].operationTree, defaultQueryConfig)
  }

  /**
    * Returns completion hints for the given statement at the given cursor position.
    * The completion happens case insensitively.
    *
    * @param statement Partial or slightly incorrect SQL statement
    * @param position cursor position
    * @return completion hints that fit at the current cursor position
    */
  def getCompletionHints(statement: String, position: Int): Array[String] = {
    planner.getCompletionHints(statement, position)
  }

  /**
    * Evaluates a SQL query on registered tables and retrieves the result as a [[Table]].
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[Table]] as follows:
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
  def sqlQuery(query: String): Table = {
    new TableImpl(this, planner.sqlQuery(query))
  }

  /**
    * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
    * NOTE: Currently only SQL INSERT statements are supported.
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[Table]] as follows:
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
    */
  def sqlUpdate(stmt: String): Unit = {
    sqlUpdate(stmt, defaultQueryConfig)
  }

  /**
    * Evaluates a SQL statement such as INSERT, UPDATE or DELETE; or a DDL statement;
    * NOTE: Currently only SQL INSERT statements are supported.
    *
    * All tables referenced by the query must be registered in the TableEnvironment.
    * A [[Table]] is automatically registered when its [[toString]] method is called, for example
    * when it is embedded into a String.
    * Hence, SQL queries can directly reference a [[Table]] as follows:
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
  def sqlUpdate(stmt: String, config: QueryConfig): Unit = {
    planner.sqlUpdate(stmt, config)
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

  /** Returns a unique table name according to the internal naming pattern. */
  protected def createUniqueTableName(): String

  /**
    * Checks if the chosen table name is valid.
    *
    * @param name The table name to check.
    */
  protected def checkValidTableName(name: String): Unit

  /**
    * Checks if a table is registered under the given name.
    *
    * @param name The table name to check.
    * @return true, if a table is registered under the name, false otherwise.
    */
  protected[flink] def isRegistered(name: String): Boolean = {
    rootSchema.getTableNames.contains(name)
  }

  /**
    * Get a table from either internal or external catalogs.
    *
    * @param name The name of the table.
    * @return The table registered either internally or externally, None otherwise.
    */
  protected def getTable(name: String): Option[org.apache.calcite.schema.Table] = {
    planner.asInstanceOf[PlannerBase].getTable(name)
  }

  /** Returns a unique temporary attribute name. */
  private[flink] def createUniqueAttributeName(): String = {
    "TMP_" + attrNameCntr.getAndIncrement()
  }

  private[flink] def getFunctionCatalog: FunctionCatalog = {
    functionCatalog
  }
}

/**
  * Object to instantiate a [[TableEnvironment]] depending on the batch or stream execution
  * environment.
  */
object TableEnvironment {

  /**
    * Returns a [[JavaBatchTableEnv]] for a Java [[JavaBatchExecEnv]].
    *
    * @param executionEnvironment The Java batch ExecutionEnvironment.
    *
    * @deprecated This method will be removed. Use BatchTableEnvironment.create() for Java instead.
    */
  @Deprecated
  def getTableEnvironment(executionEnvironment: JavaBatchExecEnv): JavaBatchTableEnv = {
    new JavaBatchTableEnv(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[JavaBatchTableEnv]] for a Java [[JavaBatchExecEnv]] and a given [[TableConfig]].
    *
    * @param executionEnvironment The Java batch ExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    *
    * @deprecated This method will be removed. Use BatchTableEnvironment.create() for Java instead.
    */
  @Deprecated
  def getTableEnvironment(
    executionEnvironment: JavaBatchExecEnv,
    tableConfig: TableConfig): JavaBatchTableEnv = {

    new JavaBatchTableEnv(executionEnvironment, tableConfig)
  }

  /**
    * Returns a [[ScalaBatchTableEnv]] for a Scala [[ScalaBatchExecEnv]].
    *
    * @param executionEnvironment The Scala batch ExecutionEnvironment.
    */
  @deprecated(
    "This method will be removed. Use BatchTableEnvironment.create() for Scala instead.",
    "1.8.0")
  def getTableEnvironment(executionEnvironment: ScalaBatchExecEnv): ScalaBatchTableEnv = {
    new ScalaBatchTableEnv(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[ScalaBatchTableEnv]] for a Scala [[ScalaBatchExecEnv]] and a given
    * [[TableConfig]].
    *
    * @param executionEnvironment The Scala batch ExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  @deprecated(
    "This method will be removed. Use BatchTableEnvironment.create() for Scala instead.",
    "1.8.0")
  def getTableEnvironment(
    executionEnvironment: ScalaBatchExecEnv,
    tableConfig: TableConfig): ScalaBatchTableEnv = {

    new ScalaBatchTableEnv(executionEnvironment, tableConfig)
  }

  /**
    * Returns a [[JavaStreamTableEnv]] for a Java [[JavaStreamExecEnv]].
    *
    * @param executionEnvironment The Java StreamExecutionEnvironment.
    *
    * @deprecated This method will be removed. Use StreamTableEnvironment.create() for Java instead.
    */
  @Deprecated
  def getTableEnvironment(executionEnvironment: JavaStreamExecEnv): JavaStreamTableEnv = {
    new JavaStreamTableEnv(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[JavaStreamTableEnv]] for a Java [[JavaStreamExecEnv]] and a given [[TableConfig]].
    *
    * @param executionEnvironment The Java StreamExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    *
    * @deprecated This method will be removed. Use StreamTableEnvironment.create() for Java instead.
    */
  @Deprecated
  def getTableEnvironment(
    executionEnvironment: JavaStreamExecEnv,
    tableConfig: TableConfig): JavaStreamTableEnv = {

    new JavaStreamTableEnv(executionEnvironment, tableConfig)
  }

  /**
    * Returns a [[ScalaStreamTableEnv]] for a Scala stream [[ScalaStreamExecEnv]].
    *
    * @param executionEnvironment The Scala StreamExecutionEnvironment.
    */
  @deprecated(
    "This method will be removed. Use StreamTableEnvironment.create() for Scala instead.",
    "1.8.0")
  def getTableEnvironment(executionEnvironment: ScalaStreamExecEnv): ScalaStreamTableEnv = {
    new ScalaStreamTableEnv(executionEnvironment, new TableConfig())
  }

  /**
    * Returns a [[ScalaStreamTableEnv]] for a Scala stream [[ScalaStreamExecEnv]].
    *
    * @param executionEnvironment The Scala StreamExecutionEnvironment.
    * @param tableConfig The TableConfig for the new TableEnvironment.
    */
  @deprecated(
    "This method will be removed. Use StreamTableEnvironment.create() for Scala instead.",
    "1.8.0")
  def getTableEnvironment(
    executionEnvironment: ScalaStreamExecEnv,
    tableConfig: TableConfig): ScalaStreamTableEnv = {

    new ScalaStreamTableEnv(executionEnvironment, tableConfig)
  }

  /**
    * Returns field names for a given [[TypeInformation]].
    *
    * @param inputType The TypeInformation extract the field names.
    * @tparam A The type of the TypeInformation.
    * @return An array holding the field names
    */
  def getFieldNames[A](inputType: TypeInformation[A]): Array[String] = {
    validateType(inputType)

    val fieldNames: Array[String] = inputType match {
      case t: CompositeType[_] => t.getFieldNames
      case _: TypeInformation[_] => Array("f0")
    }

    if (fieldNames.contains("*")) {
      throw new TableException("Field name can not be '*'.")
    }

    fieldNames
  }

  /**
    * Validate if class represented by the typeInfo is static and globally accessible
    * @param typeInfo type to check
    * @throws TableException if type does not meet these criteria
    */
  def validateType(typeInfo: TypeInformation[_]): Unit = {
    val clazz = typeInfo.getTypeClass
    if ((clazz.isMemberClass && !Modifier.isStatic(clazz.getModifiers)) ||
      !Modifier.isPublic(clazz.getModifiers) ||
      clazz.getCanonicalName == null) {
      throw new TableException(
        s"Class '$clazz' described in type information '$typeInfo' must be " +
        s"static and globally accessible.")
    }
  }

  /**
    * Returns field indexes for a given [[TypeInformation]].
    *
    * @param inputType The TypeInformation extract the field positions from.
    * @return An array holding the field positions
    */
  def getFieldIndices(inputType: TypeInformation[_]): Array[Int] = {
    getFieldNames(inputType).indices.toArray
  }

  /**
    * Returns field types for a given [[TypeInformation]].
    *
    * @param inputType The TypeInformation to extract field types from.
    * @return An array holding the field types.
    */
  def getFieldTypes(inputType: TypeInformation[_]): Array[TypeInformation[_]] = {
    validateType(inputType)

    inputType match {
      case ct: CompositeType[_] => 0.until(ct.getArity).map(i => ct.getTypeAt(i)).toArray
      case t: TypeInformation[_] => Array(t.asInstanceOf[TypeInformation[_]])
    }
  }
}
