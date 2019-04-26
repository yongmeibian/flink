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

package org.apache.flink.table.catalog

import org.apache.flink.table.api._
import org.apache.flink.table.factories._
import org.apache.flink.table.plan.schema._
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sinks.{BatchTableSink, StreamTableSink}
import org.apache.flink.table.sources.{BatchTableSource, StreamTableSource}
import org.apache.flink.table.util.JavaScalaConversionUtil.toScala
import org.apache.flink.table.util.Logging

/**
  * The utility class is used to convert [[ExternalCatalogTable]] to [[TableSourceSinkTable]].
  *
  * It uses [[TableFactoryService]] for discovering.
  */
object ExternalTableUtil extends Logging {

  /**
    * Converts an [[ExternalCatalogTable]] instance to a [[TableSourceTable]] instance
    *
    * @param externalTable the [[ExternalCatalogTable]] instance which to convert
    * @return converted [[TableSourceTable]] instance from the input catalog table
    */
  def fromExternalCatalogTable[T1, T2](
      externalTable: ExternalCatalogTable)
    : TableSourceSinkTable[T1, T2] = {

    val statistics = new FlinkStatistic(toScala(externalTable.getTableStats))

    val source: Option[TableSourceTable[T1]] = if (externalTable.isTableSource) {
      Some(createTableSource(externalTable, statistics))
    } else {
      None
    }

    val sink: Option[TableSinkTable[T2]] = if (externalTable.isTableSink) {
      Some(createTableSink(externalTable, statistics))
    } else {
      None
    }

    new TableSourceSinkTable[T1, T2](source, sink)
  }

  def getTableSchema(externalTable: ExternalCatalogTable): TableSchema = {
    if (externalTable.isTableSource) {
      TableFactoryUtil.findAndCreateTableSource[Any](externalTable).getTableSchema
    } else {
      val tableSink = TableFactoryUtil.findAndCreateTableSink(externalTable)
      new TableSchema(tableSink.getFieldNames, tableSink.getFieldTypes)
    }
  }

  private def createTableSource[T](
      externalTable: ExternalCatalogTable,
      statistics: FlinkStatistic)
    : TableSourceTable[T] = {

    TableFactoryUtil.findAndCreateTableSource(externalTable) match {
      case source: BatchTableSource[T] =>
        new BatchTableSourceTable[T](source, statistics)

      case source: StreamTableSource[T] =>
        new StreamTableSourceTable[T](source, statistics)

      case _ =>
        throw new ValidationException(
          "External catalog table does not support the current environment for a table source.")
    }

  }

  private def createTableSink[T](
      externalTable: ExternalCatalogTable,
      statistics: FlinkStatistic)
    : TableSinkTable[T] = {

    TableFactoryUtil.findAndCreateTableSink(externalTable) match {

      case sink: BatchTableSink[T] =>
        new TableSinkTable[T](sink, statistics)

      case sink: StreamTableSink[T] =>
        new TableSinkTable[T](sink, statistics)

      case _ =>
        throw new ValidationException(
          "External catalog table does not support the current environment for a table sink.")
    }
  }
}
