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

package org.apache.flink.table.planner.delegation

import org.apache.flink.sql.parser.dml.RichSqlInsert
import org.apache.flink.table.api.TableException
import org.apache.flink.table.catalog.{CatalogManager, UnresolvedIdentifier}
import org.apache.flink.table.delegation.Parser
import org.apache.flink.table.operations.Operation
import org.apache.flink.table.planner.calcite.{CalciteParser, FlinkPlannerImpl}
import org.apache.flink.table.planner.operations.SqlToOperationConverter

import org.apache.calcite.sql.SqlKind

import java.util

import scala.collection.JavaConversions._

class ParserImpl(
    catalogManager: CatalogManager,
    validatorProvider: () => FlinkPlannerImpl,
    calciteParserProvider: () => CalciteParser
  ) extends Parser {

  override def parse(statement: String): util.List[Operation] = {
    val parser = calciteParserProvider.apply()
    val planner = validatorProvider.apply()
    // parse the sql query
    val parsed = parser.parse(statement)
    parsed match {
      case insert: RichSqlInsert =>
        List(SqlToOperationConverter.convert(planner, catalogManager, insert))
      case query if query.getKind.belongsTo(SqlKind.QUERY) =>
        List(SqlToOperationConverter.convert(planner, catalogManager, query))
      case ddl if ddl.getKind.belongsTo(SqlKind.DDL) =>
        List(SqlToOperationConverter.convert(planner, catalogManager, ddl))
      case _ =>
        throw new TableException(s"Unsupported query: $statement")
    }
  }

  override def parseIdentifier(identifier: String): UnresolvedIdentifier = {
    val parser = calciteParserProvider.apply()
    val sqlIdentifier = parser.parseIdentifier(identifier)
    UnresolvedIdentifier.of(sqlIdentifier.names: _*)
  }
}
