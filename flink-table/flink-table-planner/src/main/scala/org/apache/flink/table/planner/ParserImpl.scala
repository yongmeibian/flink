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

import org.apache.flink.sql.parser.dml.RichSqlInsert
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.calcite.{CalciteParser, FlinkPlannerImpl}
import org.apache.flink.table.catalog.{CatalogManager, UnresolvedIdentifier}
import org.apache.flink.table.delegation.Parser
import org.apache.flink.table.operations.Operation
import org.apache.flink.table.sqlexec.SqlToOperationConverter

import org.apache.calcite.sql.SqlKind

import java.util.{List => JList}

import scala.collection.JavaConversions._

class ParserImpl(
    catalogManager: CatalogManager,
    validatorProvider: () => FlinkPlannerImpl,
    calciteParserProvider: () => CalciteParser
  ) extends Parser {

  override def parse(stmt: String): JList[Operation] = {
    val planner = validatorProvider.apply()
    val parser = calciteParserProvider.apply()
    // parse the sql query
    val parsed = parser.parse(stmt)

    parsed match {
      case insert: RichSqlInsert =>
        val targetColumnList = insert.getTargetColumnList
        if (targetColumnList != null && insert.getTargetColumnList.size() != 0) {
          throw new ValidationException("Partial inserts are not supported")
        }
        List(SqlToOperationConverter.convert(planner, catalogManager, insert))
      case node if node.getKind.belongsTo(SqlKind.QUERY) || node.getKind.belongsTo(SqlKind.DDL) =>
        List(SqlToOperationConverter.convert(planner, catalogManager, parsed))
      case _ =>
        throw new TableException(
          "Unsupported SQL query! parse() only accepts SQL queries of type " +
            "SELECT, UNION, INTERSECT, EXCEPT, VALUES, ORDER_BY or INSERT;" +
            "and SQL DDLs of type " +
            "CREATE TABLE")
    }
  }

  override def parseIdentifier(identifier: String): UnresolvedIdentifier = {
    val parser = calciteParserProvider.apply()
    UnresolvedIdentifier.of(parser.parseIdentifier(identifier).names: _*)
  }
}
