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

package org.apache.flink.table.planner.calcite

import org.apache.flink.table.api.SqlParserException

import org.apache.calcite.sql.parser.{SqlParser, SqlParseException => CSqlParseException}
import org.apache.calcite.sql.{SqlIdentifier, SqlNode}

import scala.util.{Failure, Success, Try}

class FlinkParser(config: SqlParser.Config) {
  def parse(sql: String): SqlNode = {
    try {
      val parser: SqlParser = SqlParser.create(sql, config)
      val sqlNode: SqlNode = parser.parseStmt
      sqlNode
    } catch {
      case e: CSqlParseException =>
        throw new SqlParserException(s"SQL parse failed. ${e.getMessage}", e)
    }
  }

  def parseIdentifier(identifier: String): SqlIdentifier = {
    val quoting = config.quoting().string
    Try(doParseIdentifier(identifier))
      .recoverWith  {
        case _ => Try(doParseIdentifier(s"$quoting$identifier$quoting"))
      } match {
      case Success(value) => value
      case Failure(e) => throw new SqlParserException(s"SQL parse failed. ${e.getMessage}", e)
    }
  }

  private def doParseIdentifier(identifier: String): SqlIdentifier = {
    val parser: SqlParser = SqlParser.create(identifier, config)
    val sqlNode: SqlNode = parser.parseExpression()
    sqlNode.asInstanceOf[SqlIdentifier]
  }
}
