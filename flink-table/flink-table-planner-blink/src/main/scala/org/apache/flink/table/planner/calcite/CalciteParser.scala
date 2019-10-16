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

/**
 * Thin wrapper around [[SqlParser]] that does exception conversion and [[SqlNode]] casting.
 */
class CalciteParser(config: SqlParser.Config) {

  /**
   * Parses a SQL statement into a [[SqlNode]]. The [[SqlNode]] is not yet validated.
   *
   * @param sql a sql string to parse
   * @return a parsed sql node
   * @throws SqlParserException if an exception is thrown when parsing the statement
   */
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

  /**
   * Parses a SQL string as an identifier into a [[SqlIdentifier]].
   *
   * @param identifier a sql string to parse as an identifier
   * @return a parsed sql node
   * @throws SqlParserException if an exception is thrown when parsing the identifier
   */
  def parseIdentifier(identifier: String): SqlIdentifier = {
    try {
      val parser: SqlParser = SqlParser.create(identifier, config)
      val sqlNode: SqlNode = parser.parseExpression()
      sqlNode.asInstanceOf[SqlIdentifier]
    } catch {
      case _: Exception =>
        throw new SqlParserException(s"Invalid sql identifier: $identifier")
    }
  }
}
