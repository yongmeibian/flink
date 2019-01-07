package org.apache.flink.table.api.scala

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.base.BaseTableEnvironment
import org.apache.flink.table.functions.{AggregateFunction, TableFunction}

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

trait STableEnvironment extends BaseTableEnvironment{

  /**
    * Registers a [[TableFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param tf The TableFunction to register.
    * @tparam T The type of the output row.
    */
  def registerFunction[T: TypeInformation](name: String, tf: TableFunction[T]): Unit = {
    registerTableFunctionInternal(name, tf)
  }

  /**
    * Registers an [[AggregateFunction]] under a unique name in the TableEnvironment's catalog.
    * Registered functions can be referenced in Table API and SQL queries.
    *
    * @param name The name under which the function is registered.
    * @param f The AggregateFunction to register.
    * @tparam T The type of the output value.
    * @tparam ACC The type of aggregate accumulator.
    */
  def registerFunction[T: TypeInformation, ACC: TypeInformation](
    name: String,
    f: AggregateFunction[T, ACC])
  : Unit = {
    registerAggregateFunctionInternal[T, ACC](name, f)
  }

}

object STableEnvironment {
  def getTableEnvironment(environment: ExecutionEnvironment): SBatchTableEnvironment = {
    new SBatchTableEnvironment(environment, new TableConfig)
  }

  def getTableEnvironment(environment: StreamExecutionEnvironment): SStreamTableEnvironment = {
    new SStreamTableEnvironment(environment, new TableConfig)
  }
}
