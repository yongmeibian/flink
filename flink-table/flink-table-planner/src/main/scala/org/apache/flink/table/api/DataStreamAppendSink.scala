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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.sinks.AppendStreamTableSink

class DataStreamAppendSink[T](val outputType: TypeInformation[T]) extends AppendStreamTableSink[T]{

  private var underlyingDataStream: DataStream[T] = _

  override def emitDataStream(dataStream: DataStream[T]): Unit = {
    underlyingDataStream = dataStream
  }

  override def getOutputType: TypeInformation[T] = outputType

  override def getFieldNames = throw new UnsupportedOperationException("Should never be called")

  override def getFieldTypes = throw new UnsupportedOperationException("Should never be called")

  override def configure(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]) =
    throw new UnsupportedOperationException("Should never be called")

  def getUnderlyingStream: DataStream[T] = underlyingDataStream
}
