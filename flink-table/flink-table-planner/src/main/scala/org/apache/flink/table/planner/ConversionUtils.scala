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

import java.lang.{Boolean => JBool}

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, PojoTypeInfo, TupleTypeInfo, TupleTypeInfoBase}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.table.api.{TableConfig, TableException, Types}
import org.apache.flink.table.codegen.{FunctionCodeGenerator, GeneratedFunction}
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.conversion.{CRowToJavaTupleMapFunction, CRowToJavaTupleMapRunner, CRowToScalaTupleMapFunction, CRowToScalaTupleMapRunner}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.types.Row

object ConversionUtils {
  /**
    * Creates a converter that maps the internal CRow type to Scala or Java Tuple2 with change flag.
    *
    * @param physicalTypeInfo the input of the sink
    * @param schema the input schema with correct field names (esp. for POJO field mapping)
    * @param requestedTypeInfo the output type of the sink.
    * @param functionName name of the map function. Must not be unique but has to be a
    *                     valid Java class identifier.
    */
  def getConversionMapperWithChanges[OUT](
    physicalTypeInfo: TypeInformation[CRow],
    schema: RowSchema,
    requestedTypeInfo: TypeInformation[OUT],
    functionName: String,
    config: TableConfig)
  : MapFunction[CRow, OUT] = requestedTypeInfo match {

    // Scala tuple
    case t: CaseClassTypeInfo[_]
      if t.getTypeClass == classOf[(_, _)] && t.getTypeAt(0) == Types.BOOLEAN =>

      val reqType = t.getTypeAt[Any](1)

      // convert Row into requested type and wrap result in Tuple2
      val converterFunction = generateRowConverterFunction(
        physicalTypeInfo.asInstanceOf[CRowTypeInfo].rowType,
        schema,
        reqType,
        functionName,
        config
      )

      converterFunction match {

        case Some(func) =>
          new CRowToScalaTupleMapRunner(
            func.name,
            func.code,
            requestedTypeInfo.asInstanceOf[TypeInformation[(Boolean, Any)]]
          ).asInstanceOf[MapFunction[CRow, OUT]]

        case _ =>
          new CRowToScalaTupleMapFunction().asInstanceOf[MapFunction[CRow, OUT]]
      }

    // Java tuple
    case t: TupleTypeInfo[_]
      if t.getTypeClass == classOf[JTuple2[_, _]] && t.getTypeAt(0) == Types.BOOLEAN =>

      val reqType = t.getTypeAt[Any](1)

      // convert Row into requested type and wrap result in Tuple2
      val converterFunction = generateRowConverterFunction(
        physicalTypeInfo.asInstanceOf[CRowTypeInfo].rowType,
        schema,
        reqType,
        functionName,
        config
      )

      converterFunction match {

        case Some(func) =>
          new CRowToJavaTupleMapRunner(
            func.name,
            func.code,
            requestedTypeInfo.asInstanceOf[TypeInformation[JTuple2[JBool, Any]]]
          ).asInstanceOf[MapFunction[CRow, OUT]]

        case _ =>
          new CRowToJavaTupleMapFunction().asInstanceOf[MapFunction[CRow, OUT]]
      }
  }

  def generateRowConverterFunction[OUT](
    inputTypeInfo: TypeInformation[Row],
    schema: RowSchema,
    requestedTypeInfo: TypeInformation[OUT],
    functionName: String,
    config: TableConfig)
  : Option[GeneratedFunction[MapFunction[Row, OUT], OUT]] = {

    // validate that at least the field types of physical and logical type match
    // we do that here to make sure that plan translation was correct
    if (schema.typeInfo != inputTypeInfo) {
      throw new TableException(
        s"The field types of physical and logical row types do not match. " +
          s"Physical type is [${schema.typeInfo}], Logical type is [$inputTypeInfo]. " +
          s"This is a bug and should not happen. Please file an issue.")
    }

    // generic row needs no conversion
    if (requestedTypeInfo.isInstanceOf[GenericTypeInfo[_]] &&
      requestedTypeInfo.getTypeClass == classOf[Row]) {
      return None
    }

    val fieldTypes = schema.fieldTypeInfos
    val fieldNames = schema.fieldNames

    // check for valid type info
    if (requestedTypeInfo.getArity != fieldTypes.length) {
      throw new TableException(
        s"Arity [${fieldTypes.length}] of result [$fieldTypes] does not match " +
          s"the number[${requestedTypeInfo.getArity}] of requested type [$requestedTypeInfo].")
    }

    // check requested types

    def validateFieldType(fieldType: TypeInformation[_]): Unit = fieldType match {
      case _: TimeIndicatorTypeInfo =>
        throw new TableException("The time indicator type is an internal type only.")
      case _ => // ok
    }

    requestedTypeInfo match {
      // POJO type requested
      case pt: PojoTypeInfo[_] =>
        fieldNames.zip(fieldTypes) foreach {
          case (fName, fType) =>
            val pojoIdx = pt.getFieldIndex(fName)
            if (pojoIdx < 0) {
              throw new TableException(s"POJO does not define field name: $fName")
            }
            val requestedTypeInfo = pt.getTypeAt(pojoIdx)
            validateFieldType(requestedTypeInfo)
            if (fType != requestedTypeInfo) {
              throw new TableException(s"Result field does not match requested type. " +
                s"Requested: $requestedTypeInfo; Actual: $fType")
            }
        }

      // Tuple/Case class/Row type requested
      case tt: TupleTypeInfoBase[_] =>
        fieldTypes.zipWithIndex foreach {
          case (fieldTypeInfo, i) =>
            val requestedTypeInfo = tt.getTypeAt(i)
            validateFieldType(requestedTypeInfo)
            if (fieldTypeInfo != requestedTypeInfo) {
              throw new TableException(s"Result field does not match requested type. " +
                s"Requested: $requestedTypeInfo; Actual: $fieldTypeInfo")
            }
        }

      // atomic type requested
      case t: TypeInformation[_] =>
        if (fieldTypes.size != 1) {
          throw new TableException(s"Requested result type is an atomic type but " +
            s"result[$fieldTypes] has more or less than a single field.")
        }
        val requestedTypeInfo = fieldTypes.head
        validateFieldType(requestedTypeInfo)
        if (requestedTypeInfo != t) {
          throw new TableException(s"Result field does not match requested type. " +
            s"Requested: $t; Actual: $requestedTypeInfo")
        }

      case _ =>
        throw new TableException(s"Unsupported result type: $requestedTypeInfo")
    }

    // code generate MapFunction
    val generator = new FunctionCodeGenerator(
      config,
      false,
      inputTypeInfo,
      None,
      None)

    val conversion = generator.generateConverterResultExpression(
      requestedTypeInfo,
      fieldNames)

    val body =
      s"""
         |${conversion.code}
         |return ${conversion.resultTerm};
         |""".stripMargin

    val generated = generator.generateFunction(
      functionName,
      classOf[MapFunction[Row, OUT]],
      body,
      requestedTypeInfo)

    Some(generated)
  }
}
