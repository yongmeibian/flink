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

import org.apache.calcite.rel.RelNode
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api._
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{FunctionCodeGenerator, GeneratedFunction}
import org.apache.flink.table.runtime.conversion._
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.runtime.{CRowMapRunner, OutputRowtimeProcessFunction}
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.types.Row

object ConversionUtils {

  /**
    * Translates a logical [[RelNode]] into a [[DataStream]].
    *
    * @param plan The root node of the relational expression tree.
    * @param logicalType The row type of the result. Since the logicalPlan can lose the
    *                    field naming during optimization we pass the row type separately.
    * @param queryConfig     The configuration for the query to generate.
    * @param withChangeFlag Set to true to emit records with change flags.
    * @param tpe         The [[TypeInformation]] of the resulting [[DataStream]].
    * @tparam A The type of the resulting [[DataStream]].
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  def convert[A](
      plan: DataStream[CRow],
      logicalType: TableSchema,
      queryConfig: StreamQueryConfig,
      withChangeFlag: Boolean,
      tpe: TypeInformation[A],
      config: TableConfig)
    : DataStream[A] = {

    val rowtimeFields = logicalType
      .getFieldTypes.zip(logicalType.getFieldNames).zipWithIndex
      .filter(f => FlinkTypeFactory.isRowtimeIndicatorType(f._1._1))

    // convert the input type for the conversion mapper
    // the input will be changed in the OutputRowtimeProcessFunction later
    val convType = if (rowtimeFields.length > 1) {
      throw new TableException(
        s"Found more than one rowtime field: [${rowtimeFields.map(_._1._2).mkString(", ")}] in " +
          s"the table that should be converted to a DataStream.\n" +
          s"Please select the rowtime field that should be used as event-time timestamp for the " +
          s"DataStream by casting all other fields to TIMESTAMP.")
    } else if (rowtimeFields.length == 1) {
      val origRowType = plan.getType.asInstanceOf[CRowTypeInfo].rowType
      val convFieldTypes = origRowType.getFieldTypes.map { t =>
        if (FlinkTypeFactory.isRowtimeIndicatorType(t)) {
          SqlTimeTypeInfo.TIMESTAMP
        } else {
          t
        }
      }
      CRowTypeInfo(new RowTypeInfo(convFieldTypes, origRowType.getFieldNames))
    } else {
      plan.getType
    }

    // convert CRow to output type
    val conversion: MapFunction[CRow, A] = if (withChangeFlag) {
      ConversionUtils.getConversionMapperWithChanges(
        convType,
        logicalType,
        tpe,
        "DataStreamSinkConversion",
        config)
    } else {
      getConversionMapper(
        convType,
        logicalType,
        tpe,
        "DataStreamSinkConversion",
        config)
    }

    val rootParallelism = plan.getParallelism

    val withRowtime = if (rowtimeFields.isEmpty) {
      // no rowtime field to set
      plan.map(conversion)
    } else {
      // set the only rowtime field as event-time timestamp for DataStream
      // and convert it to SQL timestamp
      plan.process(new OutputRowtimeProcessFunction[A](conversion, rowtimeFields.head._2))
    }

    withRowtime
      .returns(tpe)
      .name(s"to: ${tpe.getTypeClass.getSimpleName}")
      .setParallelism(rootParallelism)
  }

  /**
    * Creates a final converter that maps the internal row type to external type.
    *
    * @param inputTypeInfo the input of the sink
    * @param schema the input schema with correct field names (esp. for POJO field mapping)
    * @param requestedTypeInfo the output type of the sink
    * @param functionName name of the map function. Must not be unique but has to be a
    *                     valid Java class identifier.
    */
  private def getConversionMapper[OUT](
      inputTypeInfo: TypeInformation[CRow],
      schema: TableSchema,
      requestedTypeInfo: TypeInformation[OUT],
      functionName: String,
      config: TableConfig)
    : MapFunction[CRow, OUT] = {

    val converterFunction = ConversionUtils.generateRowConverterFunction[OUT](
      inputTypeInfo.asInstanceOf[CRowTypeInfo].rowType,
      schema,
      requestedTypeInfo,
      functionName,
      config
    )

    converterFunction match {

      case Some(func) =>
        new CRowMapRunner[OUT](func.name, func.code, func.returnType)

      case _ =>
        new CRowToRowMapFunction().asInstanceOf[MapFunction[CRow, OUT]]
    }
  }

  /**
    * Creates a converter that maps the internal CRow type to Scala or Java Tuple2 with change flag.
    *
    * @param physicalTypeInfo the input of the sink
    * @param schema the input schema with correct field names (esp. for POJO field mapping)
    * @param requestedTypeInfo the output type of the sink.
    * @param functionName name of the map function. Must not be unique but has to be a
    *                     valid Java class identifier.
    */
  private def getConversionMapperWithChanges[OUT](
    physicalTypeInfo: TypeInformation[CRow],
    schema: TableSchema,
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

  private def generateRowConverterFunction[OUT](
    inputTypeInfo: TypeInformation[Row],
    schema: TableSchema,
    requestedTypeInfo: TypeInformation[OUT],
    functionName: String,
    config: TableConfig)
  : Option[GeneratedFunction[MapFunction[Row, OUT], OUT]] = {

    // validate that at least the field types of physical and logical type match
    // we do that here to make sure that plan translation was correct
    val typeInfo = schema.toRowType
    if (typeInfo != inputTypeInfo) {
      throw new TableException(
        s"The field types of physical and logical row types do not match. " +
          s"Physical type is [$typeInfo], Logical type is [$inputTypeInfo]. " +
          s"This is a bug and should not happen. Please file an issue.")
    }

    // generic row needs no conversion
    if (requestedTypeInfo.isInstanceOf[GenericTypeInfo[_]] &&
      requestedTypeInfo.getTypeClass == classOf[Row]) {
      return None
    }

    val fieldTypes = schema.getFieldTypes
    val fieldNames = schema.getFieldNames

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
