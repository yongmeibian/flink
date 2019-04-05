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

package org.apache.flink.table.codegen

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, PojoTypeInfo, TupleTypeInfoBase}
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.types.Row

object RowConverterGenerator {

  def generateRowConverterFunction[OUT](
      inputTypeInfo: TypeInformation[Row],
      schema: RowSchema,
      requestedTypeInfo: TypeInformation[OUT],
      functionName: String,
      tableConfig: TableConfig)
    : Option[GeneratedFunction[MapFunction[Row, OUT], OUT]] = {

    checkIfLogicalSchemaMatchPhysical(inputTypeInfo, schema)

    // generic row needs no conversion
    if (isGenericRow(requestedTypeInfo)) {
      return None
    }

    val fieldTypes = schema.fieldTypeInfos
    val fieldNames = schema.fieldNames

    // check requested types

    checkTypesMatchExpected(requestedTypeInfo, fieldTypes, fieldNames)

    // code generate MapFunction
    val generator = new FunctionCodeGenerator(
      tableConfig,
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

  private def checkTypesMatchExpected[OUT](
      expectedTypeInfo: TypeInformation[OUT],
      actualTypes: Seq[TypeInformation[_]],
      actualNames: Seq[String])
    : Unit = {

    // check for valid type info
    if (expectedTypeInfo.getArity != actualTypes.length) {
      throw new TableException(
        s"Arity [${actualTypes.length}] of result [$actualTypes] does not match " +
          s"the number[${expectedTypeInfo.getArity}] of requested type [$expectedTypeInfo].")
    }

    expectedTypeInfo match {
      // POJO type requested
      case pt: PojoTypeInfo[_] =>
        val fields = actualNames.zip(actualTypes)
        checkPojoTypeMatch(pt, fields)

      // Tuple/Case class/Row type requested
      case tt: TupleTypeInfoBase[_] =>
        actualTypes.zipWithIndex foreach {
          case (fieldTypeInfo, i) =>
            val requestedTypeInfo = tt.getTypeAt(i)
            validateExpectedType(requestedTypeInfo, fieldTypeInfo)
        }

      // atomic type requested
      case t: TypeInformation[_] =>
        if (actualTypes.size != 1) {
          throw new TableException(s"Requested result type is an atomic type but " +
            s"result[$actualTypes] has more or less than a single field.")
        }
        val requestedTypeInfo = actualTypes.head
        validateExpectedType(requestedTypeInfo, t)

      case _ =>
        throw new TableException(s"Unsupported result type: $expectedTypeInfo")
    }
  }

  private def checkPojoTypeMatch[OUT](
      expectedType: PojoTypeInfo[_],
      actualTypes: Seq[(String, TypeInformation[_])])
    : Unit = {
    actualTypes foreach {
      case (fName, fType) =>
        val pojoIdx = expectedType.getFieldIndex(fName)
        if (pojoIdx < 0) {
          throw new TableException(s"POJO does not define field name: $fName")
        }
        val requestedTypeInfo = expectedType.getTypeAt(pojoIdx)
        validateExpectedType(requestedTypeInfo, fType)
    }
  }

  private def validateExpectedType(
      expectedType: TypeInformation[_],
      actualType: TypeInformation[_])
    : Unit = {
    validateFieldType(expectedType)
    if (actualType != expectedType) {
      throw new TableException(s"Result field does not match requested type. " +
        s"Requested: $expectedType; Actual: $actualType")
    }
  }

  private def isGenericRow[OUT](requestedTypeInfo: TypeInformation[OUT]): Boolean = {
    requestedTypeInfo.isInstanceOf[GenericTypeInfo[_]] &&
      requestedTypeInfo.getTypeClass == classOf[Row]
  }

  private def checkIfLogicalSchemaMatchPhysical[OUT](
      inputTypeInfo: TypeInformation[Row],
      schema: RowSchema)
    : Unit = {
    // validate that at least the field types of physical and logical type match
    // we do that here to make sure that plan translation was correct
    if (schema.typeInfo != inputTypeInfo) {
      throw new TableException(
        s"The field types of physical and logical row types do not match. " +
          s"Physical type is [${schema.typeInfo}], Logical type is [$inputTypeInfo]. " +
          s"This is a bug and should not happen. Please file an issue.")
    }
  }

  private def validateFieldType(fieldType: TypeInformation[_]): Unit = fieldType match {
    case _: TimeIndicatorTypeInfo =>
      throw new TableException("The time indicator type is an internal type only.")
    case _ => // ok
  }
}
