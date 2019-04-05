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

package org.apache.flink.table.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.expressions.{Alias, Expression, TimeAttribute, UnresolvedFieldReference}
import org.apache.flink.types.Row

object TypeUtils {

  /**
    * Reference input fields by name:
    * All fields in the schema definition are referenced by name
    * (and possibly renamed using an alias (as). In this mode, fields can be reordered and
    * projected out. Moreover, we can define proctime and rowtime attributes at arbitrary
    * positions using arbitrary names (except those that exist in the result schema). This mode
    * can be used for any input type, including POJOs.
    *
    * Reference input fields by position:
    * In this mode, fields are simply renamed. Event-time attributes can
    * replace the field on their position in the input data (if it is of correct type) or be
    * appended at the end. Proctime attributes must be appended at the end. This mode can only be
    * used if the input type has a defined field order (tuple, case class, Row) and no of fields
    * references a field of the input type.
    */
  def isReferenceByPosition(ct: CompositeType[_], fields: Array[Expression]): Boolean = {
    if (!ct.isInstanceOf[TupleTypeInfoBase[_]]) {
      return false
    }

    val inputNames = ct.getFieldNames

    // Use the by-position mode if no of the fields exists in the input.
    // This prevents confusing cases like ('f2, 'f0, 'myName) for a Tuple3 where fields are renamed
    // by position but the user might assume reordering instead of renaming.
    fields.forall {
      case UnresolvedFieldReference(name) => !inputNames.contains(name)
      case _ => true
    }
  }

  /**
    * Returns field names and field positions for a given [[TypeInformation]].
    *
    * @param inputType The TypeInformation extract the field names and positions from.
    * @tparam A The type of the TypeInformation.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  def getFieldInfo[A](inputType: TypeInformation[A]): (Array[String], Array[Int]) = {

    if (inputType.isInstanceOf[GenericTypeInfo[A]] && inputType.getTypeClass == classOf[Row]) {
      throw new TableException(
        "An input of GenericTypeInfo<Row> cannot be converted to Table. " +
          "Please specify the type of the input with a RowTypeInfo.")
    } else {
      (TableEnvironment.getFieldNames(inputType), TableEnvironment.getFieldIndices(inputType))
    }
  }

  /**
    * Returns field names and field positions for a given [[TypeInformation]] and [[Array]] of
    * [[Expression]]. It does not handle time attributes but considers them in indices.
    *
    * @param inputType The [[TypeInformation]] against which the [[Expression]]s are evaluated.
    * @param exprs     The expressions that define the field names.
    * @tparam A The type of the TypeInformation.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  def getFieldInfo[A](
      inputType: TypeInformation[A],
      exprs: Array[Expression])
    : (Array[String], Array[Int]) = {

    TableEnvironment.validateType(inputType)

    def referenceByName(name: String, ct: CompositeType[_]): Option[Int] = {
      val inputIdx = ct.getFieldIndex(name)
      if (inputIdx < 0) {
        throw new TableException(s"$name is not a field of type $ct. " +
          s"Expected: ${ct.getFieldNames.mkString(", ")}")
      } else {
        Some(inputIdx)
      }
    }

    val indexedNames: Array[(Int, String)] = inputType match {

      case g: GenericTypeInfo[A] if g.getTypeClass == classOf[Row] =>
        throw new TableException(
          "An input of GenericTypeInfo<Row> cannot be converted to Table. " +
            "Please specify the type of the input with a RowTypeInfo.")

      case t: TupleTypeInfoBase[A] if t.isInstanceOf[TupleTypeInfo[A]] ||
        t.isInstanceOf[CaseClassTypeInfo[A]] || t.isInstanceOf[RowTypeInfo] =>

        // determine schema definition mode (by position or by name)
        val isRefByPos = isReferenceByPosition(t, exprs)

        exprs.zipWithIndex flatMap {
          case (UnresolvedFieldReference(name: String), idx) =>
            if (isRefByPos) {
              Some((idx, name))
            } else {
              referenceByName(name, t).map((_, name))
            }
          case (Alias(UnresolvedFieldReference(origName), name: String, _), _) =>
            if (isRefByPos) {
              throw new TableException(
                s"Alias '$name' is not allowed if other fields are referenced by position.")
            } else {
              referenceByName(origName, t).map((_, name))
            }
          case (_: TimeAttribute, _) | (Alias(_: TimeAttribute, _, _), _) =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }

      case p: PojoTypeInfo[A] =>
        exprs flatMap {
          case UnresolvedFieldReference(name: String) =>
            referenceByName(name, p).map((_, name))
          case Alias(UnresolvedFieldReference(origName), name: String, _) =>
            referenceByName(origName, p).map((_, name))
          case _: TimeAttribute | Alias(_: TimeAttribute, _, _) =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }

      case _: TypeInformation[_] => // atomic or other custom type information
        var referenced = false
        exprs flatMap {
          case _: TimeAttribute =>
            None
          case UnresolvedFieldReference(_) if referenced =>
            // only accept the first field for an atomic type
            throw new TableException("Only the first field can reference an atomic type.")
          case UnresolvedFieldReference(name: String) =>
            referenced = true
            // first field reference is mapped to atomic type
            Some((0, name))
          case _ => throw new TableException(
            "Field reference expression expected.")
        }
    }

    val (fieldIndexes, fieldNames) = indexedNames.unzip

    if (fieldNames.contains("*")) {
      throw new TableException("Field name can not be '*'.")
    }

    (fieldNames, fieldIndexes)
  }
}
