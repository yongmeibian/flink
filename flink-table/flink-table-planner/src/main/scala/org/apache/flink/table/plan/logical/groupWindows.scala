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

package org.apache.flink.table.plan.logical

import java.util.{Optional, List => JList}

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.expressions.PlannerExpressionUtils.{isRowCountLiteral, isRowtimeAttribute, isTimeAttribute, isTimeIntervalLiteral}
import org.apache.flink.table.expressions._
import org.apache.flink.table.typeutils.TypeCheckUtils.{isLong, isTimePoint}

// ------------------------------------------------------------------------------------------------
// Over windows
// ------------------------------------------------------------------------------------------------

case class LogicalOverWindow(
    alias: Expression,
    partitionBy: JList[Expression],
    orderBy: Expression,
    preceding: Expression,
    following: Optional[Expression])

// ------------------------------------------------------------------------------------------------
// Tumbling group windows
// ------------------------------------------------------------------------------------------------

case class TumblingGroupWindow(
    alias: PlannerExpression,
    timeField: PlannerExpression,
    size: PlannerExpression)
  extends LogicalWindow(
    alias,
    timeField) {

  override def validate(isStreaming: Boolean): Unit =

      isStreaming match {

        // check size
        case _ if !isTimeIntervalLiteral(size) && !isRowCountLiteral(size) =>
          throw new ValidationException(
            "Tumbling window expects size literal of type Interval of Milliseconds " +
              "or Interval of Rows.")

        // check time attribute
        case true if !isTimeAttribute(timeField) =>
          throw new ValidationException(
            "Tumbling window expects a time attribute for grouping in a stream environment.")
        case false
          if !(isTimePoint(timeField.resultType) || isLong(timeField.resultType)) =>
          throw new ValidationException(
            "Tumbling window expects a time attribute for grouping in a batch environment.")

        // check row intervals on event-time
        case true
            if isRowCountLiteral(size) && isRowtimeAttribute(timeField) =>
          throw new ValidationException(
            "Event-time grouping windows on row intervals in a stream environment " +
              "are currently not supported.")

        case _ =>
          // validation successful
      }

  override def toString: String = s"TumblingGroupWindow($alias, $timeField, $size)"
}

// ------------------------------------------------------------------------------------------------
// Sliding group windows
// ------------------------------------------------------------------------------------------------

case class SlidingGroupWindow(
    alias: PlannerExpression,
    timeField: PlannerExpression,
    size: PlannerExpression,
    slide: PlannerExpression)
  extends LogicalWindow(
    alias,
    timeField) {

  override def validate(isStreaming: Boolean): Unit =
    isStreaming match {

        // check size
        case _ if !isTimeIntervalLiteral(size) && !isRowCountLiteral(size) =>
          throw new ValidationException(
            "Sliding window expects size literal of type Interval of Milliseconds " +
              "or Interval of Rows.")

        // check slide
        case _ if !isTimeIntervalLiteral(slide) && !isRowCountLiteral(slide) =>
          throw new ValidationException(
            "Sliding window expects slide literal of type Interval of Milliseconds " +
              "or Interval of Rows.")

        // check same type of intervals
        case _ if isTimeIntervalLiteral(size) != isTimeIntervalLiteral(slide) =>
          throw new ValidationException("Sliding window expects same type of size and slide.")

        // check time attribute
        case true if !isTimeAttribute(timeField) =>
          throw new ValidationException(
            "Sliding window expects a time attribute for grouping in a stream environment.")
        case false
          if !(isTimePoint(timeField.resultType) || isLong(timeField.resultType)) =>
          throw new ValidationException(
            "Sliding window expects a time attribute for grouping in a stream environment.")

        // check row intervals on event-time
        case true
            if isRowCountLiteral(size) && isRowtimeAttribute(timeField) =>
          throw new ValidationException(
            "Event-time grouping windows on row intervals in a stream environment " +
              "are currently not supported.")

        case _ =>
          // validation successful
      }


  override def toString: String = s"SlidingGroupWindow($alias, $timeField, $size, $slide)"
}

// ------------------------------------------------------------------------------------------------
// Session group windows
// ------------------------------------------------------------------------------------------------

case class SessionGroupWindow(
    alias: PlannerExpression,
    timeField: PlannerExpression,
    gap: PlannerExpression)
  extends LogicalWindow(
    alias,
    timeField) {

  override def validate(isStreaming: Boolean): Unit =
    isStreaming match {

        // check size
        case _ if !isTimeIntervalLiteral(gap) =>
          throw new ValidationException(
            "Session window expects size literal of type Interval of Milliseconds.")

        // check time attribute
        case true if !isTimeAttribute(timeField) =>
          throw new ValidationException(
            "Session window expects a time attribute for grouping in a stream environment.")
        case false
          if !(isTimePoint(timeField.resultType) || isLong(timeField.resultType)) =>
          throw new ValidationException(
            "Session window expects a time attribute for grouping in a stream environment.")

        case _ =>
          // validation successful
      }

  override def toString: String = s"SessionGroupWindow($alias, $timeField, $gap)"
}
