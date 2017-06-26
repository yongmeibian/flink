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

package org.apache.flink.cep.scala.pattern.proto

import org.apache.flink.cep.pattern.Quantifier.{ConsumingStrategy, Times}
import org.apache.flink.util.Preconditions

class SequencePattern[T, F <: T] extends Iterable[(ConsumingStrategy, Pattern[T, _ <: T])] {

  private var patterns: List[(ConsumingStrategy, Pattern[T, _ <: T])] = List()

  def this(pattern: Pattern[T, F]) {
    this()
    next(pattern)
  }

  override def iterator: Iterator[(ConsumingStrategy, Pattern[T, _ <: T])] =
    patterns.reverse.toIterator

  def next(pattern: Pattern[T, F]): SequencePattern[T, F] = {
    patterns :+ (ConsumingStrategy.STRICT, pattern)
    this
  }

  def followedBy(pattern: Pattern[_ <: T, F]): SequencePattern[_<:T, F] = {
    patterns :+ (ConsumingStrategy.SKIP_TILL_NEXT, pattern)
    this
  }

  def ->>(pattern: Pattern[_ <: T, F]): SequencePattern[_<:T, F] = {
    followedBy(pattern)
  }

  def followedByAny(pattern: Pattern[T, F]): SequencePattern[T, F] = {
    patterns :+ (ConsumingStrategy.SKIP_TILL_ANY, pattern)
    this
  }

  def notNext(pattern: Pattern[T, F]): SequencePattern[T, F] = {
    patterns :+ (ConsumingStrategy.NOT_NEXT, pattern)
    this
  }

  def notFollowedBy(pattern: Pattern[T, F]): SequencePattern[T, F] = {
    patterns :+ (ConsumingStrategy.NOT_FOLLOW, pattern)
    this
  }

}
