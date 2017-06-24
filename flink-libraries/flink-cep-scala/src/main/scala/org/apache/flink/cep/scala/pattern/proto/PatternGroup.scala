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

sealed trait PatternGroup[T, F <: T]
  extends PatternSequenceElement[T, F] with Iterable[Pattern[T, _ <: T]] {

  private var patterns: List[(ConsumingStrategy, Pattern[T, _ <: T])] = List()

  override def iterator: Iterator[(ConsumingStrategy, Pattern[T, _ <: T])] =
    patterns.reverse.toIterator

  def next(pattern: Pattern[T, F]): PatternGroup[T, F] = {
    patterns :+ (ConsumingStrategy.STRICT, pattern)
    this
  }

  def followedBy(pattern: Pattern[T, F]): PatternGroup[T, F] = {
    patterns :+ (ConsumingStrategy.SKIP_TILL_NEXT, pattern)
    this
  }

  def followedByAny(pattern: Pattern[T, F]): PatternGroup[T, F] = {
    patterns :+ (ConsumingStrategy.SKIP_TILL_ANY, pattern)
    this
  }

  def notNext(pattern: Pattern[T, F]): PatternGroup[T, F] = {
    patterns :+ (ConsumingStrategy.NOT_NEXT, pattern)
    this
  }

  def notFollowedBy(pattern: Pattern[T, F]): PatternGroup[T, F] = {
    patterns :+ (ConsumingStrategy.NOT_FOLLOW, pattern)
    this
  }

}

class SinglePatternGroup[T, F <: T] extends PatternGroup[T, F] {
  def oneOrMore: OneOrMorePatternGroup[T, F] = {
    new OneOrMorePatternGroup
  }

  def times(times: Int): TimesPatternGroup[T, F] = {
    Preconditions.checkArgument(times > 0, "You should give a positive number greater than 0.")
    new TimesPatternGroup(Times.of(times))
  }

  def times(from: Int, to: Int): TimesPatternGroup[T, F] = {
    new TimesPatternGroup(Times.of(from, to))
  }
}

class TimesPatternGroup[T, F <: T](val times: Times) extends PatternGroup[T, F] {

}


class OneOrMorePatternGroup[T, F <: T] extends PatternGroup[T, F] {

}


object PatternGroup {
  def begin[T](pattern: Pattern[T, T]): PatternGroup[T, T] = new SinglePatternGroup[T, T]()
}
