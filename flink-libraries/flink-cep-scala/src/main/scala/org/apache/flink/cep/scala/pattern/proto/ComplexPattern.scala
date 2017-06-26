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

trait ComplexPattern[T, F <: T] extends Pattern[T, F] {

  def pattern: Pattern[T, F]

  private var innerConsumingStrategy: ConsumingStrategy = ConsumingStrategy.SKIP_TILL_NEXT

  def getInnerConsumingStrategy: ConsumingStrategy = innerConsumingStrategy

  def allowCombinations: ComplexPattern[T, F] = {
    innerConsumingStrategy = ConsumingStrategy.SKIP_TILL_ANY
    this
  }

  def consecutive: ComplexPattern[T, F] = {
    innerConsumingStrategy = ConsumingStrategy.STRICT
    this
  }

}

case class OneOrMorePattern[T, F <: T](pattern: Pattern[T, F]) extends ComplexPattern[T, F]{
}

trait SingletonPattern[T, F <: T] extends Pattern[T, F] {

  def pattern: Pattern[T, F]

  def oneOrMore: OneOrMorePattern[T, F] = {
    new OneOrMorePattern(pattern)
  }

  def times(times: Int): TimesPattern[T, F] = {
//    Preconditions.checkArgument(times > 0, "You should give a positive number greater than 0.")
    new TimesPattern(pattern, Times.of(times))
  }

  def times(from: Int, to: Int): TimesPattern[T, F] = {
    new TimesPattern(pattern, Times.of(from, to))
  }
}


case class TimesPattern[T, F <: T](pattern: Pattern[T, F], times: Times) extends ComplexPattern[T, F] {
}

