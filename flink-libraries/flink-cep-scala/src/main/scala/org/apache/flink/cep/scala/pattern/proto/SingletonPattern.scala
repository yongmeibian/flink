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

import org.apache.flink.cep.pattern.Quantifier
import org.apache.flink.cep.pattern.Quantifier.Times
import org.apache.flink.util.Preconditions

class SingletonPattern[T, F <: T](val name: String) extends Pattern[T, F] {

  def oneOrMore: OneOrMorePattern[T, F] = {
    new OneOrMorePattern(name)
  }

  def times(times: Int): TimesPattern[T, F] = {
    Preconditions.checkArgument(times > 0, "You should give a positive number greater than 0.")
    new TimesPattern(name, Times.of(times))
  }

  def times(from: Int, to: Int): TimesPattern[T, F] = {
    new TimesPattern(name, Times.of(from, to))
  }

  override def getQuantifierProperty: Quantifier.QuantifierProperty = Quantifier.QuantifierProperty.SINGLE
}
