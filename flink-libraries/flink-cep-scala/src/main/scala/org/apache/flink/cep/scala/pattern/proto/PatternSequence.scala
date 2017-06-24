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

import org.apache.flink.cep.pattern.Quantifier.ConsumingStrategy

class PatternSequence[T, F<:T](val patterns: List[(ConsumingStrategy, PatternSequenceElement[T, _<:T])])
  extends Iterable[PatternSequenceElement[T, _<:T]] {

  override def iterator: Iterator[(ConsumingStrategy, PatternSequenceElement[T, _<:T])] =
    patterns.reverse.toIterator

  def next(pattern: PatternSequenceElement[T, F]) = new PatternSequence[T, F](
    patterns :+ (ConsumingStrategy.STRICT, pattern))

  def followedBy(pattern: PatternSequenceElement[T, F]) = new PatternSequence[T, F](
    patterns :+ (ConsumingStrategy.SKIP_TILL_NEXT, pattern))

  def followedByAny(pattern: PatternSequenceElement[T, F]) = new PatternSequence[T, F](
    patterns :+ (ConsumingStrategy.SKIP_TILL_ANY, pattern))

  def notNext(pattern: PatternSequenceElement[T, F]) = new PatternSequence[T, F](
    patterns :+ (ConsumingStrategy.NOT_NEXT, pattern))

  def notFollowedBy(pattern: PatternSequenceElement[T, F]) = new PatternSequence[T, F](
    patterns :+ (ConsumingStrategy.NOT_FOLLOW, pattern))

}

object PatternSequence {
  def begin[T](pattern: Pattern[T, _ <: T]): PatternSequence[T, T] = new PatternSequence[T, T](
    List((ConsumingStrategy.SKIP_TILL_NEXT, pattern)))
}
