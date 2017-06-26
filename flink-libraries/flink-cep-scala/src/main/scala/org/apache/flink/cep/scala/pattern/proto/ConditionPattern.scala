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

import org.apache.flink.api.java.ClosureCleaner
import org.apache.flink.cep
import org.apache.flink.cep.pattern.conditions._
import org.apache.flink.util.Preconditions
import org.apache.flink.cep.pattern.conditions.IterativeCondition.{Context => JContext}

case class ConditionPattern[F](name: String) extends SingletonPattern[F, F]{

  /** The condition an event has to satisfy to be considered a matched. */
  private var condition: IterativeCondition[F] = _

  def getCondition: IterativeCondition[F] = this.condition

  val pattern = this

  /**
    * Adds a condition that has to be satisfied by an event
    * in order to be considered a match. If another condition has already been
    * set, the new one is going to be combined with the previous with a
    * logical {@code AND}. In other case, this is going to be the only
    * condition.
    *
    * @param condition The condition as an { @link IterativeCondition}.
    * @return The pattern with the new condition is set.
    */
  def where(condition: IterativeCondition[F]): ConditionPattern[F] = {
    Preconditions.checkNotNull(condition, "The condition cannot be null.")
    ClosureCleaner.clean(condition, true)
    if (this.condition == null) {
      this.condition = condition
    } else {
      this.condition = new AndCondition[F](this.condition, condition)
    }
    this
  }

  /**
    * Adds a condition that has to be satisfied by an event
    * in order to be considered a match. If another condition has already been
    * set, the new one is going to be combined with the previous with a
    * logical {{{AND}}}. In other case, this is going to be the only
    * condition.
    *
    * @param condition The condition to be set.
    * @return The pattern with the new condition is set.
    */
  def where(condition: F => Boolean): ConditionPattern[F] = {
    val condFun = new IterativeCondition[F] {
      val cleanCond = cep.scala.cleanClosure(condition)

      override def filter(value: F, ctx: JContext[F]): Boolean = cleanCond(value)
    }
    where(condFun)
  }

  /**
    * Adds a condition that has to be satisfied by an event
    * in order to be considered a match. If another condition has already been
    * set, the new one is going to be combined with the previous with a
    * logical {@code OR}. In other case, this is going to be the only
    * condition.
    *
    * @param condition The condition as an { @link IterativeCondition}.
    * @return The pattern with the new condition is set.
    */
  def or(condition: IterativeCondition[F]): ConditionPattern[F] = {
    Preconditions.checkNotNull(condition, "The condition cannot be null.")
    ClosureCleaner.clean(condition, true)
    if (this.condition == null) {
      this.condition = condition
    } else {
      this.condition = new OrCondition[F](this.condition, condition)
    }
    this
  }

  /**
    * Applies a subtype constraint on the current pattern. This means that an event has
    * to be of the given subtype in order to be matched.
    *
    * @param subtypeClass Class of the subtype
    * @param <            S> Type of the subtype
    * @return The same pattern with the new subtype constraint
    */
  def subtype[S <: F](subtypeClass: Class[S]): ConditionPattern[S] = {
    Preconditions.checkNotNull(subtypeClass, "The class cannot be null.")
    if (condition == null) {
      this.condition = new SubtypeCondition[F](subtypeClass)
    } else {
      this.condition = new AndCondition[F](condition, new SubtypeCondition[F](subtypeClass))
    }
    @SuppressWarnings(Array("unchecked")) val result = this
                                                       .asInstanceOf[ConditionPattern[S]]
    result
  }

}
