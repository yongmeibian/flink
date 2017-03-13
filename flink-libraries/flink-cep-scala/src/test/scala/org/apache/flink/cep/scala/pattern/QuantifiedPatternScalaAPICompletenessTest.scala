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
package org.apache.flink.cep.scala.pattern

import java.lang.reflect.Method

import org.apache.flink.api.scala.completeness.ScalaAPICompletenessTestBase
import org.apache.flink.cep.pattern.{QuantifiedPattern => JQuantifiedPattern}
import org.junit.Test

import scala.language.existentials

/**
  * This checks whether the CEP Scala API is up to feature parity with the Java API.
  * Implements the [[ScalaAPICompletenessTestBase]] for CEP.
  */
class QuantifiedPatternScalaAPICompletenessTest extends ScalaAPICompletenessTestBase {

  val excludedMethods = Seq(
    classOf[JQuantifiedPattern[_, _]].getMethod("getPrevious"),
    classOf[JQuantifiedPattern[_, _]].getMethod("getFilterFunction"),
    classOf[JQuantifiedPattern[_, _]].getMethod("isFollowedBy"),
    classOf[JQuantifiedPattern[_, _]].getMethod("getName"),
    classOf[JQuantifiedPattern[_, _]].getMethod("getWindowTime"),
    classOf[JQuantifiedPattern[_, _]].getMethod("getQuantifier"),
    classOf[JQuantifiedPattern[_, _]].getMethod("getTimes")
  )

  override def isExcludedByName(method: Method): Boolean = {
    excludedMethods.contains(method)
  }

  @Test
  override def testCompleteness(): Unit = {
    checkMethods("QuantifiedPattern", "QuantifiedPattern",
      classOf[JQuantifiedPattern[_, _]],
      classOf[QuantifiedPattern[_, _]])
  }

}
