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

import org.apache.flink.cep.nfa.State
import org.apache.flink.cep.pattern.Quantifier
import org.apache.flink.cep.pattern.conditions.{IterativeCondition, NotCondition}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.cep.scala.pattern.proto.SingletonPattern

class NFACompiler[T] {

  /**
    * Creates all the states between Start and Final state.
    *
    * @param sinkState the state that last state should point to (always the Final state)
    * @return the next state after Start in the resulting graph
    */
  private def createMiddleStates(sinkState: State[T], pattern: SequencePattern[T, _ <: T]) = {
    var lastSink = sinkState


    pattern.foreach { pair =>
      pair._2 match {
        case p: SingletonPattern[T, _ <: T] =>
        case TimesPattern(p, times) =>
        case OneOrMorePattern(p) =>
      }


      //        Quantifier.ConsumingStrategy.NOT_FOLLOW) {
      //        //skip notFollow patterns, they are converted into edge conditions
      //      }
      //      else if (currentPattern.getQuantifier.getConsumingStrategy eq
      //        Quantifier.ConsumingStrategy.NOT_NEXT) {
      //        val notNext = createState(currentPattern.getName, State.StateType.Normal)
      //        val notCondition = currentPattern.getCondition
      //                           .asInstanceOf[IterativeCondition[T]]
      //        val stopState = createStopState(notCondition, currentPattern.getName)
      //        if (lastSink.isFinal) { //so that the proceed to final is not fired
      //          notNext.addIgnore(lastSink, new NotCondition[T](notCondition))
      //        }
      //        else {
      //          notNext.addProceed(lastSink, new NotCondition[T](notCondition))
      //        }
      //        notNext.addProceed(stopState, notCondition)
      //        lastSink = notNext
      //      }
      //      else {
      //        lastSink = convertPattern(lastSink)
      //      }
      //      // we traverse the pattern graph backwards
      //      followingPattern = currentPattern
      //      currentPattern = currentPattern.getPrevious
      //      val currentWindowTime = currentPattern.getWindowTime
      //      if (currentWindowTime != null && currentWindowTime.toMilliseconds <
      //        windowTime) { // the window time is the global minimum of all window times of each state
      //        windowTime = currentWindowTime.toMilliseconds
      //      }
    }

    lastSink
  }

}
