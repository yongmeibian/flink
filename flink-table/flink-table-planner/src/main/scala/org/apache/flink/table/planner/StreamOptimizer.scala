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

package org.apache.flink.table.planner

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException
import org.apache.calcite.plan.hep.{HepMatchOrder, HepPlanner, HepProgram, HepProgramBuilder}
import org.apache.calcite.plan.{Convention, RelOptPlanner, RelOptUtil, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.tools.{Programs, RuleSet, RuleSets}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.{CalciteConfig, FlinkRelBuilder, RelTimeIndicatorConverter}
import org.apache.flink.table.operations.QueryOperation
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.UpdateAsRetractionTrait
import org.apache.flink.table.plan.rules.FlinkRuleSets

import scala.collection.JavaConverters._

class StreamOptimizer(
  calciteConfig: () => CalciteConfig,
  planningConfigurationBuilder: PlanningConfigurationBuilder) {

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param tableOperation The root node of the relational expression tree.
    * @param updatesAsRetraction True if the sink requests updates as retraction messages.
    * @return The optimized [[RelNode]] tree
    */
  def optimize(
      tableOperation: QueryOperation,
      updatesAsRetraction: Boolean,
      relBuilder: FlinkRelBuilder)
    : RelNode = {
    val relNode = relBuilder.tableOperation(tableOperation).build()
    val convSubQueryPlan = optimizeConvertSubQueries(relNode)
    val expandedPlan = optimizeExpandPlan(convSubQueryPlan)
    val decorPlan = RelDecorrelator.decorrelateQuery(expandedPlan, relBuilder)
    val planWithMaterializedTimeAttributes =
      RelTimeIndicatorConverter.convert(decorPlan, relBuilder.getRexBuilder)
    val normalizedPlan = optimizeNormalizeLogicalPlan(planWithMaterializedTimeAttributes)
    val logicalPlan = optimizeLogicalPlan(normalizedPlan)

    val physicalPlan = optimizePhysicalPlan(logicalPlan, FlinkConventions.DATASTREAM)
    optimizeDecoratePlan(physicalPlan, updatesAsRetraction)
  }

  private def optimizeDecoratePlan(
      relNode: RelNode,
      updatesAsRetraction: Boolean)
    : RelNode = {
    val decoRuleSet = getDecoRuleSet
    if (decoRuleSet.iterator().hasNext) {
      val planToDecorate = if (updatesAsRetraction) {
        relNode.copy(
          relNode.getTraitSet.plus(new UpdateAsRetractionTrait(true)),
          relNode.getInputs)
      } else {
        relNode
      }
      runHepPlannerSequentially(
        HepMatchOrder.BOTTOM_UP,
        decoRuleSet,
        planToDecorate,
        planToDecorate.getTraitSet)
    } else {
      relNode
    }
  }

  private def optimizeConvertSubQueries(relNode: RelNode): RelNode = {
    runHepPlannerSequentially(
      HepMatchOrder.BOTTOM_UP,
      FlinkRuleSets.TABLE_SUBQUERY_RULES,
      relNode,
      relNode.getTraitSet)
  }

  private def optimizeExpandPlan(relNode: RelNode): RelNode = {
    val result = runHepPlannerSimultaneously(
      HepMatchOrder.TOP_DOWN,
      FlinkRuleSets.EXPAND_PLAN_RULES,
      relNode,
      relNode.getTraitSet)

    runHepPlannerSequentially(
      HepMatchOrder.TOP_DOWN,
      FlinkRuleSets.POST_EXPAND_CLEAN_UP_RULES,
      result,
      result.getTraitSet)
  }

  private def optimizeNormalizeLogicalPlan(relNode: RelNode): RelNode = {
    val normRuleSet = getNormRuleSet
    if (normRuleSet.iterator().hasNext) {
      runHepPlannerSequentially(HepMatchOrder.BOTTOM_UP, normRuleSet, relNode, relNode.getTraitSet)
    } else {
      relNode
    }
  }

  private def optimizeLogicalPlan(relNode: RelNode): RelNode = {
    val logicalOptRuleSet = getLogicalOptRuleSet
    val logicalOutputProps = relNode.getTraitSet.replace(FlinkConventions.LOGICAL).simplify()
    if (logicalOptRuleSet.iterator().hasNext) {
      runVolcanoPlanner(logicalOptRuleSet, relNode, logicalOutputProps)
    } else {
      relNode
    }
  }

  private def optimizePhysicalPlan(relNode: RelNode, convention: Convention): RelNode = {
    val physicalOptRuleSet = getPhysicalOptRuleSet
    val physicalOutputProps = relNode.getTraitSet.replace(convention).simplify()
    if (physicalOptRuleSet.iterator().hasNext) {
      runVolcanoPlanner(physicalOptRuleSet, relNode, physicalOutputProps)
    } else {
      relNode
    }
  }

  /**
    * run HEP planner with rules applied one by one. First apply one rule to all of the nodes
    * and only then apply the next rule. If a rule creates a new node preceding rules will not
    * be applied to the newly created node.
    */
  private def runHepPlannerSequentially(
    hepMatchOrder: HepMatchOrder,
    ruleSet: RuleSet,
    input: RelNode,
    targetTraits: RelTraitSet): RelNode = {

    val builder = new HepProgramBuilder
    builder.addMatchOrder(hepMatchOrder)

    val it = ruleSet.iterator()
    while (it.hasNext) {
      builder.addRuleInstance(it.next())
    }
    runHepPlanner(builder.build(), input, targetTraits)
  }

  /**
    * run HEP planner with rules applied simultaneously. Apply all of the rules to the given
    * node before going to the next one. If a rule creates a new node all of the rules will
    * be applied to this new node.
    */
  private def runHepPlannerSimultaneously(
      hepMatchOrder: HepMatchOrder,
      ruleSet: RuleSet,
      input: RelNode,
      targetTraits: RelTraitSet): RelNode = {

    val builder = new HepProgramBuilder
    builder.addMatchOrder(hepMatchOrder)

    builder.addRuleCollection(ruleSet.asScala.toList.asJava)
    runHepPlanner(builder.build(), input, targetTraits)
  }

  /**
    * run HEP planner
    */
  private def runHepPlanner(
    hepProgram: HepProgram,
    input: RelNode,
    targetTraits: RelTraitSet): RelNode = {

    val planner = new HepPlanner(hepProgram, planningConfigurationBuilder.getContext)
    planner.setRoot(input)
    if (input.getTraitSet != targetTraits) {
      planner.changeTraits(input, targetTraits.simplify)
    }
    planner.findBestExp
  }

  /**
    * run VOLCANO planner
    */
  private def runVolcanoPlanner(
    ruleSet: RuleSet,
    input: RelNode,
    targetTraits: RelTraitSet): RelNode = {
    val optProgram = Programs.ofRules(ruleSet)

    val output = try {
      optProgram.run(getPlanner, input, targetTraits,
        ImmutableList.of(), ImmutableList.of())
    } catch {
      case e: CannotPlanException =>
        throw new TableException(
          s"Cannot generate a valid execution plan for the given query: \n\n" +
            s"${RelOptUtil.toString(input)}\n" +
            s"This exception indicates that the query uses an unsupported SQL feature.\n" +
            s"Please check the documentation for the set of currently supported SQL features.")
      case t: TableException =>
        throw new TableException(
          s"Cannot generate a valid execution plan for the given query: \n\n" +
            s"${RelOptUtil.toString(input)}\n" +
            s"${t.getMessage}\n" +
            s"Please check the documentation for the set of currently supported SQL features.")
      case a: AssertionError =>
        // keep original exception stack for caller
        throw a
    }
    output
  }

  /**
    * Returns the decoration rule set for this environment
    * including a custom RuleSet configuration.
    */
  private def getDecoRuleSet: RuleSet = {
    val config = calciteConfig.apply()
    config.decoRuleSet match {

      case None =>
        FlinkRuleSets.DATASTREAM_DECO_RULES

      case Some(ruleSet) =>
        if (config.replacesDecoRuleSet) {
          ruleSet
        } else {
          RuleSets.ofList((FlinkRuleSets.DATASTREAM_DECO_RULES.asScala ++ ruleSet.asScala).asJava)
        }
    }
  }

  /**
    * Returns the normalization rule set for this environment
    * including a custom RuleSet configuration.
    */
  private def getNormRuleSet: RuleSet = {
    val config = calciteConfig.apply()
    config.normRuleSet match {

      case None =>
        FlinkRuleSets.DATASTREAM_NORM_RULES

      case Some(ruleSet) =>
        if (config.replacesNormRuleSet) {
          ruleSet
        } else {
          RuleSets.ofList((FlinkRuleSets.DATASTREAM_NORM_RULES.asScala ++ ruleSet.asScala).asJava)
        }
    }
  }

  /**
    * Returns the logical optimization rule set for this environment
    * including a custom RuleSet configuration.
    */
  private def getLogicalOptRuleSet: RuleSet = {
    val config = calciteConfig.apply()
    config.logicalOptRuleSet match {

      case None =>
        FlinkRuleSets.LOGICAL_OPT_RULES

      case Some(ruleSet) =>
        if (config.replacesLogicalOptRuleSet) {
          ruleSet
        } else {
          RuleSets.ofList((FlinkRuleSets.LOGICAL_OPT_RULES.asScala ++ ruleSet.asScala).asJava)
        }
    }
  }

  /**
    * Returns the physical optimization rule set for this environment
    * including a custom RuleSet configuration.
    */
  private def getPhysicalOptRuleSet: RuleSet = {
    val config = calciteConfig.apply()
    config.physicalOptRuleSet match {

      case None =>
        FlinkRuleSets.DATASTREAM_OPT_RULES

      case Some(ruleSet) =>
        if (calciteConfig.apply().replacesPhysicalOptRuleSet) {
          ruleSet
        } else {
          RuleSets.ofList((FlinkRuleSets.DATASTREAM_OPT_RULES.asScala ++ ruleSet.asScala).asJava)
        }
    }
  }

  /** Returns the Calcite [[org.apache.calcite.plan.RelOptPlanner]] of this TableEnvironment. */
  private def getPlanner: RelOptPlanner = {
    planningConfigurationBuilder.getPlanner
  }
}
