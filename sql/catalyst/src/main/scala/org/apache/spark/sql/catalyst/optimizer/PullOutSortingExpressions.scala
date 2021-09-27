/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.SORT

/**
 * This rule ensures that [[Sort]] nodes doesn't contain complex sorting expressions in the
 * optimization phase if it is global sorting.
 */
object PullOutSortingExpressions extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformWithPruning(_.containsPattern(SORT)) {
      case s: Sort if s.resolved && s.global =>
        val complexSortingExpressionMap = mutable.LinkedHashMap.empty[Expression, NamedExpression]
        s.order.map(_.child).foreach {
          case e if !e.foldable && e.children.nonEmpty =>
            complexSortingExpressionMap.put(e.canonicalized, Alias(e, "_sorting")())
          case o => o
        }
        if (complexSortingExpressionMap.nonEmpty) {
          val newSortExpressions: Seq[SortOrder] = s.order.map { s =>
            val newChild = complexSortingExpressionMap.get(s.child.canonicalized).map(_.toAttribute)
              .getOrElse(s.child)
            s.copy(child = newChild)
          }
          val newChild = Project(s.child.output ++ complexSortingExpressionMap.values, s.child)
          Project(s.output, s.copy(order = newSortExpressions, child = newChild))
        } else {
          s
        }
    }
  }
}
