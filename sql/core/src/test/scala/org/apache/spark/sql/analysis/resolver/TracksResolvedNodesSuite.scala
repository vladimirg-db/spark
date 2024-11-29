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

package org.apache.spark.sql

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.FunctionResolution
import org.apache.spark.sql.catalyst.analysis.resolver.{
  ExpressionResolver,
  NameScopeStack,
  Resolver
}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, StringType}

class TracksResolvedNodesSuite extends QueryTest with SharedSparkSession {
  test("Single-pass contract broken for operators") {
    val resolver = createResolver()
    val innerProject = Project(
      projectList = Seq(),
      child = OneRowRelation()
    )
    val outerProject = Project(
      projectList = Seq(),
      child = innerProject
    )
    checkError(
      exception = intercept[SparkException]({
        val resolvedOuterProject = resolver.lookupMetadataAndResolve(outerProject)
        resolver.lookupMetadataAndResolve(resolvedOuterProject.children.head)
      }),
      condition = "INTERNAL_ERROR",
      parameters = Map(
        "message" -> ("Single-pass resolver attempted to resolve the same " +
        "node more than once: Project\n+- OneRowRelation\n")
      )
    )
  }

  test("Single-pass contract broken for expressions") {
    val resolver = createResolver()
    val expressionResolver = createExpressionResolver()
    val column = AttributeReference(name = "column", dataType = BooleanType)(exprId = ExprId(0))
    val cast = Cast(
      child = column,
      dataType = StringType
    )
    checkError(
      exception = intercept[SparkException]({
        expressionResolver.resolve(cast)
        expressionResolver.resolve(column)
      }),
      condition = "INTERNAL_ERROR",
      parameters = Map(
        "message" -> ("Single-pass resolver attempted " +
        "to resolve the same node more than once: column#0")
      )
    )
  }

  private def createResolver(): Resolver = {
    new Resolver(spark.sessionState.catalogManager)
  }

  private def createExpressionResolver(): ExpressionResolver = {
    new ExpressionResolver(
      createResolver(),
      new NameScopeStack,
      new FunctionResolution(
        spark.sessionState.catalogManager,
        Resolver.createRelationResolution(spark.sessionState.catalogManager)
      )
    )
  }
}
