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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{
  withPosition,
  AliasResolution,
  FunctionResolution,
  UnresolvedAlias,
  UnresolvedAttribute,
  UnresolvedFunction,
  UnresolvedStar
}
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeReference,
  BinaryArithmetic,
  Expression,
  ExtractANSIIntervalDays,
  InheritAnalysisRules,
  Literal,
  NamedExpression,
  Predicate,
  RuntimeReplaceable,
  TimeAdd,
  TimeZoneAwareExpression,
  UnaryMinus
}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.StringUtils.orderSuggestedIdentifiersBySimilarity
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.MetadataBuilder

/**
 * The [[ExpressionResolver]] is used by the [[Resolver]] during the analysis to resolve
 * expressions.
 *
 * The functions here generally traverse unresolved [[Expression]] nodes recursively,
 * constructing and returning the resolved [[Expression]] nodes bottom-up.
 * This is the primary entry point for implementing expression analysis,
 * wherein the [[resolve]] method accepts a fully unresolved [[Expression]] and returns
 * a fully resolved [[Expression]] in response with all data types and attribute
 * reference ID assigned for valid requests. This resolver also takes responsibility
 * to detect any errors in the initial SQL query or DataFrame and return appropriate
 * error messages including precise parse locations wherever possible.
 *
 * @param resolver [[Resolver]] is passed from the parent to resolve other
 *   operators which are nested in expressions.
 * @param scopes [[NameScopeStack]] to resolve the expression tree in the correct scope.
 * @param functionResolution [[FunctionResolution]] to resolve function expressions.
 */
class ExpressionResolver(
    resolver: Resolver,
    scopes: NameScopeStack,
    functionResolution: FunctionResolution)
    extends ExpressionResolverBase[Expression]
    with SQLConfHelper
    with ProducesUnresolvedSubtree
    with ResolvesExpressionChildren
    with TracksResolvedNodes[Expression] {
  private val timezoneAwareExpressionResolver = new TimezoneAwareExpressionResolver(this)
  private val predicateResolver =
    new PredicateResolver(this, timezoneAwareExpressionResolver)
  private val binaryArithmeticResolver = {
    new BinaryArithmeticResolver(
      this,
      timezoneAwareExpressionResolver
    )
  }
  private val timeAddResolver = new TimeAddResolver(this, timezoneAwareExpressionResolver)
  private val unaryMinusResolver = new UnaryMinusResolver(this, timezoneAwareExpressionResolver)

  /**
   * This method is an expression analysis entry point. The method first checks if the expression
   * has already been resolved (necessary because of partially-unresolved subtrees, see
   * [[ProducesUnresolvedSubtree]]). If not already resolved, method takes an unresolved
   * [[Expression]] and chooses the right `resolve*` method using pattern matching on the
   * `unresolvedExpression` type. This pattern matching enumerates all the expression node types
   * that are supported by the single-pass analysis.
   * When developers introduce a new [[Expression]] type to the Catalyst, they should implement
   * a corresponding `resolve*` method in the [[ExpressionResolver]] and add it to this pattern
   * match list.
   *
   * [[resolve]] will be called recursively during the expression tree traversal eventually
   * producing a fully resolved expression subtree or a descriptive error message.
   *
   * [[resolve]] can recursively call `resolver` to resolve nested operators (e.g. scalar
   * subqueries):
   *
   * {{{ SELECT * FROM VALUES (1), (2) WHERE col1 IN (SELECT 1); }}}
   *
   * In this case `IN` is an expression and `SELECT 1` is a nested operator tree for which
   * the [[ExpressionResolver]] would invoke the [[Resolver]].
   */
  def resolve(unresolvedExpression: Expression): Expression =
    if (unresolvedExpression
        .getTagValue(ExpressionResolver.SINGLE_PASS_SUBTREE_BOUNDARY)
        .nonEmpty) {
      unresolvedExpression
    } else {
      withTrackResolvedNodes(unresolvedExpression) {
        unresolvedExpression match {
          case unresolvedBinaryArithmetic: BinaryArithmetic =>
            binaryArithmeticResolver.resolve(unresolvedBinaryArithmetic)
          case unresolvedExtractANSIIntervalDays: ExtractANSIIntervalDays =>
            resolveExtractANSIIntervalDays(unresolvedExtractANSIIntervalDays)
          case unresolvedNamedExpression: NamedExpression =>
            resolveNamedExpression(unresolvedNamedExpression)
          case unresolvedFunction: UnresolvedFunction =>
            resolveFunction(unresolvedFunction)
          case unresolvedLiteral: Literal =>
            resolveLiteral(unresolvedLiteral)
          case unresolvedPredicate: Predicate =>
            predicateResolver.resolve(unresolvedPredicate)
          case unresolvedTimeAdd: TimeAdd =>
            timeAddResolver.resolve(unresolvedTimeAdd)
          case unresolvedUnaryMinus: UnaryMinus =>
            unaryMinusResolver.resolve(unresolvedUnaryMinus)
          case unresolvedRuntimeReplaceable: RuntimeReplaceable =>
            resolveRuntimeReplaceable(unresolvedRuntimeReplaceable)
          case unresolvedTimezoneExpression: TimeZoneAwareExpression =>
            timezoneAwareExpressionResolver.resolve(unresolvedTimezoneExpression)
          case _ =>
            withPosition(unresolvedExpression) {
              throwUnsupportedSinglePassAnalyzerFeature(unresolvedExpression)
            }
        }
      }
    }

  private def resolveNamedExpression(unresolvedNamedExpression: NamedExpression): NamedExpression =
    unresolvedNamedExpression match {
      case unresolvedAlias: UnresolvedAlias =>
        resolveAlias(unresolvedAlias)
      case unresolvedAttribute: UnresolvedAttribute =>
        resolveAttribute(unresolvedAttribute)
      case unresolvedStar: UnresolvedStar =>
        withPosition(unresolvedStar) {
          throwInvalidStarUsageError(unresolvedStar)
        }
      case attributeReference: AttributeReference =>
        handleResolvedAttributeReference(attributeReference)
      case _ =>
        withPosition(unresolvedNamedExpression) {
          throwUnsupportedSinglePassAnalyzerFeature(unresolvedNamedExpression)
        }
    }

  /**
   * The [[Project]] list can contain different unresolved expressions before the resolution, which
   * will be resolved using generic [[resolve]]. However, [[UnresolvedStar]] is a special case,
   * because it is expanded into a sequence of [[NamedExpression]]s. Because of that this method
   * returns a sequence and doesn't conform to generic [[resolve]] interface - it's called directly
   * from the [[Resolver]] during [[Project]] resolution.
   *
   * The output sequence can be larger than the input sequence due to [[UnresolvedStar]] expansion.
   */
  def resolveProjectList(unresolvedProjectList: Seq[NamedExpression]): Seq[NamedExpression] = {
    unresolvedProjectList.flatMap {
      case unresolvedStar: UnresolvedStar =>
        resolveStar(unresolvedStar)
      case other =>
        Seq(resolveNamedExpression(other))
    }
  }

  /**
   * [[UnresolvedAlias]] is currently resolved using the existing logic from the fixed-point
   * analyzer.
   */
  private def resolveAlias(unresolvedAlias: UnresolvedAlias): NamedExpression = {
    val aliasWithResolvedChildren = withResolvedChildren(unresolvedAlias, resolve)
    AliasResolution.resolve(aliasWithResolvedChildren).asInstanceOf[NamedExpression]
  }

  /**
   * [[UnresolvedAttribute]] resolution relies on [[NameScope]] to lookup the attribute by its
   * multipart name:
   * - No results from the [[NameScope]] mean that the attribute lookup failed as in:
   *   {{{ SELECT col1 FROM (SELECT 1 as col2); }}}
   *
   * - Several results from the [[NameScope]] mean that the reference is ambiguous as in:
   *   {{{ SELECT col1 FROM (SELECT 1 as col1), (SELECT 2 as col1); }}}
   *
   * - Single result from the [[NameScope]] means that the attribute was found as in:
   *   {{{ SELECT col1 FROM VALUES (1); }}}
   */
  private def resolveAttribute(unresolvedAttribute: UnresolvedAttribute): NamedExpression =
    withPosition(unresolvedAttribute) {
      val attributes = scopes.top.getMatchedAttributes(unresolvedAttribute.nameParts)
      if (attributes.isEmpty) {
        throwUnresolvedColumnError(unresolvedAttribute)
      }
      if (attributes.size > 1) {
        throwAmbiguousReferenceError(unresolvedAttribute, attributes)
      }

      attributes.head
    }

  /**
   * [[AttributeReference]] is already resolved if it's passed to us from DataFrame `col(...)`
   * function, for example.
   */
  private def handleResolvedAttributeReference(attributeReference: AttributeReference) =
    tryStripAmbiguousSelfJoinMetadata(attributeReference)

  /**
   * [[ExtractANSIIntervalDays]] resolution doesn't require any specific resolution logic apart
   * from resolving its children.
   */
  private def resolveExtractANSIIntervalDays(
      unresolvedExtractANSIIntervalDays: ExtractANSIIntervalDays) =
    withResolvedChildren(unresolvedExtractANSIIntervalDays, resolve)

  /**
   * [[UnresolvedFunction]] is currently resolved using the existing logic from the fixed-point
   * analyzer.
   */
  private def resolveFunction(unresolvedFunction: UnresolvedFunction): Expression = {
    val functionWithResolvedChildren = withResolvedChildren(unresolvedFunction, resolve)
    functionResolution.resolveFunction(functionWithResolvedChildren)
  }

  /**
   * [[UnresolvedStar]] resolution relies on the [[NameScope]]'s ability to get the attributes by
   * a plan miltipart name ([[UnresolvedStar]]'s `target` field):
   *
   * {{{
   * SELECT t.* FROM VALUES (1) AS t;
   * ->
   * Project [col1#19]
   * }}}
   *
   * Or, if the `target` is not defined, we just get all the attributes from this top [[NameScope]]:
   *
   * {{{
   * SELECT * FROM (SELECT 1 as col1), (SELECT 2 as col2);
   * ->
   * Project [col1#19, col2#20]
   * }}}
   */
  private def resolveStar(unresolvedStar: UnresolvedStar): Seq[NamedExpression] =
    withPosition(unresolvedStar) {
      if (unresolvedStar.target.isDefined) {
        scopes.top.expandStar(unresolvedStar)
      } else {
        scopes.top.getAllAttributes
      }
    }

  /**
   * [[Literal]] resolution doesn't require any specific resolution logic at this point.
   *
   * Since [[TracksResolvedNodes]] requires all the expressions in the tree to be unique objects,
   * we reallocate the literal in [[ANALYZER_SINGLE_PASS_TRACK_RESOLVED_NODES_ENABLED]] mode,
   * otherwise we preserve the old object to avoid unnecessary memory allocations.
   */
  private def resolveLiteral(literal: Literal): Expression = {
    if (conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_TRACK_RESOLVED_NODES_ENABLED)) {
      literal.copy()
    } else {
      literal
    }
  }

  /**
   * When [[RuntimeReplaceable]] is mixed in with [[InheritAnalysisRules]], child expression will
   * be runtime replacement. In that case we need to resolve the children of the expression.
   * otherwise, no resolution is necessary because replacement is already resolved.
   */
  private def resolveRuntimeReplaceable(unresolvedRuntimeReplaceable: RuntimeReplaceable) =
    unresolvedRuntimeReplaceable match {
      case inheritAnalysisRules: InheritAnalysisRules =>
        withResolvedChildren(inheritAnalysisRules, resolve)
      case other => other
    }

  /**
   * [[DetectAmbiguousSelfJoin]] rule in the fixed-point Analyzer detects ambiguous references in
   * self-joins based on special metadata added by [[Dataset]] code (see SPARK-27547). Just strip
   * this for now since we don't support joins yet.
   */
  private def tryStripAmbiguousSelfJoinMetadata(attributeReference: AttributeReference) = {
    val metadata = attributeReference.metadata
    if (ExpressionResolver.AMBIGUOUS_SELF_JOIN_METADATA.exists(metadata.contains(_))) {
      val metadataBuilder = new MetadataBuilder().withMetadata(metadata)
      for (metadataKey <- ExpressionResolver.AMBIGUOUS_SELF_JOIN_METADATA) {
        metadataBuilder.remove(metadataKey)
      }
      attributeReference.withMetadata(metadataBuilder.build())
    } else {
      attributeReference
    }
  }

  private def throwUnsupportedSinglePassAnalyzerFeature(unresolvedExpression: Expression): Nothing =
    throw QueryCompilationErrors.unsupportedSinglePassAnalyzerFeature(
      s"${unresolvedExpression.getClass} expression resolution"
    )

  private def throwInvalidStarUsageError(unresolvedStar: UnresolvedStar): Nothing =
    // TODO(vladimirg-db): Use parent operator name instead of "query"
    throw QueryCompilationErrors.invalidStarUsageError("query", Seq(unresolvedStar))

  private def throwUnresolvedColumnError(unresolvedAttribute: UnresolvedAttribute): Nothing =
    throw QueryCompilationErrors.unresolvedColumnError(
      unresolvedAttribute.name,
      proposal = orderSuggestedIdentifiersBySimilarity(
        unresolvedAttribute.name,
        candidates =
          scopes.top.getAllAttributes.map(attribute => attribute.qualifier :+ attribute.name)
      )
    )

  private def throwAmbiguousReferenceError(
      unresolvedAttribute: UnresolvedAttribute,
      attributes: Seq[Attribute]
  ): Nothing =
    throw QueryCompilationErrors.ambiguousReferenceError(unresolvedAttribute.name, attributes)
}

object ExpressionResolver {
  private val AMBIGUOUS_SELF_JOIN_METADATA = Seq("__dataset_id", "__col_position")
  val SINGLE_PASS_SUBTREE_BOUNDARY = TreeNodeTag[Unit]("single_pass_subtree_boundary")
}
