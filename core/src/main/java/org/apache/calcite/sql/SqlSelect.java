/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * A <code>SqlSelect</code> is a node of a parse tree which represents a select
 * statement. It warrants its own node type just because we have a lot of
 * methods to put somewhere.
 */
public class SqlSelect extends SqlCall {
  //~ Static fields/initializers ---------------------------------------------

  // constants representing operand positions
  public static final int FROM_OPERAND = 2;
  public static final int WHERE_OPERAND = 3;
  public static final int HAVING_OPERAND = 5;

  public static final int QUALIFY_OPERAND = 6;

  SqlNodeList keywordList;
  SqlNodeList selectList;
  @Nullable SqlNode from;
  @Nullable SqlNode where;
  @Nullable SqlNodeList groupBy;
  @Nullable SqlNode having;

  @Nullable SqlNode qualify;
  SqlNodeList windowDecls;
  @Nullable SqlNodeList orderBy;
  @Nullable SqlNode offset;
  @Nullable SqlNode fetch;
  @Nullable SqlNodeList hints;

  //~ Constructors -----------------------------------------------------------

  public SqlSelect(SqlParserPos pos,
      @Nullable SqlNodeList keywordList,
      SqlNodeList selectList,
      @Nullable SqlNode from,
      @Nullable SqlNode where,
      @Nullable SqlNodeList groupBy,
      @Nullable SqlNode having,
      @Nullable SqlNode qualify,
      @Nullable SqlNodeList windowDecls,
      @Nullable SqlNodeList orderBy,
      @Nullable SqlNode offset,
      @Nullable SqlNode fetch,
      @Nullable SqlNodeList hints) {
    super(pos);
    this.keywordList = requireNonNull(keywordList != null
        ? keywordList : new SqlNodeList(pos));
    this.selectList = requireNonNull(selectList, "selectList");
    this.from = from;
    this.where = where;
    this.groupBy = groupBy;
    this.having = having;
    this.qualify = qualify;
    this.windowDecls = requireNonNull(windowDecls != null
        ? windowDecls : new SqlNodeList(pos));
    this.orderBy = orderBy;
    this.offset = offset;
    this.fetch = fetch;
    this.hints = hints;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return SqlSelectOperator.INSTANCE;
  }

  @Override public SqlKind getKind() {
    return SqlKind.SELECT;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(keywordList, selectList, from, where,
        groupBy, having, qualify, windowDecls, orderBy, offset, fetch, hints);
  }

  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      keywordList = requireNonNull((SqlNodeList) operand);
      break;
    case 1:
      selectList = requireNonNull((SqlNodeList) operand);
      break;
    case 2:
      from = operand;
      break;
    case 3:
      where = operand;
      break;
    case 4:
      groupBy = (SqlNodeList) operand;
      break;
    case 5:
      having = operand;
      break;
    case 6:
      windowDecls = requireNonNull((SqlNodeList) operand);
      break;
    case 7:
      orderBy = (SqlNodeList) operand;
      break;
    case 8:
      offset = operand;
      break;
    case 9:
      fetch = operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  public final boolean isDistinct() {
    return getModifierNode(SqlSelectKeyword.DISTINCT) != null;
  }

  public final @Nullable SqlNode getModifierNode(SqlSelectKeyword modifier) {
    for (SqlNode keyword : keywordList) {
      SqlSelectKeyword keyword2 =
          ((SqlLiteral) keyword).symbolValue(SqlSelectKeyword.class);
      if (keyword2 == modifier) {
        return keyword;
      }
    }
    return null;
  }

  @Pure
  public final @Nullable SqlNode getFrom() {
    return from;
  }

  public void setFrom(@Nullable SqlNode from) {
    this.from = from;
  }

  @Pure
  public final @Nullable SqlNodeList getGroup() {
    return groupBy;
  }

  public void setGroupBy(@Nullable SqlNodeList groupBy) {
    this.groupBy = groupBy;
  }

  @Pure
  public final @Nullable SqlNode getHaving() {
    return having;
  }

  public void setHaving(@Nullable SqlNode having) {
    this.having = having;
  }

  @Pure
  public final @Nullable SqlNode getQualify() {
    return qualify;
  }

  public void setQualify(@Nullable SqlNode qualify) {
    this.qualify = qualify;
  }

  @Pure
  public final SqlNodeList getSelectList() {
    return selectList;
  }

  public void setSelectList(SqlNodeList selectList) {
    this.selectList = selectList;
  }

  @Pure
  public final @Nullable SqlNode getWhere() {
    return where;
  }

  public void setWhere(@Nullable SqlNode whereClause) {
    this.where = whereClause;
  }

  public final SqlNodeList getWindowList() {
    return windowDecls;
  }

  @Pure
  public final @Nullable SqlNodeList getOrderList() {
    return orderBy;
  }

  public void setOrderBy(@Nullable SqlNodeList orderBy) {
    this.orderBy = orderBy;
  }

  @Pure
  public final @Nullable SqlNode getOffset() {
    return offset;
  }

  public void setOffset(@Nullable SqlNode offset) {
    this.offset = offset;
  }

  @Pure
  public final @Nullable SqlNode getFetch() {
    return fetch;
  }

  public void setFetch(@Nullable SqlNode fetch) {
    this.fetch = fetch;
  }

  public void setHints(@Nullable SqlNodeList hints) {
    this.hints = hints;
  }

  @Pure
  public @Nullable SqlNodeList getHints() {
    return this.hints;
  }

  @EnsuresNonNullIf(expression = "hints", result = true)
  public boolean hasHints() {
    // The hints may be passed as null explicitly.
    return this.hints != null && this.hints.size() > 0;
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateQuery(this, scope, validator.getUnknownType());
  }

  // Override SqlCall, to introduce a sub-query frame.
  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (!writer.inQuery()) {
      // If this SELECT is the topmost item in a sub-query, introduce a new
      // frame. (The topmost item in the sub-query might be a UNION or
      // ORDER. In this case, we don't need a wrapper frame.)
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.SUB_QUERY, "(", ")");
      writer.getDialect().unparseCall(writer, this, 0, 0);
      writer.endList(frame);
    } else {
      writer.getDialect().unparseCall(writer, this, leftPrec, rightPrec);
    }
  }

  public boolean hasOrderBy() {
    return orderBy != null && orderBy.size() != 0;
  }

  public boolean hasWhere() {
    return where != null;
  }

  public boolean isKeywordPresent(SqlSelectKeyword targetKeyWord) {
    return getModifierNode(targetKeyWord) != null;
  }

  /**
   * Rewrite a select statement with a where condition if the where
   * condition depends on the columns in the select list. This writes
   * the code to use two select statements.
   *
   * For example Select A + 1 as X from table where X > 3
   * becomes Select TEMP_COLUMN0 FROM (Select A + 1 as TEMP_COLUMN0) where TEMP_COLUMN0 > 3
   *
   * We only do this replacement if we detect that the where may reference an alias generated
   * by the select statment. This approach simplifies code but is not required for correctness.
   * If this fails then the alias will be inlined as a fallback in validation.
   *
   * @return The new top level select
   */
  public SqlSelect rewriteSqlSelectIfWhereAlias() {
    // Right now to make sure everything executes in order we require
    // a select statement only contain a where
    if (!keywordList.isEmpty() || where == null || groupBy != null
        || having != null || qualify != null || !windowDecls.isEmpty()
        || orderBy != null || offset != null || fetch != null) {
      // Only perform the rewrite if we just have a where.
      return this;
    }
    // Create a set of Alias names that could be referenced by a where
    Set<ImmutableList<String>> possibleAliases = new HashSet<>();
    // Iterate through the select statements and check for any aliases that
    // may be used in the where.
    for (int i = 0; i < selectList.size(); i++) {
      SqlNode selectVal = selectList.get(i);
      if (selectVal instanceof SqlBasicCall && selectVal.getKind() == SqlKind.AS) {
        SqlBasicCall innerAlias = (SqlBasicCall) selectVal;
        // Check if the alias is unnecessary. If so skip the transformation.
        List<SqlNode> operands = innerAlias.getOperandList();
        SqlIdentifier aliasIdentifier = (SqlIdentifier) operands.get(operands.size() - 1);
        if (operands.size() == 2 && (operands.get(0) instanceof SqlIdentifier
            || operands.get(0) instanceof SqlLiteral)) {
          // No need to replace if the alias is a simple column
          // rename or a simple name for a literal.
          continue;
        }
        possibleAliases.add(aliasIdentifier.names);
      }
    }

    // If we don't have any aliases we won't need this rewrite
    if (possibleAliases.isEmpty()) {
      return this;
    }
    // Check if the Where clause uses this alias anywhere. If so
    // we need to replace.
    ArrayDeque<SqlNode> nodeQueue = new ArrayDeque<>();
    nodeQueue.add(where);
    boolean needsReplacement = false;
    while (!nodeQueue.isEmpty()) {
      SqlNode node = nodeQueue.pop();
      if (node instanceof SqlCall) {
        SqlCall callNode = (SqlCall) node;
        List<@Nullable SqlNode> operands = callNode.getOperandList();
        for (@Nullable SqlNode operand: operands) {
          if (operand != null) {
            nodeQueue.add(operand);
          }
        }
      } else if (node instanceof SqlIdentifier) {
        SqlIdentifier identifier = (SqlIdentifier) node;
        if (possibleAliases.contains(identifier.names)) {
          needsReplacement = true;
          break;
        }
      }
    }
    // We didn't find any aliases that need replacement
    // we don't need to rewrite this Node.
    if (!needsReplacement) {
      return this;
    }

    // The rewrite is requires so we need to generate new Select statements from the
    // new node lists. Generate an inner select without the where we need to update
    // the inner select to include any column in the FROM.
    List<@Nullable SqlNode> innerNodes = new ArrayList<>();
    List<@Nullable SqlNode> outerNodes = new ArrayList<>();

    // Also generate what the new select statements
    // would be at this time.
    for (int i = 0; i < selectList.size(); i++) {
      SqlNode selectVal = selectList.get(i);
      SqlIdentifier aliasIdentifier;
      SqlNode innerVal;
      if (selectVal instanceof SqlBasicCall && selectVal.getKind() == SqlKind.AS) {
        // We have found an alias
        innerVal = selectVal;
        SqlBasicCall innerAlias = (SqlBasicCall) selectVal;
        aliasIdentifier = (SqlIdentifier) innerAlias.getOperandList()
            .get(innerAlias.operandCount() - 1);
      } else if (selectVal instanceof SqlIdentifier) {
        // If we have just found an identifier we don't want to generate an alias.
        innerVal = selectVal;
        aliasIdentifier = (SqlIdentifier) selectVal;
      } else {
        // We need to generate a new alias with an internal name.
        // TODO: Check this for uniqueness???
        aliasIdentifier = new SqlIdentifier(
            String.format(Locale.ROOT,
            "$TEMP_COLUMN%d", i), selectVal.getParserPosition());
        List<SqlNode> aliasNodes = Arrays.asList(new SqlNode[]{selectVal, aliasIdentifier});
        innerVal = new SqlBasicCall(SqlStdOperatorTable.AS, aliasNodes,
            selectVal.getParserPosition());
      }
      innerNodes.add(innerVal);
      outerNodes.add(aliasIdentifier);
    }
    // Add the * in case the where references any inner values
    innerNodes.add(SqlIdentifier.STAR);
    SqlNodeList innerSelectList = new SqlNodeList(innerNodes, selectList.getParserPosition());
    SqlNode innerSelect = new SqlSelect(this.getParserPosition(), this.keywordList, innerSelectList,
        from, null, groupBy, having, qualify, windowDecls, orderBy, offset, fetch, hints);
    // Generate the outer select list
    // TODO: Fix column pruning
    SqlNodeList outerSelectList = new SqlNodeList(outerNodes, selectList.getParserPosition());
    // Generate a new outer select list
    SqlSelect newSelect = new SqlSelect(this.getParserPosition(), null, outerSelectList,
        innerSelect, where, null, null, null, null, null,
        null, null, null);
    return newSelect;
  }
}
