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

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import java.util.List;

/**
 * A <code>SqlMerge</code> is a node of a parse tree which represents a MERGE
 * statement.
 */
public class SqlMerge extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("MERGE", SqlKind.MERGE);

  SqlNode targetTable;
  SqlNode condition;
  SqlNode source;

  SqlNodeList matchedCallList;
  SqlNodeList notMatchedCallList;

  @Nullable SqlSelect sourceSelect;
  @Nullable SqlIdentifier alias;

  //~ Constructors -----------------------------------------------------------

  public SqlMerge(SqlParserPos pos,
      SqlNode targetTable,
      SqlNode condition,
      SqlNode source,
      SqlNodeList matchedCallList,
      SqlNodeList notMatchedCallList,
      @Nullable SqlSelect sourceSelect,
      @Nullable SqlIdentifier alias) {
    super(pos);
    this.targetTable = targetTable;
    this.condition = condition;
    this.source = source;
    this.matchedCallList = matchedCallList;
    this.notMatchedCallList = notMatchedCallList;
    this.sourceSelect = sourceSelect;
    this.alias = alias;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public SqlKind getKind() {
    return SqlKind.MERGE;
  }

  @SuppressWarnings("nullness")
  @Override public List<@Nullable SqlNode> getOperandList() {
    return ImmutableNullableList.of(targetTable, condition, source, matchedCallList,
        notMatchedCallList, sourceSelect, alias);
  }

  @SuppressWarnings("assignment.type.incompatible")
  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      assert operand instanceof SqlIdentifier;
      targetTable = operand;
      break;
    case 1:
      condition = operand;
      break;
    case 2:
      source = operand;
      break;
    case 3:
      matchedCallList = (SqlNodeList) operand;
      break;
    case 4:
      notMatchedCallList = (SqlNodeList) operand;
      break;
    case 5:
      sourceSelect = (@Nullable SqlSelect) operand;
      break;
    case 6:
      alias = (SqlIdentifier) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  /** Return the identifier for the target table of this MERGE. */
  public SqlNode getTargetTable() {
    // Based on parser.jj the TargetTable is 1 of two types:
    //
    // 1. If there is no hint. We have updated the parser to output
    // a SqlTableIdentifierWithID
    //
    // 2. There are 1 or more hints. This node is a SqlTableRefWithID
    // where the identifier is a SqlTableIdentifierWithID.
    return targetTable;
  }

  /** Returns the alias for the target table of this MERGE. */
  @Pure
  public @Nullable SqlIdentifier getAlias() {
    return alias;
  }

  /** Returns the source query of this MERGE. */
  public SqlNode getSourceTableRef() {
    return source;
  }

  public void setSourceTableRef(SqlNode tableRef) {
    this.source = tableRef;
  }

  /** Returns the list of WHEN MATCHED clauses statements for this MERGE. */
  public SqlNodeList getMatchedCallList() {
    return matchedCallList;
  }

  /** Returns the INSERT statements for this MERGE. */
  public SqlNodeList getNotMatchedCallList() {
    return notMatchedCallList;
  }

  /** Returns the list of WHEN NOT MATCHED clauses statements for this MERGE. */
  public SqlNode getCondition() {
    return condition;
  }

  /**
   * Gets the source SELECT expression for the data to be updated/inserted.
   * Returns null before the statement has been expanded by
   * {@link SqlValidatorImpl#performUnconditionalRewrites(SqlNode, boolean)}.
   *
   * @return the source SELECT for the data to be updated
   */
  public @Nullable SqlSelect getSourceSelect() {
    return sourceSelect;
  }

  public void setSourceSelect(SqlSelect sourceSelect) {
    this.sourceSelect = sourceSelect;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SELECT, "MERGE INTO", "");
    final int opLeft = getOperator().getLeftPrec();
    final int opRight = getOperator().getRightPrec();
    targetTable.unparse(writer, opLeft, opRight);
    SqlIdentifier alias = this.alias;
    if (alias != null) {
      writer.keyword("AS");
      alias.unparse(writer, opLeft, opRight);
    }

    writer.newlineAndIndent();
    writer.keyword("USING");
    source.unparse(writer, opLeft, opRight);

    writer.newlineAndIndent();
    writer.keyword("ON");
    condition.unparse(writer, opLeft, opRight);


    SqlNodeList matchedCallList = this.matchedCallList;
    for (int i = 0; i < matchedCallList.size(); i++) {

      SqlNode curMatchedClause = matchedCallList.get(i);
      writer.newlineAndIndent();
      writer.keyword("WHEN MATCHED");
      SqlNode cond;

      if (curMatchedClause instanceof SqlUpdate) {
        cond = ((SqlUpdate) curMatchedClause).getCondition();
      } else {
        assert curMatchedClause instanceof SqlDelete;
        cond = ((SqlDelete) curMatchedClause).getCondition();
      }

      if (cond != null) {
        writer.keyword("AND");
        cond.unparse(writer, 0, 0);
      }

      if (curMatchedClause instanceof SqlUpdate) {

        SqlUpdate curUpdateCall = (SqlUpdate) curMatchedClause;
        writer.keyword("THEN UPDATE");

        final SqlWriter.Frame setFrame =
            writer.startList(
                SqlWriter.FrameTypeEnum.UPDATE_SET_LIST,
                "SET",
                "");

        for (Pair<SqlNode, SqlNode> pair : Pair.zip(
            curUpdateCall.targetColumnList, curUpdateCall.sourceExpressionList)) {
          writer.sep(",");
          SqlIdentifier id = (SqlIdentifier) pair.left;
          id.unparse(writer, opLeft, opRight);
          writer.keyword("=");
          SqlNode sourceExp = pair.right;
          sourceExp.unparse(writer, opLeft, opRight);
        }
        writer.endList(setFrame);
      } else {
        assert curMatchedClause instanceof SqlDelete;
        writer.newlineAndIndent();
        writer.keyword("THEN DELETE");
      }
    }


    SqlNodeList insertCallList = this.notMatchedCallList;
    for (int i = 0; i < insertCallList.size(); i++) {
      SqlInsert curInsertCall = (SqlInsert) insertCallList.get(i);
      writer.newlineAndIndent();
      writer.keyword("WHEN NOT MATCHED");
      SqlNode cond = curInsertCall.getCondition();
      if (cond != null) {
        writer.keyword("AND");
        cond.unparse(writer, 0, 0);
      }
      writer.keyword("THEN INSERT");
      SqlNodeList targetColumnList = curInsertCall.getTargetColumnList();
      if (targetColumnList != null) {
        targetColumnList.unparse(writer, opLeft, opRight);
      }
      curInsertCall.getSource().unparse(writer, opLeft, opRight);
    }
    writer.endList(frame);
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateMerge(this);
  }
}
