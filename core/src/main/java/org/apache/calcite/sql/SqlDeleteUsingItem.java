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
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Sql node for representing an item in the.
 * This is heavily based on SqlWithItem which can be found here:
 * core/src/main/java/org/apache/calcite/sql/SqlWithItem.java
 */
public class SqlDeleteUsingItem extends SqlCall {

  public @Nullable SqlIdentifier name;
  public SqlNode query;



  public SqlDeleteUsingItem(SqlParserPos pos, @Nullable SqlIdentifier name, SqlNode query) {
    super(pos);
    this.name = name;
    this.query = query;
  }

  @Override public SqlOperator getOperator() {
    return SqlDeleteUsingItemOperator.INSTANCE;
  }

  /**
   * Helper function used when converting Delete to Merge
   * @return
   */
  public SqlNode getSqlDeleteItemAsJoinExpression() {
    if (name == null) {
      return query;
    } else {
      return SqlStdOperatorTable.AS.createCall(
          this.pos, query, name);
    }

  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, query);
  }

  @SuppressWarnings("assignment.type.incompatible")
  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      name = (SqlIdentifier) operand;
      break;
    case 1:
      query = operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  /**
   * SqlWithItemOperator is used to represent an item in a WITH clause of a
   * query. It has a name, an optional column list, and a query.
   */
  private static class SqlDeleteUsingItemOperator extends SqlSpecialOperator {
    private static final SqlDeleteUsingItemOperator INSTANCE =
        new SqlDeleteUsingItemOperator();

    //We're not casing by this SqlKind anywhere, so just leaving it as "Other" for now.
    SqlDeleteUsingItemOperator() {
      super("DELETE_WITH_ITEM", SqlKind.OTHER, 0);
    }

    //~ Methods ----------------------------------------------------------------

    @Override public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      final SqlDeleteUsingItem usingItem = (SqlDeleteUsingItem) call;
      if (usingItem.name != null) {
        usingItem.name.unparse(writer, getLeftPrec(), getRightPrec());
        writer.keyword("AS");
      }
      usingItem.query.unparse(writer, 10, 10);
    }

    @SuppressWarnings("argument.type.incompatible")
    @Override public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
        SqlParserPos pos, @Nullable SqlNode... operands) {
      assert functionQualifier == null;
      assert operands.length == 2;
      return new SqlDeleteUsingItem(pos, (SqlIdentifier) operands[0], operands[1]);
    }
  }
}
