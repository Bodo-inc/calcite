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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.TableModify}
 * not targeted at any particular engine or calling convention.
 */
public final class LogicalTableModify extends TableModify {
  //~ Constructors -----------------------------------------------------------

  private final @Nullable List<Pair<MatchAction, RexNode>> updateColumnsListList;
  private final @Nullable List<Pair<NotMatchedAction, RexNode>> insertColumnsListList;

  /**
   * Creates a LogicalTableModify.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   */
  public LogicalTableModify(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, Prepare.CatalogReader schema, RelNode input,
      Operation operation,
      // For non-merge operations, these columns should now be NULL
      @Nullable List<String> updateColumnList,
      @Nullable List<RexNode> sourceExpressionList,
      boolean flattened,
      // These are the fields used for MERGE INTO
      // for all other operations, they should be NULL
      @Nullable List<Pair<MatchAction, RexNode>> updateColumnsListList,
      @Nullable List<Pair<NotMatchedAction, RexNode>> insertColumnsListList) {
    super(cluster, traitSet, table, schema, input, operation, updateColumnList,
        sourceExpressionList, flattened);
    this.updateColumnsListList = updateColumnsListList;
    this.insertColumnsListList = insertColumnsListList;
  }

  /**
   * Creates a LogicalTableModify by parsing serialized output.
   */
  public LogicalTableModify(RelInput input) {
    super(input);
    // For Bodo, we're never handling serialized output
    this.updateColumnsListList = null;
    this.insertColumnsListList = null;
  }

  @Deprecated // to be removed before 2.0
  public LogicalTableModify(RelOptCluster cluster, RelOptTable table,
      Prepare.CatalogReader schema, RelNode input, Operation operation,
      List<String> updateColumnList, boolean flattened,
      @Nullable List<Pair<MatchAction, RexNode>> updateColumnsListList,
      @Nullable List<Pair<NotMatchedAction, RexNode>> insertColumnsListList) {
    this(cluster,
        cluster.traitSetOf(Convention.NONE),
        table,
        schema,
        input,
        operation,
        updateColumnList,
        null,
        flattened,
        updateColumnsListList,
        insertColumnsListList);
  }

  /** Creates a LogicalTableModify. */
  public static LogicalTableModify create(RelOptTable table,
      Prepare.CatalogReader schema, RelNode input,
      Operation operation, @Nullable List<String> updateColumnList,
      @Nullable List<RexNode> sourceExpressionList,
      boolean flattened,
      @Nullable List<Pair<MatchAction, RexNode>> updateColumnsListList,
      @Nullable List<Pair<NotMatchedAction, RexNode>> insertColumnsListList) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalTableModify(cluster, traitSet, table, schema, input,
        operation, updateColumnList, sourceExpressionList, flattened,
        updateColumnsListList, insertColumnsListList);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalTableModify copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalTableModify(getCluster(), traitSet, table, catalogReader,
        sole(inputs), getOperation(), getUpdateColumnList(),
        getSourceExpressionList(), isFlattened(), updateColumnsListList, insertColumnsListList);
  }

  public @Nullable List<Pair<MatchAction, RexNode>> getUpdateColumnsListList() {
    return this.updateColumnsListList;
  }

  public @Nullable List<Pair<NotMatchedAction, RexNode>> getInsertColumnsListList() {
    return this.insertColumnsListList;
  }


  /**
   * MatchAction is a wrapper around either a List of Pairs of String and RexNode,
   * which is the columns to
   * update and the expressions to use for updating, or a DELETE action.
   *
   * NonMatchedAction is a List of Pairs of String and RexNode,
   * which is the columns to insert, and the
   * expression to use for inserting.
   *
   * TODO: is there a better way to index into columns besides name? I could probably figure out
   * how to index properly into the dest table, especially since the columns in the dest table
   * should be constant, relative to the query's plan.
   *
   */
  public static class MatchAction {
    private final boolean isDelete;
    private final @Nullable List<Pair<String, RexNode>> updateAction;

    MatchAction(boolean isDelete, @Nullable List<Pair<String, RexNode>> updateAction) {
      this.isDelete = isDelete;
      this.updateAction = updateAction;
    }

    public boolean isDelete() {
      return isDelete;
    }

    public List<Pair<String, RexNode>> getUpdateAction() {
      if (updateAction == null) {
        throw new RuntimeException("Error, attempted to get update action from a Delete action");
      }
      return updateAction;
    }
  }

  /**
   * See above.
   */
  public static class NotMatchedAction {
    private final List<Pair<String, RexNode>> insertAction;

    NotMatchedAction(List<Pair<String, RexNode>> insertAction) {
      this.insertAction = insertAction;
    }

    public List<Pair<String, RexNode>> getInsertAction() {
      return insertAction;
    }
  }
}
