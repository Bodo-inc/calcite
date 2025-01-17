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
package org.apache.calcite.prepare;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.StarTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Context for populating a {@link Prepare.Materialization}.
 */
class CalciteMaterializer extends CalcitePrepareImpl.CalcitePreparingStmt {
  CalciteMaterializer(CalcitePrepareImpl prepare,
      CalcitePrepare.Context context,
      CatalogReader catalogReader, CalciteSchema schema,
      RelOptCluster cluster, SqlRexConvertletTable convertletTable) {
    super(prepare, context, catalogReader, catalogReader.getTypeFactory(),
        schema, EnumerableRel.Prefer.ANY, cluster, BindableConvention.INSTANCE,
        convertletTable);
  }

  /** Populates a materialization record, converting a table path
   * (essentially a list of strings, like ["hr", "sales"]) into a table object
   * that can be used in the planning process. */
  void populate(Materialization materialization) {
    SqlParser parser = SqlParser.create(materialization.sql);
    SqlNode node;
    try {
      node = parser.parseStmt();
    } catch (SqlParseException e) {
      throw new RuntimeException("parse failed", e);
    }
    final SqlToRelConverter.Config config =
        SqlToRelConverter.config().withTrimUnusedFields(true);
    SqlToRelConverter sqlToRelConverter2 =
        getSqlToRelConverter(getSqlValidator(), catalogReader, config);

    RelRoot root = sqlToRelConverter2.convertQuery(node, true, true);
    materialization.queryRel = trimUnusedFields(root).rel;

    // Identify and substitute a StarTable in queryRel.
    //
    // It is possible that no StarTables match. That is OK, but the
    // materialization patterns that are recognized will not be as rich.
    //
    // It is possible that more than one StarTable matches. TBD: should we
    // take the best (whatever that means), or all of them?
    useStar(schema, materialization);

    List<String> tableName = materialization.materializedTable.path();
    RelOptTable table = requireNonNull(
        this.catalogReader.getTable(tableName),
        () -> "table " + tableName + " is not found");
    materialization.tableRel = sqlToRelConverter2.toRel(table,
        ImmutableList.of(), false);
  }

  /** Converts a relational expression to use a
   * {@link StarTable} defined in {@code schema}.
   * Uses the first star table that fits. */
  private void useStar(CalciteSchema schema, Materialization materialization) {
    RelNode queryRel = requireNonNull(materialization.queryRel, "materialization.queryRel");
    for (Callback x : useStar(schema, queryRel)) {
      // Success -- we found a star table that matches.
      materialization.materialize(x.rel, x.starRelOptTable);
      if (CalciteSystemProperty.DEBUG.value()) {
        System.out.println("Materialization "
            + materialization.materializedTable + " matched star table "
            + x.starTable + "; query after re-write: "
            + RelOptUtil.toString(queryRel));
      }
    }
  }

  /** Converts a relational expression to use a
   * {@link org.apache.calcite.schema.impl.StarTable} defined in {@code schema}.
   * Uses the first star table that fits. */
  private Iterable<Callback> useStar(CalciteSchema schema, RelNode queryRel) {
    List<CalciteSchema.TableEntry> starTables =
        Schemas.getStarTables(schema.root());
    if (starTables.isEmpty()) {
      // Don't waste effort converting to leaf-join form.
      return ImmutableList.of();
    }
    final List<Callback> list = new ArrayList<>();
    final RelNode rel2 =
        RelOptMaterialization.toLeafJoinForm(queryRel);
    for (CalciteSchema.TableEntry starTable : starTables) {
      final Table table = starTable.getTable();
      assert table instanceof StarTable;
      RelOptTableImpl starRelOptTable =
          RelOptTableImpl.create(catalogReader, table.getRowType(typeFactory),
              starTable, null);
      final RelNode rel3 =
          RelOptMaterialization.tryUseStar(rel2, starRelOptTable);
      if (rel3 != null) {
        list.add(new Callback(rel3, starTable, starRelOptTable));
      }
    }
    return list;
  }

  /** Called when we discover a star table that matches. */
  static class Callback {
    public final RelNode rel;
    public final CalciteSchema.TableEntry starTable;
    public final RelOptTableImpl starRelOptTable;

    Callback(RelNode rel,
        CalciteSchema.TableEntry starTable,
        RelOptTableImpl starRelOptTable) {
      this.rel = rel;
      this.starTable = starTable;
      this.starRelOptTable = starRelOptTable;
    }
  }
}
