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
package org.apache.calcite.test;

import org.apache.calcite.sql.parser.bodo.SqlBodoParserImpl;

import org.junit.jupiter.api.Test;

/**
 * Checks SqlToRel conversion for operations specific to the Bodo parser. Any changes made
 * directly to the core parser should be tested in the core SqlToRelTest file.
 */
public class BodoSqlToRelConverterTest extends SqlToRelTestBase {

  //Set the default SqlToRel Fixture to use the Bodo parser.
  private static final SqlToRelFixture LOCAL_FIXTURE =
      SqlToRelFixture.DEFAULT
          .withDiffRepos(DiffRepository.lookup(BodoSqlToRelConverterTest.class))
          .withFactory(
              f -> f.withParserConfig(
                c -> c.withParserFactory(SqlBodoParserImpl.FACTORY)));

  @Override public SqlToRelFixture fixture() {
    return LOCAL_FIXTURE;
  }

  @Test void testWithBodoParser() {
    // Simple test to confirm that we correctly read the expected output
    // from the XML file
    final String sql = "select 1, 2, 3 from emp";
    sql(sql).ok();
  }

  @Test void testCreateTableSimple() {
    // Simple test to confirm that we can handle create table statements
    final String sql = "CREATE TABLE out_test AS select 1, 2, 3 from emp";
    sql(sql).ok();
  }

  @Test void testCreateTableIfNotExists() {
    // Tests create table with
    final String sql = "CREATE TABLE out_test IF NOT EXISTS AS select * from emp";
    sql(sql).ok();
  }


  @Test void testCreateOrReplaceTable() {
    // Tests create table with
    final String sql = "CREATE OR REPLACE TABLE out_test IF NOT EXISTS AS\n"
        + "select deptno, empname, empno from emp";

    sql(sql).ok();
  }


}
