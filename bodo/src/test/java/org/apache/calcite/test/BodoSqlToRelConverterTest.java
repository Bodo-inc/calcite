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
    // Tests create table with IF NOT exists specified
    final String sql = "CREATE TABLE IF NOT EXISTS out_test AS select * from emp";
    sql(sql).ok();
  }


  @Test void testCreateOrReplaceTable() {
    // Tests create table with Replace specified
    final String sql = "CREATE OR REPLACE TABLE CUSTOMER.out_test AS\n"
        + "select dept.deptno, emp.empno\n"
        + " from emp join dept on emp.deptno = dept.deptno";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testCreateTableOrderByExpr() {
    // Test where create table has an order by operation that generates additional columns
    final String sql = "CREATE TABLE out_test as select emp.empno from emp "
        + "order by emp.empno is not null";
    sql(sql).ok();
  }



  @Test void testCreateTableRewrite() {
    // Tests create table with a query that will require unconditional rewriting
    final String sql = "CREATE TABLE foo as select * from dept limit 10";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testCreateTableLike() {
    // Tests create table like
    final String sql = "CREATE TABLE CUSTOMER.out_test LIKE\n"
        + "emp";
    sql(sql).withExtendedTester().ok();
  }

  @Test void testCreateOrReplaceTableLike() {
    // Tests create table like with Replace specified
    final String sql = "CREATE OR REPLACE TABLE CUSTOMER.out_test LIKE\n"
        + "emp";
    sql(sql).withExtendedTester().ok();
  }
  @Test void testCreateIfNotExistsTableLike() {
    // Tests create table like with IF NOT EXISTS specified
    final String sql = "CREATE TABLE IF NOT EXISTS CUSTOMER.out_test LIKE\n"
        + "emp";
    sql(sql).withExtendedTester().ok();
  }



  @Test void testCreateTableWith() {
    // Tests create table with a query that uses "with" syntax
    final String sql = "CREATE TABLE foo as\n"
        +
        "with temporaryTable as (select * from dept limit 10),\n"
        +
        "temporaryTable2 as (select * from dept limit 10)\n"
        +
        "SELECT * from temporaryTable join temporaryTable2\n"
        +
        "on temporaryTable.deptno = temporaryTable2.deptno";
    sql(sql).withExtendedTester().ok();
  }
  @Test void testValuesUnreserved() {
    //Test that confirms we can use "values" as a column name, and table name
    final String sql = "SELECT ename, dept2.values + values.values FROM\n"
        +
        "(select deptno as values from dept) dept2 JOIN\n"
        +
        "(select ename, deptno as values from emp) values\n"
        +
        "on values.values = dept2.values";
    sql(sql).ok();
  }

  @Test void testValueUnreserved() {
    //Test that confirms we can use "value" as a column name, and table name
    final String sql = "SELECT ename, dept2.value + value.value FROM\n"
        +
        "(select deptno as value from dept) dept2 JOIN\n"
        +
        "(select ename, deptno as value from emp) value\n"
        +
        "on value.value = dept2.value";
    sql(sql).ok();
  }

  @Test void testCreateTableOrderBy() {
    // Tests an error specific to CREATE TABLE with "WITH" and "ORDER BY" clauses during
    // SqlToRelConversion. The converter was previously optimizing out the sort node,
    // but its parent process expected the conversion collation to exist, which lead
    // to an assertion error.

    final String sql = "CREATE TABLE testing_output AS ("
        +
        "with part_two as (\n"
        +
        "        select 'foo' as p_partkey from (VALUES (1, 2, 3))\n"
        +
        "    )\n"
        +
        "    select\n"
        +
        "                       p_partkey\n"
        +
        "                     from\n"
        +
        "                       part_two\n"
        +
        "                     order by\n"
        +
        "                       p_partkey"
        +
        ")";
    sql(sql).ok();
  }

  @Test void testOrderByNoCreateTable() {
    // Tests that the default path for a query with "WITH" and "ORDER BY" clauses still works.
    // This test should not be necessary at this time, but it may be useful in
    // the case that we attempt to resolve the "WITH"/"ORDER BY" clause issue in a more performant
    // way:
    // https://bodo.atlassian.net/browse/BE-4483
    final String sql =
        "with part_two as (\n"
        +
        "        select 'foo' as p_partkey from (VALUES (1, 2, 3))\n"
        +
        "    )\n"
        +
        "    select\n"
        +
        "                       p_partkey\n"
        +
        "                     from\n"
        +
        "                       part_two\n"
        +
        "                     order by\n"
        +
        "                       p_partkey";
    sql(sql).ok();
  }

}
