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

import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.parser.bodo.SqlBodoParserImpl;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlValidatorTester;

import org.junit.jupiter.api.Test;

/**
 * Module for validator tests that require the Bodo Parser.
 */
public class BodoSqlValidatorTestCase extends SqlValidatorTestCase {
  public static final SqlValidatorFixture FIXTURE =
      new SqlValidatorFixture(SqlValidatorTester.DEFAULT,
          SqlTestFactory.INSTANCE.withParserConfig(
              c -> c.withParserFactory(SqlBodoParserImpl.FACTORY)),
          StringAndPos.of("?"), false, false);

  @Override public SqlValidatorFixture fixture() {
    return FIXTURE;
  }

  @Test void testMultipleSameAsPass() {
    sql("select 1 as again,2 as \"again\", 3 as AGAiN from (values (true))")
        .ok();
  }

}
