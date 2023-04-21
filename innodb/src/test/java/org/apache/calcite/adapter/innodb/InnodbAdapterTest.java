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
package org.apache.calcite.adapter.innodb;

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Sources;

import org.apache.commons.collections.CollectionUtils;

import com.alibaba.innodb.java.reader.util.Utils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneRules;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * Tests for the {@code org.apache.calcite.adapter.innodb} package.
 *
 * <p>Will read InnoDB data file {@code emp.ibd} and {@code dept.ibd}.
 */
public class InnodbAdapterTest {

  private static final ImmutableMap<String, String> INNODB_MODEL = ImmutableMap.of("model",
      Sources.of(InnodbAdapterTest.class.getResource("/model.json"))
          .file().getAbsolutePath());

  static List<Pair<Integer, String>> rows = Lists.newArrayList(
      Pair.of(7369, "EMPNO=7369; ENAME=SMITH; JOB=CLERK; AGE=30; MGR=7902; "
          + "HIREDATE=1980-12-17; SAL=800.00; COMM=null; DEPTNO=20; EMAIL=smith@calcite; "
          + "CREATE_DATETIME=2020-01-01 18:35:40; CREATE_TIME=18:35:40; UPSERT_TIME="
          + expectedLocalTime("2020-01-01 18:35:40")),
      Pair.of(7499, "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; AGE=24; MGR=7698; "
          + "HIREDATE=1981-02-20; SAL=1600.00; COMM=300.00; DEPTNO=30; EMAIL=allen@calcite; "
          + "CREATE_DATETIME=2018-04-09 09:00:00; CREATE_TIME=09:00:00; UPSERT_TIME="
          + expectedLocalTime("2018-04-09 09:00:00")),
      Pair.of(7521, "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; AGE=41; MGR=7698; "
          + "HIREDATE=1981-02-22; SAL=1250.00; COMM=500.00; DEPTNO=30; EMAIL=ward@calcite; "
          + "CREATE_DATETIME=2019-11-16 10:26:40; CREATE_TIME=10:26:40; UPSERT_TIME="
          + expectedLocalTime("2019-11-16 10:26:40")),
      Pair.of(7566, "EMPNO=7566; ENAME=JONES; JOB=MANAGER; AGE=28; MGR=7839; "
          + "HIREDATE=1981-02-04; SAL=2975.00; COMM=null; DEPTNO=20; EMAIL=jones@calcite; "
          + "CREATE_DATETIME=2015-03-09 22:16:30; CREATE_TIME=22:16:30; UPSERT_TIME="
          + expectedLocalTime("2015-03-09 22:16:30")),
      Pair.of(7654, "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; AGE=27; MGR=7698; "
          + "HIREDATE=1981-09-28; SAL=1250.00; COMM=1400.00; DEPTNO=30; EMAIL=martin@calcite; "
          + "CREATE_DATETIME=2018-09-02 12:12:56; CREATE_TIME=12:12:56; UPSERT_TIME="
          + expectedLocalTime("2018-09-02 12:12:56")),
      Pair.of(7698, "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; AGE=38; MGR=7839; "
          + "HIREDATE=1981-01-05; SAL=2850.00; COMM=null; DEPTNO=30; EMAIL=blake@calcite; "
          + "CREATE_DATETIME=2018-06-01 14:45:00; CREATE_TIME=14:45:00; UPSERT_TIME="
          + expectedLocalTime("2018-06-01 14:45:00")),
      Pair.of(7782, "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; AGE=32; MGR=7839; "
          + "HIREDATE=1981-06-09; SAL=2450.00; COMM=null; DEPTNO=10; EMAIL=null; "
          + "CREATE_DATETIME=2019-09-30 02:14:56; CREATE_TIME=02:14:56; UPSERT_TIME="
          + expectedLocalTime("2019-09-30 02:14:56")),
      Pair.of(7788, "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; AGE=45; MGR=7566; "
          + "HIREDATE=1987-04-19; SAL=3000.00; COMM=null; DEPTNO=20; EMAIL=scott@calcite; "
          + "CREATE_DATETIME=2019-07-28 12:12:12; CREATE_TIME=12:12:12; UPSERT_TIME="
          + expectedLocalTime("2019-07-28 12:12:12")),
      Pair.of(7839, "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; AGE=22; MGR=null; "
          + "HIREDATE=1981-11-17; SAL=5000.00; COMM=null; DEPTNO=10; EMAIL=king@calcite; "
          + "CREATE_DATETIME=2019-06-08 15:15:15; CREATE_TIME=null; UPSERT_TIME="
          + expectedLocalTime("2019-06-08 15:15:15")),
      Pair.of(7844, "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; AGE=54; MGR=7698; "
          + "HIREDATE=1981-09-08; SAL=1500.00; COMM=0.00; DEPTNO=30; EMAIL=turner@calcite; "
          + "CREATE_DATETIME=2017-08-17 22:01:37; CREATE_TIME=22:01:37; UPSERT_TIME="
          + expectedLocalTime("2017-08-17 22:01:37")),
      Pair.of(7876, "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; AGE=35; MGR=7788; "
          + "HIREDATE=1987-05-23; SAL=1100.00; COMM=null; DEPTNO=20; EMAIL=adams@calcite; "
          + "CREATE_DATETIME=null; CREATE_TIME=23:11:06; UPSERT_TIME="
          + expectedLocalTime("2017-08-18 23:11:06")),
      Pair.of(7900, "EMPNO=7900; ENAME=JAMES; JOB=CLERK; AGE=40; MGR=7698; "
          + "HIREDATE=1981-12-03; SAL=950.00; COMM=null; DEPTNO=30; EMAIL=james@calcite; "
          + "CREATE_DATETIME=2020-01-02 12:19:00; CREATE_TIME=12:19:00; UPSERT_TIME="
          + expectedLocalTime("2020-01-02 12:19:00")),
      Pair.of(7902, "EMPNO=7902; ENAME=FORD; JOB=ANALYST; AGE=28; MGR=7566; "
          + "HIREDATE=1981-12-03; SAL=3000.00; COMM=null; DEPTNO=20; EMAIL=ford@calcite; "
          + "CREATE_DATETIME=2019-05-29 00:00:00; CREATE_TIME=null; UPSERT_TIME="
          + expectedLocalTime("2019-05-29 00:00:00")),
      Pair.of(7934, "EMPNO=7934; ENAME=MILLER; JOB=CLERK; AGE=32; MGR=7782; "
          + "HIREDATE=1982-01-23; SAL=1300.00; COMM=null; DEPTNO=10; EMAIL=null; "
          + "CREATE_DATETIME=2016-09-02 23:15:01; CREATE_TIME=23:15:01; UPSERT_TIME="
          + expectedLocalTime("2016-09-02 23:15:01"))
  );

  static List<Pair<Integer, String>> reversedRows = rows.stream()
      .sorted(Comparator.reverseOrder()).collect(toList());

  static Map<Integer, String> empnoMap = rows.stream()
      .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

  /**
   * Whether to run this test.
   */
  private boolean enabled() {
    return CalciteSystemProperty.TEST_INNODB.value();
  }

  private CalciteAssert.AssertQuery sql(String sql) {
    return CalciteAssert.that()
        .with(INNODB_MODEL)
        .enable(enabled())
        .query(sql);
  }

  Hook.Closeable closeable;

  @BeforeEach
  public void before() {
    this.closeable =
        Hook.SQL2REL_CONVERTER_CONFIG_BUILDER.addThread(
            InnodbAdapterTest::assignHints);
  }

  @AfterEach
  public void after() {
    if (this.closeable != null) {
      this.closeable.close();
      this.closeable = null;
    }
  }

  static void assignHints(Holder<SqlToRelConverter.Config> configHolder) {
    HintStrategyTable strategies = HintStrategyTable.builder()
        .hintStrategy("index", HintPredicates.TABLE_SCAN)
        .build();
    configHolder.accept(config -> config.withHintStrategyTable(strategies));
  }

  private static String expectedLocalTime(String dateTime) {
    ZoneRules rules = ZoneId.systemDefault().getRules();
    LocalDateTime ldt = Utils.parseDateTimeText(dateTime);
    Instant instant = ldt.toInstant(ZoneOffset.of("+00:00"));
    ZoneOffset standardOffset = rules.getOffset(instant);
    OffsetDateTime odt = instant.atOffset(standardOffset);
    return odt.toLocalDateTime().format(Utils.TIME_FORMAT_TIMESTAMP[0]);
  }

  private static String all() {
    return String.join("\n", Pair.right(rows)) + "\n";
  }

  private static String allReversed() {
    return String.join("\n", Pair.right(reversedRows)) + "\n";
  }

  private static String someEmpnoGt(int empno) {
    return some(rows.stream().map(Pair::getKey).filter(i -> i > empno).collect(toList()));
  }

  private static String someEmpnoGte(int empno) {
    return some(rows.stream().map(Pair::getKey).filter(i -> i >= empno).collect(toList()));
  }

  private static String someEmpnoLt(int empno) {
    return some(rows.stream().map(Pair::getKey).filter(i -> i < empno).collect(toList()));
  }

  private static String someEmpnoLte(int empno) {
    return some(rows.stream().map(Pair::getKey).filter(i -> i <= empno).collect(toList()));
  }

  private static String some(int... empnos) {
    return some(Arrays.stream(empnos).boxed().collect(toList()));
  }

  private static String some(List<Integer> empnos) {
    if (empnos == null) {
      return "";
    }
    List<String> result = empnos.stream()
        .map(empno -> empnoMap.get(empno)).collect(toList());
    return join(result);
  }

  private static String join(List<String> empList) {
    if (CollectionUtils.isEmpty(empList)) {
      return "";
    }
    return String.join("\n", empList) + "\n";
  }
}
