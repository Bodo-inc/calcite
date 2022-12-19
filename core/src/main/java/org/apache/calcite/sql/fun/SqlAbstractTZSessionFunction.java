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

package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.BodoTZInfo;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlMonotonicity;

import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.sql.validate.SqlNonNullableAccessors.getOperandLiteralValueOrThrow;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Base class for functions such as CURRENT_TIMESTAMP that return
 * TZ_Aware data dependent on the current TZ_INFO session parameter.
 */
public class SqlAbstractTZSessionFunction extends SqlFunction {

  //~ Static fields/initializers ---------------------------------------------
  private static final SqlOperandTypeChecker OTC_CUSTOM =
      OperandTypes.or(
          OperandTypes.POSITIVE_INTEGER_LITERAL, OperandTypes.NILADIC);

  /**
   * Creates a new SqlFunction for a call to a built-in function.
   *
   * @param name                 Name of built-in function
   * @param kind                 kind of operator implemented by function
   * @param returnTypeInference  strategy to use for return type inference
   * @param operandTypeInference strategy to use for parameter type inference
   * @param operandTypeChecker   strategy to use for parameter type checking
   * @param category             categorization for function
   */
  public SqlAbstractTZSessionFunction(String name, SqlKind kind, @Nullable SqlReturnTypeInference returnTypeInference, @Nullable SqlOperandTypeInference operandTypeInference, @Nullable SqlOperandTypeChecker operandTypeChecker, SqlFunctionCategory category) {
    super(name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, category);
  }

  public SqlAbstractTZSessionFunction(String name) {
    super(name, SqlKind.OTHER_FUNCTION, null, null, OTC_CUSTOM, SqlFunctionCategory.TIMEDATE);
  }

  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.FUNCTION_ID;
  }

  @Override public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    // TODO: Replace with a session parameter for the current Timezone.
    return opBinding.getTypeFactory().createTZAwareSqlType(BodoTZInfo.UTC);
  }


  // All TZ-Aware Timestamp functions are increasing. Not strictly increasing.
  // This is based on this Class replacing other uses of SqlAbstractionTimeFunction,
  // which is also increasing.
  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    return SqlMonotonicity.INCREASING;
  }

  // Plans referencing context variables should never be cached
  @Override public boolean isDynamicFunction() {
    return true;
  }
}
