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

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.TZAwareSqlType;

import com.google.common.collect.Sets;

import java.util.Set;

import static org.apache.calcite.sql.fun.BodoSqlTimestampAddFunction.deduceType;
import static org.apache.calcite.sql.fun.BodoSqlTimestampAddFunction.standardizeTimeUnit;
import static org.apache.calcite.sql.validate.SqlNonNullableAccessors.getOperandLiteralValueOrThrow;
import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/**
 * The <code>DATEADD</code> function, which adds an interval to a
 * datetime (TIMESTAMP, TIME or DATE).
 */
public class BodoSqlDateAddFunction extends SqlFunction {
  private static final SqlReturnTypeInference RETURN_TYPE_INFERENCE =
      opBinding -> {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        assert opBinding instanceof SqlCallBinding;
        SqlCallBinding opBindingWithCast = (SqlCallBinding) opBinding;
        String fnName = opBindingWithCast.getCall().getOperator().getName();
        RelDataType ret;
        RelDataType arg0Type = opBindingWithCast.getOperandType(0);
        RelDataType arg1Type = opBindingWithCast.getOperandType(1);
        if (opBindingWithCast.getOperandCount() == 3) { // Snowflake DATEADD
          TimeUnit arg0timeUnit;
          RelDataType arg2Type = opBindingWithCast.getOperandType(2);
          switch (arg0Type.getSqlTypeName()) {
          // This must be a constant string or time unit input,
          // due to the way that we handle the parsing
          case CHAR:
          case VARCHAR:
            //This will fail if the value is a non-literal
            try {
              String inputTimeStr =
                  requireNonNull(opBindingWithCast.getOperandLiteralValue(0, String.class));
              arg0timeUnit = standardizeTimeUnit(fnName,
                  inputTimeStr,
                  arg2Type.getSqlTypeName() == SqlTypeName.TIME);
            } catch (RuntimeException e) {
              String errMsg = requireNonNull(e.getMessage());
              throw opBindingWithCast.getValidator().newValidationError(opBindingWithCast.getCall(),
                  RESOURCE.wrongTimeUnit(fnName, errMsg));
            }
            break;

          default:
            arg0timeUnit = getOperandLiteralValueOrThrow(opBinding, 0, TimeUnit.class);
          }

          try {
            ret = deduceType(typeFactory, arg0timeUnit,
                opBinding.getOperandType(1), opBinding.getOperandType(2), fnName);
          } catch (RuntimeException e) {
            String errMsg = requireNonNull(e.getMessage());
            throw opBindingWithCast.getValidator().newValidationError(opBindingWithCast.getCall(),
                RESOURCE.wrongTimeUnit(fnName, errMsg));
          }
        } else { // MySQL DATEADD
          if (arg1Type.getSqlTypeName().equals(SqlTypeName.INTEGER)) {
            // when the second argument is integer, it is equivalent to adding day interval
            if (arg0Type.getSqlTypeName().equals(SqlTypeName.DATE)) {
              ret = typeFactory.createSqlType(SqlTypeName.DATE);
            } else if (arg0Type instanceof TZAwareSqlType) {
              ret = arg0Type;
            } else {
              ret = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
            }
          } else {
            // if the first argument is date, the return type depends on the interval type
            if (arg0Type.getSqlTypeName().equals(SqlTypeName.DATE)) {
              Set<SqlTypeName> date_interval_type =
                  Sets.immutableEnumSet(SqlTypeName.INTERVAL_YEAR_MONTH,
                      SqlTypeName.INTERVAL_YEAR,
                      SqlTypeName.INTERVAL_MONTH,
                      SqlTypeName.INTERVAL_WEEK,
                      SqlTypeName.INTERVAL_DAY);
              if (date_interval_type.contains(arg1Type.getSqlTypeName())) {
                ret = typeFactory.createSqlType(SqlTypeName.DATE);
              } else {
                ret = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
              }
            } else if (arg0Type instanceof TZAwareSqlType) {
              ret = arg0Type;
            } else {
              ret = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
            }
          }
        }
        return ret;
      };


  /** Creates a BodoSqlDateAddFunction. */
  BodoSqlDateAddFunction() {
    super("DATEADD", SqlKind.OTHER, RETURN_TYPE_INFERENCE, null,
        OperandTypes.or(
            OperandTypes.sequence(
                "DATEADD(UNIT, VALUE, DATETIME)",
                OperandTypes.ANY,
                OperandTypes.INTEGER,
                OperandTypes.DATETIME),
            OperandTypes.sequence(
                "DATEADD(DATETIME_OR_DATETIME_STRING, INTERVAL_OR_INTEGER)",
                OperandTypes.or(OperandTypes.DATETIME, OperandTypes.STRING),
                OperandTypes.or(OperandTypes.INTERVAL, OperandTypes.INTEGER))),
        SqlFunctionCategory.TIMEDATE);
  }
}
