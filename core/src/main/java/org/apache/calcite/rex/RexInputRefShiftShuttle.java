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
package org.apache.calcite.rex;

/**
 * Shuttle which shifts all input refs by some fixed amount.
 * throws an error if this would cause the inputRefs to be invalid (negative index)
 */
public class RexInputRefShiftShuttle extends RexShuttle {

  private final int shiftAmount;
  private final RexBuilder builder;

  public RexInputRefShiftShuttle(RexBuilder builder, int shiftAmount) {
    super();
    this.shiftAmount = shiftAmount;
    this.builder = builder;
  }

  @Override public RexNode visitTableInputRef(RexTableInputRef ref) {
    System.out.println("TODO!");
    return builder.makeInputRef(ref.type, ref.getIndex());
  }

}
