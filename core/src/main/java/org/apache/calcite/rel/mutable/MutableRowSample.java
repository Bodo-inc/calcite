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
package org.apache.calcite.rel.mutable;

import org.apache.calcite.plan.RelOptRowSamplingParameters;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/** Mutable equivalent of {@link org.apache.calcite.rel.core.RowSample}. */
public class MutableRowSample extends MutableSingleRel {
  public final RelOptRowSamplingParameters params;

  private MutableRowSample(MutableRel input, RelOptRowSamplingParameters params) {
    super(MutableRelType.ROW_SAMPLE, input.rowType, input);
    this.params = params;
  }

  /**
   * Creates a MutableRowSample.
   *
   * @param input   Input relational expression
   * @param params  parameters necessary to produce a sample of a relation
   */
  public static MutableRowSample of(
      MutableRel input, RelOptRowSamplingParameters params) {
    return new MutableRowSample(input, params);
  }

  @Override public boolean equals(@Nullable Object obj) {
    return obj == this
        || obj instanceof MutableRowSample
        && params.equals(((MutableRowSample) obj).params)
        && input.equals(((MutableRowSample) obj).input);
  }

  @Override public int hashCode() {
    return Objects.hash(input, params);
  }

  @Override public StringBuilder digest(StringBuilder buf) {
    return buf.append("RowSample(mode: ")
        .append(params.isBernoulli() ? "bernoulli" : "system")
        .append("rows")
        .append(params.getNumberOfRows())
        .append("repeatableSeed")
        .append(params.isRepeatable() ? params.getRepeatableSeed() : "-")
        .append(")");
  }

  @Override public MutableRel clone() {
    return MutableRowSample.of(input.clone(), params);
  }
}
