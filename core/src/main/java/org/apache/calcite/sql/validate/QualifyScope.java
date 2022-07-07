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
package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

/**
 * Scope of a Qualify clause. Mostly a wrapper around the parent's selectScope, but has some utility
 * for checking validity of the qualify clause.
 */
public class QualifyScope extends DelegatingScope  implements WindowedSelectScope {

  //~ Instance fields --------------------------------------------------------

  private final SqlNode qualifyNode;
  private final SqlSelect select;

  //~ Constructors -----------------------------------------------------------

  QualifyScope(
      SqlValidatorScope parent,
      SqlNode qualifyNode,
      SqlSelect select) {
    super(parent);
    this.qualifyNode = qualifyNode;
    this.select = select;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean checkWindowedAggregateExpr(SqlNode expr, boolean deep) {
    //TODO:
    return false;
  }

  @Override public SqlNode getNode() {
    return qualifyNode;
  }


}
