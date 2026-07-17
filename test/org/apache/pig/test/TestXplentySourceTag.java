/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.parser.SourceLocation;
import org.junit.Test;

/**
 * XPLENTY: regression test for the source-tag change. The tag format
 * " (at <alias>[<line>,<offset>])" is a downstream contract (xplenty
 * log-attribution parses it), and stamping must be additive: no tag when no
 * identity, and NEVER a change to the operator's own alias field.
 */
public class TestXplentySourceTag {

    private static final class ProbeOp extends PhysicalOperator {
        private static final long serialVersionUID = 1L;
        ProbeOp() { super(new OperatorKey("scope", 1L)); }
        public String tag() { return xplentySourceTag(); }
        @Override public void visit(PhyPlanVisitor v) throws VisitorException {}
        @Override public String name() { return "Probe"; }
        @Override public boolean supportsMultipleInputs() { return false; }
        @Override public boolean supportsMultipleOutputs() { return false; }
        @Override public Result processInput() { return new Result(); }
        @Override public org.apache.pig.data.Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) { return null; }
    }

    @Test
    public void noIdentityRendersEmptyTagSoMessagesStayStockIdentical() {
        assertEquals("", new ProbeOp().tag());
    }

    @Test
    public void stampedIdentityRendersTheContractFormat() {
        ProbeOp op = new ProbeOp();
        op.xplentyStampLocation("sel_cols", new SourceLocation(null, 410, 11));
        assertEquals(" (at sel_cols[410,11])", op.tag());
    }

    @Test
    public void multipleLocationsRenderCommaJoinedInOneParen() {
        ProbeOp op = new ProbeOp();
        op.xplentyStampLocation("a", new SourceLocation(null, 1, 2));
        op.xplentyStampLocation("b", new SourceLocation(null, 3, 4));
        assertEquals(" (at a[1,2], b[3,4])", op.tag());
    }

    @Test
    public void stampingNeverTouchesTheAliasField() {
        // addOriginalLocation overwrites this.alias (EXPLAIN/plan output);
        // the location-only stamping path must not.
        ProbeOp op = new ProbeOp();
        op.xplentyStampLocation("sel_cols", new SourceLocation(null, 410, 11));
        assertNull(op.getAlias());
        List<PhysicalOperator.OriginalLocation> locs = op.getOriginalLocations();
        assertEquals(1, locs.size());
        assertEquals("sel_cols", locs.get(0).getAlias());
        assertTrue(op.name().startsWith("Probe"));
    }
}
