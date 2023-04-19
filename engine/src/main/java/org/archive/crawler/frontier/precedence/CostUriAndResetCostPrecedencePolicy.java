/*
 *  This file is part of the Heritrix web crawler (crawler.archive.org).
 *
 *  Licensed to the Internet Archive (IA) by one or more individual 
 *  contributors. 
 *
 *  The IA licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.archive.crawler.frontier.precedence;

import org.archive.modules.CrawlURI;
import org.archive.spring.HasKeyedProperties;
import org.archive.spring.KeyedProperties;

/**
 * UriPrecedencePolicy which sets a URI's precedence to its 'cost' -- which
 * simulates the in-queue sorting order in Heritrix 1.x, where cost 
 * contributed the same bits to the queue-insert-key that precedence now does.
 * 
 * This class assumes that FrontierPrepararer calcules the cost and right after
 * that step it sets the precedence
 */
public class CostUriAndResetCostPrecedencePolicy extends UriPrecedencePolicy 
implements HasKeyedProperties {
    private static final long serialVersionUID = -8164425278358540710L;

    protected KeyedProperties kp = new KeyedProperties();
    public KeyedProperties getKeyedProperties() {
        return kp;
    }

    {
        setFinalCost(1);
    }
    public int getFinalCost() {
        return (Integer) kp.get("finalCost");
    }
    /** constant cost to assign; default is 1 */
    public void setFinalCost(int final_cost) {
        kp.put("finalCost", final_cost);
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.frontier.precedence.UriPrecedencePolicy#uriScheduled(org.archive.crawler.datamodel.CrawlURI)
     */
    @Override
    public void uriScheduled(CrawlURI curi) {
        curi.setPrecedence(curi.getHolderCost()); 

        // reset cost
        curi.setHolderCost(getCost(curi));
    }

    private int getCost(CrawlURI curi) {
        return getFinalCost();
    }
}
