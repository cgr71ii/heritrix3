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
package org.archive.crawler.frontier;

import org.archive.crawler.util.PUC;

import org.archive.modules.CrawlURI;

import org.archive.spring.KeyedProperties;

import org.archive.net.UURI;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.nio.file.Paths;

/**
 * A CostAssignment policy that checks parts of the authority in order to
 * assign the cost.
 * 
 * @author cgr71ii
 */
public class AuthorityCostAssignmentPolicy extends CostAssignmentPolicy {
    private static final long serialVersionUID = 1L;

    private static final Logger logger = Logger.getLogger(AuthorityCostAssignmentPolicy.class.getName());

    protected KeyedProperties kp = new KeyedProperties();
    public KeyedProperties getKeyedProperties() {
        return kp;
    }

    {
        setLoggerFine(false);
    }

    public Boolean getLoggerFine() {
        return (Boolean) kp.get("loggerFine");
    }

    public void setLoggerFine(Boolean logger_fine) {
        if (logger_fine) {
            logger.setLevel(Level.FINE);
        }
        else {
            logger.setLevel(Level.INFO);
        }

        kp.put("loggerFine", logger_fine);
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.frontier.CostAssignmentPolicy#costOf(org.archive.crawler.datamodel.CrawlURI)
     */
    public int costOf(CrawlURI curi) {
        UURI uri = curi.getUURI();
        UURI via = curi.getVia();
        String str_uri = uri.toCustomString();
        int cost = 50;
        String uri_file = "";

        try {
            uri_file = Paths.get(uri.getPath()).getFileName().toString();
        }
        catch (Exception e) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.warning("URI path exception: " + e.toString());
            }
        }

        if (uri_file.equals("robots.txt")){
            cost = 1;
        }
        else if (via != null) {
            String str_via = via.toCustomString();
            String uri_domain = PUC.getDomain(str_uri, logger);
            String via_domain = PUC.getDomain(str_via, logger);

            if (uri_domain.equals(via_domain)) {
                // Same domain
                cost = 1;
            }
            else if (logger.isLoggable(Level.FINE)) {
                logger.fine(String.format("Different domain: %s vs %s", str_via, str_uri));
            }
        }
        else {
            cost = 1;

            if (logger.isLoggable(Level.FINE)) {
                logger.fine(String.format("via is null. uri: (cost: %d) %s", cost, str_uri));
            }
        }

        return 1;
    }

}
