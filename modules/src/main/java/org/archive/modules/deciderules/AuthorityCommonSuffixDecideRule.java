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
package org.archive.modules.deciderules;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.net.URI;

import org.archive.modules.CrawlURI;
import org.archive.net.UURI;

/**
 * Applies its decision if the current URI differs in that portion of
 * its hostname/domain that is assigned/sold by registrars, its
 * 'assignment-level-domain' (ALD) (AKA 'public suffix' or in previous 
 * Heritrix versions, 'topmost assigned SURT')
 * 
 * @author Olaf Freyer
 */
public class AuthorityCommonSuffixDecideRule extends PredicatedDecideRule {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger
            .getLogger(AuthorityCommonSuffixDecideRule.class.getName());

    public AuthorityCommonSuffixDecideRule() {
    }

    {
        setDifferentSuffix(false);
        setCommonSuffix("");
        setLoggerFine(false);
    }

    public Boolean getDifferentSuffix() {
        return (Boolean) kp.get("differentSuffix");
    }

    public void setDifferentSuffix(Boolean different_suffix) {
        kp.put("differentSuffix", different_suffix);
    }

    public String getCommonSuffix() {
        return (String) kp.get("commonSuffix");
    }

    public void setCommonSuffix(String common_suffix) {
        kp.put("commonSuffix",common_suffix);
    }

    public Boolean getLoggerFine() {
        return (Boolean) kp.get("loggerFine");
    }

    public void setLoggerFine(Boolean logger_fine) {
        if (logger_fine) {
            LOGGER.setLevel(Level.FINE);
        }
        else {
            LOGGER.setLevel(Level.INFO);
        }

        kp.put("loggerFine", logger_fine);
    }

    private String getAuthority(String uri) {
        try {
            String authority = (new URI(uri)).getHost();

            return authority;
        }
        catch (Exception e) {
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("uri=" + uri);
            }

            // Try to recover using regex
            if (uri.startsWith("http://") || uri.startsWith("https://")) {
                String[] authority_parts = uri.split("/");

                if (authority_parts.length > 2) {
                    return authority_parts[2];
                }
            }

            LOGGER.log(Level.WARNING,"uri="+uri, e);
        }

        return "";
    }

    protected boolean evaluate(CrawlURI uri) {
        UURI current_uri = uri.getUURI();
        String str_uri = current_uri.toCustomString();
        String common_suffix = getCommonSuffix();

        if (common_suffix.equals("") || (!str_uri.startsWith("http://") && !str_uri.startsWith("https://"))) {
            return false;
        }

        String authority = getAuthority(str_uri);

        if ((!getDifferentSuffix() && authority.endsWith(common_suffix)) ||
            (getDifferentSuffix() && !authority.endsWith(common_suffix))) {
            if(LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("rule matched for " + str_uri);
            }

            return true;
        }

        return false;
    }
}
