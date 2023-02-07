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

import org.archive.net.UURI;

import org.archive.modules.CrawlURI;

import org.archive.spring.HasKeyedProperties;
import org.archive.spring.KeyedProperties;
import org.archive.spring.ConfigPath;

import org.archive.io.GenerationFileHandler;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.stream.Collectors;

import java.util.Base64;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.InputStream;

import java.nio.charset.StandardCharsets;

import java.nio.file.Paths;

import java.net.HttpURLConnection;
import java.net.URL;

import org.commoncrawl.langdetect.cld2.Cld2;
import org.commoncrawl.langdetect.cld2.Result;

/**
 * A CostAssignment policy that uses the via and current URI and are
 * provided to a classifier through its API and uses the given
 * score as inverse cost (1 - score)
 * 
 * @author cgr71ii
 */
public class LangPreferenceCostAssignmentPolicy extends CostAssignmentPolicy implements HasKeyedProperties {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = Logger.getLogger(LangPreferenceCostAssignmentPolicy.class.getName());

    protected KeyedProperties kp = new KeyedProperties();
    public KeyedProperties getKeyedProperties() {
        return kp;
    }

    {
        setLoggerFine(false);
        setLangPreference("en");
    }

    public String getLangPreference() {
        return (String) kp.get("langPreference");
    }

    public void setLangPreference(String lang_preference) {
        kp.put("langPreference", lang_preference);
    }

    public Boolean getLoggerFine() {
        return (Boolean) kp.get("loggerFine");
    }

    public void setLoggerFine(Boolean logger_fine) {
        if (logger_fine) {
            logger.setLevel(Level.FINE);
        }

        kp.put("loggerFine", logger_fine);
    }

    public String getContent(CrawlURI curi) {
        String str_content = "";

        try (InputStream stream = curi.getFullVia().getRecorder().getContentReplayInputStream()) {
            str_content = new BufferedReader(
                new InputStreamReader(stream, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));
        }
        catch (Exception e) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.warning("stream exception: " + e.toString());
            }
        }

        return str_content;
    }

    public int costOf(CrawlURI curi) {
        UURI uri = curi.getUURI();
        UURI via = curi.getVia();
        String str_uri = uri.toCustomString();
        int cost = 101;
        String uri_file = "";
        String lang_preference = getLangPreference();

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
            String str_via_content = getContent(curi);
            Result lang_result = Cld2.detect(str_via_content, false);

            if ((str_uri.startsWith("http://") || str_uri.startsWith("https://")) &&
                (str_via.startsWith("http://") || str_via.startsWith("https://"))) {

                // Metric should be a value in [0, 100]
                double similarity = 1.0;

                cost = 100 - (int)similarity + 1; // [1, 101]

                if (logger.isLoggable(Level.FINE)) {
                    logger.fine(String.format("cost<tab>similarity<tab>via<tab>uri: %d\t%f\t%s\t%s", cost, similarity, str_via, str_uri));
                }
            }
            else if (str_uri.startsWith("dns:") || str_via.startsWith("dns:")) {
                cost = 1;
            }
            else {
                cost = 101;

                if (logger.isLoggable(Level.WARNING)) {
                    logger.warning(String.format("Unexpected URI scheme: via<tab>uri: %s\t%s", str_via, str_uri));
                }
            }
        }
        else {
            cost = 1;

            if (logger.isLoggable(Level.FINE)) {
                logger.fine(String.format("via is null. uri: (cost: %d) %s", cost, str_uri));
            }
        }

        return cost;
    }

}
