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

import org.archive.net.UURI;

import org.archive.modules.CrawlURI;

import org.archive.spring.HasKeyedProperties;
import org.archive.spring.KeyedProperties;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.Base64;

/**
 * A CostAssignment policy that uses the via and current URI and are
 * provided to a classifier through its API and uses the given
 * score as inverse cost (1 - score)
 * 
 * @author cgr71ii
 */
public class PUCCostAssignmentPolicy extends CostAssignmentPolicy implements HasKeyedProperties {

    private static final long serialVersionUID = 1L;

    //private static final ConfigPath logFile = new ConfigPath(PUCCostAssignmentPolicy.class.getName(),"${launchId}/logs/cost_puc.log");
    private static final Logger logger = Logger.getLogger(PUCCostAssignmentPolicy.class.getName());

    protected KeyedProperties kp = new KeyedProperties();
    public KeyedProperties getKeyedProperties() {
        return kp;
    }

    /*
    private static void setupLogFile() throws IOException, SecurityException {
        logger.setLevel(Level.INFO);

        GenerationFileHandler fh = GenerationFileHandler.makeNew(logFile.getFile().getAbsolutePath(), false, false);

        logger.addHandler(fh);
        logger.setUseParentHandlers(false);
    }
    */

    {
        setClassifierExplorationValue(1);
        setLangPreference1("");
        setLangPreference2("");
        setOnlyReliableDetection(true);
        setUseLanguages(true);
        setSameDomain(true);
        setMetricServerUrl("http://localhost:5000/inference");
        setUserAgent(String.format("heritrix: %s", PUCCostAssignmentPolicy.class.getName()));
        setUrlsBase64(true);
        setLoggerFine(false);

        /*
        try {
            setupLogFile();
        } catch (Exception e) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.warning("couldn't setup the log file: " + e.toString());
            }
        }
        */
    }

    public Integer getClassifierExplorationValue() {
        return (Integer) kp.get("classifierExplorationValue");
    }

    public void setClassifierExplorationValue(Integer classifier_exploration_value) {
        kp.put("classifierExplorationValue", classifier_exploration_value);
    }

    public Boolean getSameDomain() {
        return (Boolean) kp.get("sameDomain");
    }

    public void setSameDomain(Boolean same_domain) {
        kp.put("sameDomain", same_domain);
    }

    public Boolean getUseLanguages() {
        return (Boolean) kp.get("useLanguages");
    }

    public void setUseLanguages(Boolean use_languages) {
        kp.put("useLanguages", use_languages);
    }

    public Boolean getOnlyReliableDetection() {
        return (Boolean) kp.get("onlyReliableDetection");
    }

    public void setOnlyReliableDetection(Boolean only_reliable_detection) {
        kp.put("onlyReliableDetection", only_reliable_detection);
    }

    public String getLangPreference1() {
        return (String) kp.get("langPreference1");
    }

    public void setLangPreference1(String lang_preference) {
        kp.put("langPreference1", lang_preference);
    }

    public String getLangPreference2() {
        return (String) kp.get("langPreference2");
    }

    public void setLangPreference2(String lang_preference) {
        kp.put("langPreference2", lang_preference);
    }

    public String getMetricServerUrl() {
        return (String) kp.get("metricServerUrl");
    }

    public void setMetricServerUrl(String metric_url) {
        kp.put("metricServerUrl", metric_url);
    }

    public String getUserAgent() {
        return (String) kp.get("userAgent");
    }

    public void setUserAgent(String user_agent) {
        kp.put("userAgent", user_agent);
    }

    public Boolean getUrlsBase64() {
        return (Boolean) kp.get("urlsBase64");
    }

    public void setUrlsBase64(Boolean urls_base64) {
        kp.put("urlsBase64", urls_base64);
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

    public double requestMetric(String src_url, String trg_url, String src_lang, String trg_lang) {
        double result = 0.0;
        String request_param = "";

        if (src_lang.equals("") || trg_lang.equals("")) {
            request_param = String.format("src_urls=%s&trg_urls=%s", src_url, trg_url);

            if (getUseLanguages()) {
                logger.log(Level.WARNING, String.format("Src or trg is empty but languages were expected: %s %s %s %s", src_lang, trg_lang, src_url, trg_url));
            }
        }
        else {
            request_param = String.format("src_urls=%s&trg_urls=%s&src_urls_lang=%s&trg_urls_lang=%s", src_url, trg_url, src_lang, trg_lang);
        }

        if (logger.isLoggable(Level.FINER)) {
            logger.finer("request param: " + request_param);
        }

        try {
            String request_result = PUC.sendPOST(getMetricServerUrl(), request_param, getUserAgent());

            if (request_result == null) {
                logger.log(Level.WARNING, "Request result was null");

                // Set similarity to minimum value
                result = 0.0;
            }
            else {
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer("request result: " + request_result);
                }

                JSONObject obj = new JSONObject(request_result);

                try {
                    JSONArray scores = obj.getJSONArray("ok");

                    if (scores.length() != 1) {
                        logger.log(Level.WARNING, String.format("Unexpected length of scores: %d", scores.length()));

                        // Set similarity to minimum value
                        result = 0.0;
                    } else {
                        String str_result = scores.getString(0);

                        result = Double.parseDouble(str_result);
                    }
                } catch (JSONException e) {
                    try {
                        String error = obj.getString("err");

                        logger.log(Level.WARNING, String.format("PUC error: %s", error), e);
                    } catch (JSONException e2) {
                        logger.log(Level.WARNING, "JSON exception", e2);
                    }
                }
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Request exception", e);

            // Set similarity to minimum value
            result = 0.0;
        }

        // The result is expected to be [0, 1]
        result = result * 100.0; // [0, 1] -> [0, 100]

        return result;
    }

    public int costOf(CrawlURI curi) {
        UURI uri = curi.getUURI();
        UURI via = curi.getVia();
        String str_uri = PUC.removeTrailingSlashes(uri.toCustomString());
        int cost = 101;
        String uri_file = "";
        int uri_resource_idx = str_uri.lastIndexOf("/");

        if (uri_resource_idx >= 0) {
            uri_file = str_uri.substring(uri_resource_idx + 1);
        }

        if (uri_file.equals("robots.txt")){
            cost = 1;
        }
        else if (via != null) {
            String str_via = PUC.removeTrailingSlashes(via.toCustomString());
            String src_urls_lang = "";
            String trg_urls_lang = "";

            if ((str_uri.startsWith("http://") || str_uri.startsWith("https://")) &&
                (str_via.startsWith("http://") || str_via.startsWith("https://"))) {
                String lang_ok = "";
                String detected_lang = "";
                String uri_domain = PUC.getDomain(str_uri, logger);
                String via_domain = PUC.getDomain(str_via, logger);

                // Language
                if (getUseLanguages()) {
                    String lang1 = getLangPreference1();
                    String lang2 = getLangPreference2();
                    String[] lang_result;

                    lang_result = PUC.isLangOk(curi, lang1, lang2, getOnlyReliableDetection(), logger);
                    lang_ok = lang_result[0];
                    detected_lang = lang_result[1];

                    if (!lang_ok.equals("")) {
                        // via doc lang detected
                        if (lang_ok.equals(lang1)) {
                            src_urls_lang = lang1;
                            trg_urls_lang = lang2;
                        }
                        else if (lang_ok.equals(lang2)) {
                            src_urls_lang = lang2;
                            trg_urls_lang = lang1;
                        }
                        else {
                            logger.log(Level.WARNING, String.format("Unexpected languages mismatch: lang1 | lang2 | lang_ok | detected_lang: %s | %s | %s | %s", lang1, lang2, lang_ok, detected_lang));
                        }
                    }
                }

                if (getUseLanguages() && lang_ok.equals("")) {
                    cost = 110;
                }
                else if (getSameDomain() && !uri_domain.equals(via_domain)) {
                    cost = 1; // We want to explore
                }
                // Apply classifier (we don't want to evaluate docs which are not in the selected language or domain)
                else {
                    if (getUrlsBase64()) {
                        str_uri = Base64.getEncoder().encodeToString(str_uri.getBytes());
                        str_via = Base64.getEncoder().encodeToString(str_via.getBytes());
                    }

                    // Metric should be a value in [0, 100]
                    double similarity = requestMetric(str_via, str_uri, src_urls_lang, trg_urls_lang);
                    Integer exploration_value = getClassifierExplorationValue();

                    cost = 100 - (int)similarity + exploration_value; // [exploration_value, 100 + exploration_value]

                    if (logger.isLoggable(Level.FINE)) {
                        logger.fine(String.format("cost | similarity | via (detected lang) -> uri | src_lang - trg_lang: %d | %f | %s (%s) -> %s | %s - %s", cost, similarity, str_via, detected_lang, str_uri, src_urls_lang, trg_urls_lang));
                    }
                }
            }
            else if (str_uri.startsWith("dns:") || str_via.startsWith("dns:")) {
                cost = 1;
            }
            else {
                cost = 150;

                logger.log(Level.WARNING, String.format("Unexpected URI scheme: %s -> %s", str_via, str_uri));
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
