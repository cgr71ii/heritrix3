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

import org.archive.crawler.reporting.CrawlerLoggerModule;

import org.springframework.beans.factory.annotation.Autowired;

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
 * Assumption: model has been trained only with URLs of the same web domain and equal or different subdomain and/or TLD
 * 
 * @author cgr71ii
 */
public class PUCCostAssignmentPolicy extends CostAssignmentPolicy implements HasKeyedProperties {

    private static final long serialVersionUID = 1L;

    protected KeyedProperties kp = new KeyedProperties();
    public KeyedProperties getKeyedProperties() {
        return kp;
    }

    protected CrawlerLoggerModule loggerModule;
    public CrawlerLoggerModule getLoggerModule() {
        return this.loggerModule;
    }
    @Autowired
    public void setLoggerModule(CrawlerLoggerModule loggerModule) {
        this.loggerModule = loggerModule;
    }

    private Logger getLogger() {
        return loggerModule.getUriCost();
    }

    {
        setClassificationInsteadOfRanking(true);
        setClassificationThreshold(50.0);
        setApplyOnlyToHTML(true);
        setLangPreference1("");
        setLangPreference2("");
        setOnlyReliableDetection(true);
        setUseLanguages(true);
        setPriorizeSameDomain(false); // Exploration by default instead of exploitation
        setMetricServerUrl("http://localhost:5000/inference");
        setUserAgent(String.format("heritrix: %s", PUCCostAssignmentPolicy.class.getName()));
        setUrlsBase64(true);
        setLoggerFine(false);
    }

    public Boolean GetClassificationInsteadOfRanking() {
        return (Boolean) kp.get("classificationInsteadOfRanking");
    }

    public void setClassificationInsteadOfRanking(Boolean classification) {
        kp.put("classificationInsteadOfRanking", classification);
    }

    public Double GetClassificationThreshold() {
        return (Double) kp.get("classificationThreshold");
    }

    public void setClassificationThreshold(Double threshold) {
        kp.put("classificationThreshold", threshold);
    }

    public Boolean GetApplyOnlyToHTML() {
        return (Boolean) kp.get("applyOnlyToHTML");
    }

    public void setApplyOnlyToHTML(Boolean apply_only_to_html) {
        kp.put("applyOnlyToHTML", apply_only_to_html);
    }

    /*
     * If true, exploitation of the same web domain will prevail over exploration
     * If false, BE AWARE: we're assuming that URIs of different web domains will create a new queue in heritrix, what it happens by default
     */
    public Boolean getPriorizeSameDomain() {
        return (Boolean) kp.get("priorizeSameDomain");
    }

    public void setPriorizeSameDomain(Boolean priorize_same_domain) {
        kp.put("priorizeSameDomain", priorize_same_domain);
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
        kp.put("loggerFine", logger_fine);
    }

    public double requestMetric(String src_url, String trg_url, String src_lang, String trg_lang) {
        double result = 0.0;
        String request_param = "";

        if (src_lang.equals("") || trg_lang.equals("")) {
            request_param = String.format("src_urls=%s&trg_urls=%s", src_url, trg_url);

            if (getUseLanguages()) {
                getLogger().log(Level.WARNING, String.format("Src or trg is empty but languages were expected: %s %s %s %s", src_lang, trg_lang, src_url, trg_url));
            }
        }
        else {
            request_param = String.format("src_urls=%s&trg_urls=%s&src_urls_lang=%s&trg_urls_lang=%s", src_url, trg_url, src_lang, trg_lang);
        }

        if (getLogger().isLoggable(Level.FINER)) {
            getLogger().finer("request param: " + request_param);
        }

        try {
            String request_result = PUC.sendPOST(getMetricServerUrl(), request_param, getUserAgent());

            if (request_result == null) {
                getLogger().log(Level.WARNING, "Request result was null");

                // Set similarity to minimum value
                result = 0.0;
            }
            else {
                if (getLogger().isLoggable(Level.FINER)) {
                    getLogger().finer("request result: " + request_result);
                }

                JSONObject obj = new JSONObject(request_result);

                try {
                    JSONArray scores = obj.getJSONArray("ok");

                    if (scores.length() != 1) {
                        getLogger().log(Level.WARNING, String.format("Unexpected length of scores: %d", scores.length()));

                        // Set similarity to minimum value
                        result = 0.0;
                    } else {
                        String str_result = scores.getString(0);

                        result = Double.parseDouble(str_result);
                    }
                } catch (JSONException e) {
                    try {
                        String error = obj.getString("err");

                        getLogger().log(Level.WARNING, String.format("PUC error: %s", error), e);
                    } catch (JSONException e2) {
                        getLogger().log(Level.WARNING, "JSON exception", e2);
                    }
                }
            }
        } catch (Exception e) {
            getLogger().log(Level.WARNING, "Request exception", e);

            // Set similarity to minimum value
            result = 0.0;
        }

        // The result is expected to be [0, 1]
        result = result * 100.0; // [0, 1] -> [0, 100]

        return result;
    }

    public int costOf(CrawlURI curi) {
        if (getLoggerFine()) {
            getLogger().setLevel(Level.FINE);
        }
        else {
            getLogger().setLevel(Level.INFO);
        }

        UURI uri = curi.getUURI();
        UURI via = curi.getVia();
        String str_uri = PUC.removeTrailingSlashes(uri.toCustomString());
        int cost = 101;
        int uri_resource_idx = str_uri.lastIndexOf("/");
        String uri_resource = uri_resource_idx >= 0 ? str_uri.substring(uri_resource_idx + 1) : "";
        Boolean is_classification = GetClassificationInsteadOfRanking();
        Boolean is_ranking = !is_classification;

        if (uri_resource.equals("robots.txt") || str_uri.startsWith("dns:")) {
            return 1;
        }
        if (via == null) {
            cost = 1;

            if (getLogger().isLoggable(Level.FINE)) {
                // Format: cost <tab> uri
                getLogger().fine(String.format("via is null\t%d\t%s", cost, str_uri));
            }

            return cost;
        }

        String str_via = PUC.removeTrailingSlashes(via.toCustomString());
        String src_urls_lang = "";
        String trg_urls_lang = "";
        int via_resource_idx = str_via.lastIndexOf("/");
        String via_resource = via_resource_idx >= 0 ? str_via.substring(via_resource_idx + 1) : "";

        if (via_resource.equals("robots.txt") || str_via.startsWith("dns:")) {
            return 1;
        }
        if (str_via.equals(str_uri)) {
            return is_ranking ? 104 : 4;
        }
        if ((!str_uri.startsWith("http://") && !str_uri.startsWith("https://")) ||
            (!str_via.startsWith("http://") && !str_via.startsWith("https://"))) {
            // Format: via <tab> uri
            getLogger().log(Level.WARNING, String.format("Unexpected URI scheme\t%s\t%s", str_via, str_uri));

            return is_ranking ? 105 : 5;
        }
        if (GetApplyOnlyToHTML()) {
            CrawlURI via_curi = curi.getFullVia();

            // We want via and current URI documents to be HTML, but current URI head hasn't been downloaded yet...
            if (!via_curi.getContentType().startsWith("text/html")) {
                // The content is not HTML
                if (getLogger().isLoggable(Level.FINE)) {
                    // Format: via <tab> via content-type
                    getLogger().fine(String.format("Content-Type is not HTML\t%s\t%s", str_via, via_curi.getContentType()));
                }

                return is_ranking ? 103 : 3;
            }
        }

        String lang_ok = "";
        String detected_lang = "";
        String uri_domain = PUC.getDomain(str_uri, getLogger());
        String via_domain = PUC.getDomain(str_via, getLogger());

        // Language
        if (getUseLanguages()) {
            String lang1 = getLangPreference1();
            String lang2 = getLangPreference2();
            String[] lang_result;

            lang_result = PUC.isLangOk(curi, lang1, lang2, getOnlyReliableDetection(), getLogger());
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
                    // Format: lang1 <tab> lang2 <tab> lang_ok <tab> detected_lang
                    getLogger().log(Level.WARNING, String.format("Unexpected languages mismatch\t%s\t%s\t%s\t%s", lang1, lang2, lang_ok, detected_lang));
                }
            }
        }

        if (getUseLanguages() && lang_ok.equals("")) {
            return is_ranking ? 102 : 2;
        }
        if (getPriorizeSameDomain() && !uri_domain.equals(via_domain)) {
            // Exploitation of the current web domain
            return is_ranking ? 102 : 2;
        }
        else if (!uri_domain.equals(via_domain)) {
            // Exploration
            // PUC hasn't seen URLs of different domains
            return 1;
        }

        // Apply classifier (we don't want to evaluate docs which are not in the selected language or domain)
        if (getUrlsBase64()) {
            str_uri = Base64.getEncoder().encodeToString(str_uri.getBytes()).replace('+', '_');
            str_via = Base64.getEncoder().encodeToString(str_via.getBytes()).replace('+', '_');
        }

        // Metric should be a value in [0, 100]
        double similarity = requestMetric(str_via, str_uri, src_urls_lang, trg_urls_lang);

        if (getUrlsBase64()) {
            str_uri = Base64.getEncoder().encodeToString(str_uri.getBytes()).replace('_', '+');
            str_via = Base64.getEncoder().encodeToString(str_via.getBytes()).replace('_', '+');
        }

        if (is_classification) {
            if (similarity >= GetClassificationThreshold()) {
                cost = 1;
            }
            else {
                cost = 2;
            }
        }
        else {
            cost = 100 - (int)(similarity + 0.5) + 1; // [1, 101]
        }

        if (getLogger().isLoggable(Level.FINE)) {
            // Format: cost <tab> similarity <tab> via <tab> via detected lang <tab> uri <tab> src_lang <tab> trg_lang
            getLogger().fine(String.format("ok\t%d\t%f\t%s\t%s\t%s\t%s\t%s", cost, similarity, str_via, detected_lang, str_uri, src_urls_lang, trg_urls_lang));
        }

        return cost;
    }

}
