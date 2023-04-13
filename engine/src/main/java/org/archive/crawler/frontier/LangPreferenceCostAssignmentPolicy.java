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
import java.util.Arrays;

import org.commoncrawl.langdetect.cld2.Cld2;
import org.commoncrawl.langdetect.cld2.Result;

/**
 * A CostAssignment policy that uses the current URI and is
 * provided to CLD2 and assign the cost.
 * 
 * @author cgr71ii
 */
public class LangPreferenceCostAssignmentPolicy extends CostAssignmentPolicy implements HasKeyedProperties {

    private static final long serialVersionUID = 1L;

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

    protected KeyedProperties kp = new KeyedProperties();
    public KeyedProperties getKeyedProperties() {
        return kp;
    }

    {
        setApplyOnlyToHTML(true);
        setPriorizeSameDomain(false); // Exploration by default instead of exploitation
        setLangPreference("en|fr");
        setUseOnlyMainLang(true);
        setUseCoveredText(false);
        setOnlyReliableDetection(true);
        setLoggerFine(false);
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

    public Boolean getUseCoveredText() {
        return (Boolean) kp.get("useCoveredText");
    }

    public void setUseCoveredText(Boolean use_covered_text) {
        kp.put("useCoveredText", use_covered_text);
    }

    public String getLangPreference() {
        return (String) kp.get("langPreference");
    }

    public void setLangPreference(String lang_preference) {
        kp.put("langPreference", lang_preference);
    }

    public Boolean getUseOnlyMainLang() {
        return (Boolean) kp.get("onlyReliableDetection");
    }

    public void setUseOnlyMainLang(Boolean use_only_main_lang) {
        kp.put("useOnlyMainLang", use_only_main_lang);
    }

    public Boolean getOnlyReliableDetection() {
        return (Boolean) kp.get("onlyReliableDetection");
    }

    public void setOnlyReliableDetection(Boolean only_reliable_detection) {
        kp.put("onlyReliableDetection", only_reliable_detection);
    }

    public Boolean getLoggerFine() {
        return (Boolean) kp.get("loggerFine");
    }

    public void setLoggerFine(Boolean logger_fine) {
        kp.put("loggerFine", logger_fine);
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
        String lang_preference = getLangPreference();
        String[] langs_preference = lang_preference.split("[|]");
        int uri_resource_idx = str_uri.lastIndexOf("/");
        String uri_resource = uri_resource_idx >= 0 ? str_uri.substring(uri_resource_idx + 1) : "";
        Boolean use_covered_text = getUseCoveredText();

        if (uri_resource.equals("robots.txt") || str_uri.startsWith("dns:")) {
            return 1;
        }
        if (via == null) {
            if (getLogger().isLoggable(Level.FINE)) {
                // Format: cost <tab> uri
                getLogger().fine(String.format("via is null\t%d\t%s", cost, str_uri));
            }

            return 1;
        }

        String str_via = PUC.removeTrailingSlashes(via.toCustomString());
        int via_resource_idx = str_via.lastIndexOf("/");
        String via_resource = via_resource_idx >= 0 ? str_via.substring(via_resource_idx + 1) : "";

        if (via_resource.equals("robots.txt") || str_via.startsWith("dns:")) {
            return 1;
        }
        if (str_via.equals(str_uri)) {
            return use_covered_text ? 104 : 4;
        }
        if ((!str_uri.startsWith("http://") && !str_uri.startsWith("https://")) ||
            (!str_via.startsWith("http://") && !str_via.startsWith("https://"))) {
            // Format: via <tab> uri
            getLogger().log(Level.WARNING, String.format("Unexpected URI scheme\t%s\t%s", str_via, str_uri));

            return use_covered_text ? 105 : 5;
        }
        if (GetApplyOnlyToHTML()) {
            CrawlURI via_curi = curi.getFullVia();

            // We apply to via_curi because is the document we are going to process (curi hasn't been downloaded yet)
            if (!via_curi.getContentType().startsWith("text/html")) {
                // The content is not HTML
                if (getLogger().isLoggable(Level.FINE)) {
                    // Format: via <tab> via content-type
                    getLogger().fine(String.format("Content-Type is not HTML\t%s\t%s", str_via, via_curi.getContentType()));
                }

                return use_covered_text ? 103 : 3;
            }
        }

        String uri_domain = PUC.getDomain(str_uri, getLogger());
        String via_domain = PUC.getDomain(str_via, getLogger());
        String str_via_content = PUC.getContent(curi, getLogger());
        Result lang_result = Cld2.detect(str_via_content, false); // https://github.com/commoncrawl/language-detection-cld2/blob/master/src/main/java/org/commoncrawl/langdetect/cld2/Result.java
        boolean is_reliable = lang_result.isReliable();

        if (!(getOnlyReliableDetection() && is_reliable) && getOnlyReliableDetection()) {
            // Detected lang is not relaiable

            if (getLogger().isLoggable(Level.FINE)) {
                // Format: reliable <tab> via <tab> uri
                getLogger().fine(String.format("reliable\t%s\t%s\t%s", is_reliable, str_via, str_uri));
            }

            return use_covered_text ? 102 : 2;
        }

        String detected_langs = lang_result.toJSON();
        JSONObject obj = new JSONObject(detected_langs);
        Double text_covered_perc = null;

        try {
            JSONArray langs = obj.getJSONArray("languages");
            String[] lang_codes = new String[langs.length()];
            String[] text_covered_langs = new String[langs.length()];

            for (int i = 0; i < langs.length(); i++) {
                JSONObject lang_json = langs.getJSONObject(i);
                String lang_code = lang_json.getString("code");
                Double text_covered = lang_json.getDouble("text-covered") * 100.0;

                if (text_covered_perc == null && Arrays.asList(langs_preference).contains(lang_code)) {
                    // Only first detected languaged will be processed

                    if (!getUseOnlyMainLang() || (getUseOnlyMainLang() && i == 0)) {
                        if (use_covered_text) {
                            text_covered_perc = text_covered;
                        }
                        else {
                            text_covered_perc = 100.0; // The specific value MATTERS! The queue rotation will be affected
                        }
                    }
                }

                lang_codes[i] = lang_code;
                text_covered_langs[i] = text_covered.toString();
            }

            if (getLogger().isLoggable(Level.FINE)) {
                // Format: langs <tab> text_covered <tab> via <tab> uri
                getLogger().fine(String.format("all detected langs\t%s\t%s\t%s\t%s", String.join(" ", lang_codes), String.join(" ", text_covered_langs), str_via, str_uri));
            }
        } catch (JSONException e) {
            // Format: via <tab> uri
            getLogger().log(Level.WARNING, String.format("JSON exception (unexpected)\t%s\t%s", str_via, str_uri), e);
        }

        if (text_covered_perc == null) {
            // Target langs not detected
            return use_covered_text ? 102 : 2;
        }
        if (getPriorizeSameDomain() && !uri_domain.equals(via_domain)) {
            // Exploitation of the current web domain
            return use_covered_text ? 102 : 2; // Not being in the same domain it will assign the same cost that if the target languages were not detected
        }
        else if (!uri_domain.equals(via_domain)) {
            // Exploration
            return 1;
        }

        // Check similarity based on language detection
        // Metric should be a value in [0, 100]
        double similarity = text_covered_perc; // [0.0, 100.0]

        cost = 100 - (int)(similarity + 0.5) + 1; // [1, 101]

        if (getLogger().isLoggable(Level.FINE)) {
            // Format: cost <tab> similarity <tab> via <tab> uri
            getLogger().fine(String.format("ok\t%d\t%f\t%s\t%s", cost, similarity, str_via, str_uri));
        }

        return cost;
    }

}
