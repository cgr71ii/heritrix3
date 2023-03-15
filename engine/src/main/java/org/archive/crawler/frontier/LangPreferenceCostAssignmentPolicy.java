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
    private static final Logger logger = Logger.getLogger(LangPreferenceCostAssignmentPolicy.class.getName());

    protected KeyedProperties kp = new KeyedProperties();
    public KeyedProperties getKeyedProperties() {
        return kp;
    }

    {
        setApplyOnlyToHTML(true);
        setSameDomainReward(0.0);
        setLangPreference("en|fr");
        setUseOnlyMainLang(true);
        setUseCoveredText(true);
        setOnlyReliableDetection(true);
        setLoggerFine(false);
    }

    public Boolean GetApplyOnlyToHTML() {
        return (Boolean) kp.get("applyOnlyToHTML");
    }

    public void setApplyOnlyToHTML(Boolean apply_only_to_html) {
        kp.put("applyOnlyToHTML", apply_only_to_html);
    }

    public Double getSameDomainReward() {
        return (Double) kp.get("sameDomainReward");
    }

    public void setSameDomainReward(Double same_domain_reward) {
        kp.put("sameDomainReward", same_domain_reward);
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
        if (logger_fine) {
            logger.setLevel(Level.FINE);
        }
        else {
            logger.setLevel(Level.INFO);
        }

        kp.put("loggerFine", logger_fine);
    }

    public int costOf(CrawlURI curi) {
        UURI uri = curi.getUURI();
        UURI via = curi.getVia();
        String str_uri = PUC.removeTrailingSlashes(uri.toCustomString());
        int cost = 101;
        String lang_preference = getLangPreference();
        String[] langs_preference = lang_preference.split("[|]");
        int uri_resource_idx = str_uri.lastIndexOf("/");
        String uri_resource = uri_resource_idx >= 0 ? str_uri.substring(uri_resource_idx + 1) : "";

        if (uri_resource.equals("robots.txt") || str_uri.startsWith("dns:")) {
            return 1;
        }
        if (via == null) {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine(String.format("via is null. uri: (cost: %d) %s", cost, str_uri));
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
            return 110;
        }
        if ((!str_uri.startsWith("http://") && !str_uri.startsWith("https://")) ||
            (!str_via.startsWith("http://") && !str_via.startsWith("https://"))) {
            logger.log(Level.WARNING, String.format("Unexpected URI scheme: %s -> %s", str_via, str_uri));

            return 150;
        }
        if (GetApplyOnlyToHTML()) {
            CrawlURI via_curi = curi.getFullVia();

            // We apply to via_curi because is the document we are going to process (curi hasn't been downloaded yet)
            if (!via_curi.getContentType().startsWith("text/html")) {
                // The content is not HTML
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine(String.format("Content-Type is not HTML (via uri | via content-type): %s | %s", str_via, via_curi.getContentType()));
                }

                return 100;
            }
        }

        String uri_domain = PUC.getDomain(str_uri, logger);
        String via_domain = PUC.getDomain(str_via, logger);
        String str_via_content = PUC.getContent(curi, logger);
        Result lang_result = Cld2.detect(str_via_content, false); // https://github.com/commoncrawl/language-detection-cld2/blob/master/src/main/java/org/commoncrawl/langdetect/cld2/Result.java
        boolean is_reliable = lang_result.isReliable();

        if (!(getOnlyReliableDetection() && is_reliable) && getOnlyReliableDetection()) {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine(String.format("reliable | via -> uri: %s | %s -> %s", is_reliable, str_via, str_uri));
            }

            return 110;
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
                        if (getUseCoveredText()) {
                            text_covered_perc = text_covered;
                        }
                        else {
                            text_covered_perc = 100.1; // It doesn't matter the specific result but that all have the same value
                        }
                    }
                }

                lang_codes[i] = lang_code;
                text_covered_langs[i] = text_covered.toString();
            }

            if (logger.isLoggable(Level.FINE)) {
                logger.fine(String.format("langs | text_covered | via -> uri: %s | %s | %s -> %s", String.join(" ", lang_codes), String.join(" ", text_covered_langs), str_via, str_uri));
            }
        } catch (JSONException e) {
            logger.log(Level.WARNING, String.format("JSON exception (unexpected): %s -> %s", str_via, str_uri), e);
        }

        if (text_covered_perc == null) {
            // Target langs not detected
            cost = 110;
        }
        else {
            // Metric should be a value in [0, 100]
            double similarity = text_covered_perc; // 0.0, 100.1 or text_covered

            if (uri_domain.equals(via_domain)) {
                similarity += getSameDomainReward();
            }

            if (similarity > 100.0) {
                similarity = 100.0;
            }

            cost = 100 - (int)similarity + 1; // [1, 101]

            if (logger.isLoggable(Level.FINE)) {
                logger.fine(String.format("cost | similarity | via -> uri: %d | %f | %s -> %s", cost, similarity, str_via, str_uri));
            }
        }

        return cost;
    }

}
