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

import java.util.stream.Collectors;

import java.util.Arrays;

import org.archive.modules.CrawlURI;
import org.archive.net.UURI;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.IOException;

import java.nio.charset.StandardCharsets;

import org.commoncrawl.langdetect.cld2.Cld2;
import org.commoncrawl.langdetect.cld2.Result;

/**
 * Applies its decision based on the detected language by CLD2
 * 
 * @author Cristian García-Romero
 */
public class LangPreferenceDecideRule extends PredicatedDecideRule {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger
            .getLogger(LangPreferenceDecideRule.class.getName());

    public LangPreferenceDecideRule() {
    }

    {
        setApplyOnlyToHTML(true);
        setLangPreference(""); // Multiple langs can be provided. Example for english and icelandic: en|is
        setOnlyReliableDetection(true); // You might want to change this value in order to maximize the classified documents
        setCheckAbsenceInsteadOfPresence(false);
        setSkipCheckIfIsSeed(true);
        setLoggerFine(false);
    }

    public Boolean GetApplyOnlyToHTML() {
        return (Boolean) kp.get("applyOnlyToHTML");
    }

    public void setApplyOnlyToHTML(Boolean apply_only_to_html) {
        kp.put("applyOnlyToHTML", apply_only_to_html);
    }

    public String getLangPreference() {
        return (String) kp.get("langPreference");
    }

    public void setLangPreference(String lang_preference) {
        kp.put("langPreference", lang_preference);
    }

    public Boolean getOnlyReliableDetection() {
        return (Boolean) kp.get("onlyReliableDetection");
    }

    public void setOnlyReliableDetection(Boolean only_reliable_detection) {
        kp.put("onlyReliableDetection", only_reliable_detection);
    }

    public Boolean getCheckAbsenceInsteadOfPresence() {
        return (Boolean) kp.get("checkAbsenceInsteadOfPresence");
    }

    public void setCheckAbsenceInsteadOfPresence(Boolean check_absence_instead_of_presence) {
        kp.put("checkAbsenceInsteadOfPresence", check_absence_instead_of_presence);
    }

    public Boolean getSkipCheckIfIsSeed() {
        return (Boolean) kp.get("skipCheckIfIsSeed");
    }

    public void setSkipCheckIfIsSeed(Boolean skip_check_if_is_seed) {
        kp.put("skipCheckIfIsSeed", skip_check_if_is_seed);
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

    public static String getContent(CrawlURI curi, Logger logger) {
        String str_content = "";
        Boolean input_stream_opened = false;
        InputStream stream = null;

        try {
            stream = curi.getFullVia().getRecorder().getContentReplayInputStream();
            input_stream_opened = true;
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

        if (input_stream_opened && stream != null) {
            try {
                stream.close();
            } catch (IOException e) {
                if (logger.isLoggable(Level.WARNING)) {
                    logger.warning("could not close the stream: " + e.toString());
                }
            }
        }

        return str_content;
    }

    protected boolean evaluate(CrawlURI uri) {
        if (getSkipCheckIfIsSeed() && uri.isSeed()) {
            return false;
        }

        UURI current_uri = uri.getUURI();
        String str_uri = current_uri.toCustomString();
        String lang_preference = getLangPreference();

        if (lang_preference.equals("") || (!str_uri.startsWith("http://") && !str_uri.startsWith("https://"))) {
            return false;
        }

        if (GetApplyOnlyToHTML()) {
            if (!uri.getContentType().startsWith("text/html")) {
                // The content is not HTML
                if (LOGGER.isLoggable(Level.FINE)) {
                    // Format: via <tab> via content-type
                    LOGGER.fine(String.format("Content-Type is not HTML\t%s\t%s", str_uri, uri.getContentType()));
                }

                return false;
            }
        }

        String str_via_content = getContent(uri, LOGGER);
        Result lang_result = Cld2.detect(str_via_content, false); // https://github.com/commoncrawl/language-detection-cld2/blob/master/src/main/java/org/commoncrawl/langdetect/cld2/Result.java
        boolean is_reliable = lang_result.isReliable();
        String best_lang_code = lang_result.getLanguageCode();

        if (!(getOnlyReliableDetection() && is_reliable) && getOnlyReliableDetection()) {
            // Detected lang is not relaiable

            if (LOGGER.isLoggable(Level.FINE)) {
                // Format: reliable <tab> best_lang_score <tab> uri
                LOGGER.fine(String.format("reliable\t%s\t%s\t%s", is_reliable, best_lang_code, str_uri));
            }

            return false;
        }

        String[] langs_preference = lang_preference.split("[|]");

        if ((!getCheckAbsenceInsteadOfPresence() && Arrays.asList(langs_preference).contains(best_lang_code)) ||
            (getCheckAbsenceInsteadOfPresence() && !Arrays.asList(langs_preference).contains(best_lang_code))) {
            // Best language based on score
            if(LOGGER.isLoggable(Level.FINE)) {
                // Format: reliable <tab> best_lang_score <tab> uri
                LOGGER.fine(String.format("rule ok\t%s\t%s\t%s", is_reliable, best_lang_code, str_uri));
            }

            return true;
        }

        if(LOGGER.isLoggable(Level.FINE)) {
            // Format: reliable <tab> best_lang_score <tab> uri
            LOGGER.fine(String.format("rule nok\t%s\t%s\t%s", is_reliable, best_lang_code, str_uri));
        }

        return false;
    }
}
