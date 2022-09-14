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

import org.json.JSONObject;
import org.json.JSONArray;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.Base64;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;

import java.net.HttpURLConnection;
import java.net.URL;

/**
 * A CostAssignment policy that uses a constant value of 1 for all CrawlURIs.
 * 
 * @author cgr71ii
 */
public class PUCCostAssignmentPolicy extends CostAssignmentPolicy implements HasKeyedProperties {
//public class PUCCostAssignmentPolicy extends CostAssignmentPolicy {
//public class PUCCostAssignmentPolicy extends CostAssignmentPolicy implements Serializable, HasKeyedProperties {
    private static final long serialVersionUID = 1L;

    private static final Logger logger = Logger.getLogger(PUCCostAssignmentPolicy.class.getName());

    protected KeyedProperties kp = new KeyedProperties();
    public KeyedProperties getKeyedProperties() {
        return kp;
    }

    {
        setMetricServerUrl("http://localhost:5000/inference");
        setUserAgent(String.format("heritrix: %s", PUCCostAssignmentPolicy.class.getName()));
        setUrlsBase64(true);
    }

    public String getMetricServerUrl() {
        return (String) kp.get("metricServerUrl");
    }

    public void setMetricServerUrl(String metric_url) {
        kp.put("metricServerUrl",metric_url);
    }

    public String getUserAgent() {
        return (String) kp.get("userAgent");
    }

    public void setUserAgent(String user_agent) {
        kp.put("userAgent",user_agent);
    }

    public Boolean getUrlsBase64() {
        return (Boolean) kp.get("urlsBase64");
    }

    public void setUrlsBase64(Boolean urls_base64) {
        kp.put("urlsBase64",urls_base64);
    }

    private String sendPOST(String params) throws IOException {
		URL obj = new URL(getMetricServerUrl());
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		con.setRequestMethod("POST");
		con.setRequestProperty("User-Agent", getUserAgent());

		// For POST only - START
		con.setDoOutput(true);
		OutputStream os = con.getOutputStream();
		os.write(params.getBytes());
		os.flush();
		os.close();
		// For POST only - END

		int responseCode = con.getResponseCode();

		if (responseCode == HttpURLConnection.HTTP_OK) { //success
			BufferedReader in = new BufferedReader(new InputStreamReader(
					con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();

            return response.toString();
		}

        return null;
	}

    public double requestMetric(String src_url, String trg_url) {
        double result = 0.0;
        String request_param = String.format("src_urls=%s&trg_urls=%s", src_url, trg_url);

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("request param: " + request_param);
        }

        try {
            String request_result = sendPOST(request_param);

            if (request_result == null) {
                if (logger.isLoggable(Level.WARNING)) {
                    logger.warning("request result was null");
                }

                // Set similarity to minimum value
                result = 0.0;
            }
            else {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("request result: " + request_result);
                }

                JSONObject obj = new JSONObject(request_result);
                JSONArray scores = obj.getJSONArray("ok");

                if (scores.length() != 1) {
                    if (logger.isLoggable(Level.WARNING)) {
                        logger.warning(String.format("unexpected length of scores: %d", scores.length()));
                    }

                    // Set similarity to minimum value
                    result = 0.0;
                } else {
                    String str_result = scores.getString(0);

                    result = Double.parseDouble(str_result);
                }
            }
        } catch (Exception e) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.warning("request exception: " + e.toString());
            }

            // Set similarity to minimum value
            result = 0.0;
        }

        // The result is expected to be [0, 1]
        result = result * 100.0 * 100.0; // [0, 1] -> [0, 100] -> more precission (2 decimals) [0, 10000]

        return result;
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.frontier.CostAssignmentPolicy#costOf(org.archive.crawler.datamodel.CrawlURI)
     */
    public int costOf(CrawlURI curi) {
        UURI uri = curi.getUURI();
        UURI via = curi.getVia();
        String str_uri = uri.toCustomString();

        if (via != null) {
            String str_via = via.toCustomString();

            if (getUrlsBase64()) {
                str_uri = Base64.getEncoder().encodeToString(str_uri.getBytes());
                str_via = Base64.getEncoder().encodeToString(str_via.getBytes());
            }

            if (logger.isLoggable(Level.FINE)) {
                logger.fine(String.format("uri: %s", str_uri));
                logger.fine(String.format("via: %s", str_via));
            }

            // Metric should be a value in [0, 10000]
            double similarity = requestMetric(str_uri, str_via);

            return 10000 - (int)similarity + 1; // [1, 10001]
        }
        else {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine(String.format("via is null. uri: %s", str_uri));
            }
        }

        return 1;
    }

}
